// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backup

import (
	"context"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
	"path"
	"sync"
	"testing"
	"time"
)

const (
	ModuleName = "Backup"
)

func TestBackupData(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithQuickScanAndCKPOpts(nil)
	db := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer db.Close()

	schema := catalog.MockSchemaAll(13, 3)
	schema.BlockMaxRows = 10
	schema.SegmentMaxBlocks = 10
	testutil.CreateRelation(t, db, "db", schema, true)

	totalRows := uint64(schema.BlockMaxRows * 30)
	bat := catalog.MockBatch(schema, int(totalRows))
	defer bat.Close()
	bats := bat.Split(100)

	var wg sync.WaitGroup
	pool, _ := ants.NewPool(80)
	defer pool.Release()

	start := time.Now()
	for _, data := range bats {
		wg.Add(1)
		err := pool.Submit(testutil.AppendClosure(t, data, schema.Name, db, &wg))
		assert.Nil(t, err)
	}
	wg.Wait()
	t.Logf("Append %d rows takes: %s", totalRows, time.Since(start))
	{
		txn, rel := testutil.GetDefaultRelation(t, db, schema.Name)
		testutil.CheckAllColRowsByScan(t, rel, int(totalRows), false)
		assert.NoError(t, txn.Commit(context.Background()))
	}
	t.Log(db.Catalog.SimplePPString(common.PPL1))

	now := time.Now()
	testutils.WaitExpect(20000, func() bool {
		return db.Runtime.Scheduler.GetPenddingLSNCnt() == 0
	})
	t.Log(time.Since(now))
	t.Logf("Checkpointed: %d", db.Runtime.Scheduler.GetCheckpointedLSN())
	t.Logf("GetPenddingLSNCnt: %d", db.Runtime.Scheduler.GetPenddingLSNCnt())
	assert.Equal(t, uint64(0), db.Runtime.Scheduler.GetPenddingLSNCnt())
	t.Log(db.Catalog.SimplePPString(common.PPL1))
	wg.Add(1)
	testutil.AppendFailClosure(t, bats[0], schema.Name, db, &wg)()
	wg.Wait()

	dir := path.Join(db.Dir, "/local")
	c := fileservice.Config{
		Name:    defines.LocalFileServiceName,
		Backend: "DISK",
		DataDir: dir,
	}
	service, err := fileservice.NewFileService(ctx, c, nil)
	assert.Nil(t, err)
	checkpoints := db.BGCheckpointRunner.GetAllCheckpoints()
	var data *logtail.CheckpointData
	files := make(map[string]string, 0)
	for _, candidate := range checkpoints {
		data, err = collectCkpData(candidate, db.Catalog)
		assert.Nil(t, err)
		defer data.Close()
		ins, _, _, _ := data.GetBlkBatchs()
		for i := 0; i < ins.Length(); i++ {
			metaLoc := objectio.Location(ins.GetVectorByName(pkgcatalog.BlockMeta_MetaLoc).Get(i).([]byte))
			if metaLoc == nil {
				continue
			}
			if files[metaLoc.Name().String()] == "" {
				files[metaLoc.Name().String()] = metaLoc.String()
			}
		}
	}

	locations := make([]string, 0)
	for _, location := range files {
		locations = append(locations, location)
	}
	err = execBackup(ctx, db.Opts.Fs, service, locations)
	assert.Nil(t, err)
}

func collectCkpData(
	ckp *checkpoint.CheckpointEntry,
	catalog *catalog.Catalog,
) (data *logtail.CheckpointData, err error) {
	factory := logtail.IncrementalCheckpointDataFactory(
		ckp.GetStart(),
		ckp.GetEnd(),
	)
	data, err = factory(catalog)
	return
}
