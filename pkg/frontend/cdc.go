// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	cdc2 "github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	pb "github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/tools"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

const (
	insertNewCdcTaskFormat = `insert into mo_catalog.mo_cdc_task values(` +
		`%d,` + //account id
		`"%s",` + //task id
		`"%s",` + //task name
		`"%s",` + //source_uri
		`"%s",` + //source_password
		`"%s",` + //sink_uri
		`"%s",` + //sink_type
		`"%s",` + //sink_password
		`"%s",` + //sink_ssl_ca_path
		`"%s",` + //sink_ssl_cert_path
		`"%s",` + //sink_ssl_key_path
		`"%s",` + //tables
		`"%s",` + //filters
		`"%s",` + //opfilters
		`"%s",` + //source_state
		`"%s",` + //sink_state
		`%d,` + //start_ts
		`"%d",` + //start_ts_str
		`%d,` + //end_ts
		`"%d",` + //end_ts_str
		`"%s",` + //config_file
		`"%s",` + //task_create_time
		`"%s",` + //state
		`%d,` + //checkpoint
		`"%d",` + //checkpoint_str
		`"%s",` + //full_config
		`"%s",` + //incr_config
		`"",` + //reserved0
		`"",` + //reserved1
		`"",` + //reserved2
		`"",` + //reserved3
		`""` + //reserved4
		`)`

	getCdcTaskFormat = `select ` +
		`sink_uri, ` +
		`sink_type, ` +
		`sink_password, ` +
		`tables, ` +
		`start_ts_str, ` +
		`checkpoint_str ` +
		`from ` +
		`mo_catalog.mo_cdc_task ` +
		`where ` +
		`account_id = %d and ` +
		`task_id = "%s"`

	getDbIdAndTableIdFormat = "select reldatabase_id,rel_id from mo_catalog.mo_tables where account_id = %d and reldatabase = '%s' and relname = '%s'"

	getTables = "select account_name, reldatabase, relname from mo_catalog.mo_tables join mo_catalog.mo_account on mo_catalog.mo_tables.account_id = mo_catalog.mo_account.account_id where REGEXP_LIKE(account_name, '%s') and REGEXP_LIKE(reldatabase, '%s') and REGEXP_LIKE(relname, '%s')"

	getCdcTaskId = "select task_id from mo_catalog.mo_cdc_task where account_id = %d"

	getCdcTaskIdWhere = "select task_id from mo_catalog.mo_cdc_task where account_id = %d and task_name = '%s'"

	dropCdcMeta = "delete from mo_catalog.mo_cdc_task where account_id = %d and task_id = '%s'"

	updateCdcMeta = "update mo_catalog.mo_cdc_task set state = '%s' where account_id = %d and task_id = '%s'"

	updatedWatermark = "update mo_catalog.mo_cdc_task set checkpoint_str = '%s' where account_id = %d and task_id = '%s'"
)

type dbTableInfo struct {
	dbName  string
	tblName string
	dbId    uint64
	tblId   uint64
}

func getSqlForNewCdcTask(
	accId uint64,
	taskId uuid.UUID,
	taskName string,
	sourceUri string,
	sourcePwd string,
	sinkUri string,
	sinkTyp string,
	sinkPwd string,
	sinkCaPath string,
	sinkCertPath string,
	sinkKeyPath string,
	tables string,
	filters string,
	opfilters string,
	sourceState string,
	sinkState string,
	startTs uint64,
	endTs uint64,
	configFile string,
	taskCreateTime time.Time,
	state string,
	checkpoint uint64,
	fullConfig string,
	incrConfig string,
) string {
	return fmt.Sprintf(insertNewCdcTaskFormat,
		accId,
		taskId,
		taskName,
		sourceUri,
		sourcePwd,
		sinkUri,
		sinkTyp,
		sinkPwd,
		sinkCaPath,
		sinkCertPath,
		sinkKeyPath,
		tables,
		filters,
		opfilters,
		sourceState,
		sinkState,
		startTs,
		startTs,
		endTs,
		endTs,
		configFile,
		taskCreateTime.Format(time.DateTime),
		state,
		checkpoint,
		checkpoint,
		fullConfig,
		incrConfig,
	)
}

func getSqlForRetrievingCdcTask(
	accId uint64,
	taskId uuid.UUID,
) string {
	return fmt.Sprintf(getCdcTaskFormat, accId, taskId)
}

func getSqlForDbIdAndTableId(accId uint64, db, table string) string {
	return fmt.Sprintf(getDbIdAndTableIdFormat, accId, db, table)
}

func getSqlForTables(
	pt *PatternTuple,
) string {
	return fmt.Sprintf(getTables, pt.SourceAccount, pt.SourceDatabase, pt.SourceTable)
}

func getSqlForTaskIdAndName(ses *Session, all bool, taskName string) string {
	if all {
		return fmt.Sprintf(getCdcTaskId, ses.GetAccountId())
	} else {
		return fmt.Sprintf(getCdcTaskIdWhere, ses.GetAccountId(), taskName)
	}
}

func getSqlForDropCdcMeta(ses *Session, taskId string) string {
	return fmt.Sprintf(dropCdcMeta, ses.GetAccountId(), taskId)
}

func getSqlForUpdateCdcMeta(ses *Session, taskId string, status string) string {
	return fmt.Sprintf(updateCdcMeta, status, ses.GetAccountId(), taskId)
}

const (
	AccountLevel  = "account"
	ClusterLevel  = "cluster"
	MysqlSink     = "mysql"
	MatrixoneSink = "matrixone"

	SASCommon = "common"
	SASError  = "error"

	SyncLoading = "loading"
	SyncRunning = "running"
	SyncStopped = "stopped"
)

func handleCreateCdc(ses *Session, execCtx *ExecCtx, create *tree.CreateCDC) error {
	return doCreateCdc(execCtx.reqCtx, ses, create)
}

func doCreateCdc(ctx context.Context, ses *Session, create *tree.CreateCDC) (err error) {
	fmt.Fprintln(os.Stderr, "===>create cdc", create.Tables)
	ts := getGlobalPu().TaskService
	if ts == nil {
		return moerr.NewInternalError(ctx, "no task service is found")
	}

	err = createCdc(
		ctx,
		ses,
		ts,
		create,
	)
	return err
}

func cdcTaskMetadata(cdcId string) pb.TaskMetadata {
	return pb.TaskMetadata{
		ID:       cdcId,
		Executor: pb.TaskCode_InitCdc,
		Options: pb.TaskOptions{
			MaxRetryTimes: defaultConnectorTaskMaxRetryTimes,
			RetryInterval: defaultConnectorTaskRetryInterval,
			DelayDuration: 0,
			Concurrency:   0,
		},
	}
}

func string2uint64(str string) (res uint64, err error) {
	if str != "" {
		res, err = strconv.ParseUint(str, 10, 64)
		if err != nil {
			return 0, err
		}
	} else {
		res = uint64(0)
	}
	return res, nil
}

type PatternTuple struct {
	SourceAccount  string
	SourceDatabase string
	SourceTable    string
	SinkAccount    string
	SinkDatabase   string
	SinkTable      string
	OriginString   string
}

func splitPattern(pattern string) (*PatternTuple, error) {
	pt := &PatternTuple{OriginString: pattern}
	if strings.Contains(pattern, ":") {
		splitRes := strings.Split(pattern, ":")
		if len(splitRes) != 2 {
			return nil, fmt.Errorf("invalid pattern format")
		}

		source := strings.Split(splitRes[0], ".")
		if len(source) != 2 && len(source) != 3 {
			return nil, fmt.Errorf("invalid pattern format")
		}
		if len(source) == 2 {
			pt.SourceDatabase, pt.SourceTable = source[0], source[1]
		} else {
			pt.SourceAccount, pt.SourceDatabase, pt.SourceTable = source[0], source[1], source[2]
		}

		sink := strings.Split(splitRes[1], ".")
		if len(sink) != 2 && len(sink) != 3 {
			return nil, fmt.Errorf("invalid pattern format")
		}
		if len(sink) == 2 {
			pt.SinkDatabase, pt.SinkTable = sink[0], sink[1]
		} else {
			pt.SinkAccount, pt.SinkDatabase, pt.SinkTable = sink[0], sink[1], sink[2]
		}
		return pt, nil
	}

	source := make([]string, 0)
	current := strings.Builder{}
	inRegex := false
	isRegex := false
	for i := 0; i < len(pattern); i++ {
		char := pattern[i]
		if char == '/' {
			isRegex = true
			inRegex = !inRegex
		} else if char == '.' && !inRegex {
			res := current.String()
			if !isRegex {
				res = strings.ReplaceAll(res, "?", ".")
				res = strings.ReplaceAll(res, "*", ".*")
				res = "^" + res + "$"
			}
			isRegex = false
			source = append(source, res)
			current.Reset()
		} else {
			current.WriteByte(char)
		}
	}
	if current.Len() > 0 {
		res := current.String()
		if !isRegex {
			res = strings.ReplaceAll(res, "?", ".")
			res = strings.ReplaceAll(res, "*", ".*")
			res = "^" + res + "$"
		}
		isRegex = false
		source = append(source, res)
		current.Reset()
	}
	if (len(source) != 2 && len(source) != 3) || inRegex {
		return nil, fmt.Errorf("invalid pattern format")
	}
	if len(source) == 2 {
		pt.SourceDatabase, pt.SourceTable = source[0], source[1]
	} else {
		pt.SourceAccount, pt.SourceDatabase, pt.SourceTable = source[0], source[1], source[2]
	}
	return pt, nil
}

func string2patterns(pattern string) ([]*PatternTuple, error) {
	pts := make([]*PatternTuple, 0)
	current := strings.Builder{}
	inRegex := false
	for i := 0; i < len(pattern); i++ {
		char := pattern[i]
		if char == '/' {
			inRegex = !inRegex
		}
		if char == ',' && !inRegex {
			pt, err := splitPattern(current.String())
			if err != nil {
				return nil, err
			}
			pts = append(pts, pt)
			current.Reset()
		} else {
			current.WriteByte(char)
		}
	}

	if current.Len() != 0 {
		pt, err := splitPattern(current.String())
		if err != nil {
			return nil, err
		}
		pts = append(pts, pt)
	}
	return pts, nil
}

func patterns2tables(ctx context.Context, pts []*PatternTuple, bh BackgroundExec) (map[string]string, error) {
	resMap := make(map[string]string)
	for _, pt := range pts {
		sql := getSqlForTables(pt)
		bh.ClearExecResultSet()
		err := bh.Exec(ctx, sql)
		if err != nil {
			return nil, err
		}
		erArray, err := getResultSet(ctx, bh)
		if err != nil {
			return nil, err
		}
		if execResultArrayHasData(erArray) {
			res := erArray[0]
			for rowIdx := range erArray[0].GetRowCount() {
				sourceString := strings.Builder{}
				sinkString := strings.Builder{}
				acc, err := res.GetString(ctx, rowIdx, 0)
				if err != nil {
					return nil, err
				}
				db, err := res.GetString(ctx, rowIdx, 1)
				if err != nil {
					return nil, err
				}
				tbl, err := res.GetString(ctx, rowIdx, 2)
				if err != nil {
					return nil, err
				}
				sourceString.WriteString(acc)
				sourceString.WriteString(".")
				sourceString.WriteString(db)
				sourceString.WriteString(".")
				sourceString.WriteString(tbl)
				if pt.SinkTable != "" {
					if pt.SinkAccount != "" {
						sinkString.WriteString(pt.SinkAccount)
						sinkString.WriteString(".")
					}
					sinkString.WriteString(pt.SinkDatabase)
					sinkString.WriteString(".")
					sinkString.WriteString(pt.SinkTable)
				} else {
					sinkString.WriteString(sourceString.String())
				}
				resMap[sourceString.String()] = sinkString.String()
			}
		}
	}
	return resMap, nil
}

func createCdc(
	ctx context.Context,
	ses *Session,
	ts taskservice.TaskService,
	create *tree.CreateCDC,
) error {
	var startTs, endTs uint64
	var err error

	accInfo := ses.GetTenantInfo()
	cdcId, _ := uuid.NewV7()

	details := &pb.Details{
		AccountID: accInfo.GetTenantID(),
		Account:   accInfo.GetTenant(),
		Username:  accInfo.GetUser(),
		Details: &pb.Details_CreateCdc{
			CreateCdc: &pb.CreateCdcDetails{
				TaskName:  create.TaskName,
				AccountId: uint64(accInfo.GetTenantID()),
				TaskId:    cdcId.String(),
			},
		},
	}

	fmt.Fprintln(os.Stderr, "====>save cdc task",
		accInfo.GetTenantID(),
		cdcId,
		create.TaskName,
		create.SourceUri,
		create.SinkUri,
		create.SinkType,
	)
	cdcTaskOptionsMap := make(map[string]string)
	for i := 0; i < len(create.Option); i += 2 {
		cdcTaskOptionsMap[create.Option[i]] = create.Option[i+1]
	}

	pts, err := string2patterns(create.Tables)
	if err != nil {
		return err
	}

	if strings.EqualFold(cdcTaskOptionsMap["Level"], ClusterLevel) {
		if !ses.tenant.IsMoAdminRole() {
			return moerr.NewInternalError(ctx, "Only sys account administrator are allowed to create cluster level task")
		}
	} else if strings.EqualFold(cdcTaskOptionsMap["Level"], AccountLevel) {
		if !ses.tenant.IsAccountAdminRole() || ses.GetTenantName() != cdcTaskOptionsMap["Account"] {
			return moerr.NewInternalError(ctx, "No privilege to create task on %s", cdcTaskOptionsMap["Account"])
		}
		for _, pt := range pts {
			if pt.SourceAccount == "" {
				pt.SourceAccount = ses.GetTenantName()
			}
			if ses.GetTenantName() != pt.SourceAccount {
				return moerr.NewInternalError(ctx, "No privilege to create task on table %s", pt.OriginString)
			}
		}
	}

	startTs, err = string2uint64(cdcTaskOptionsMap["StartTS"])
	if err != nil {
		return err
	}
	endTs, err = string2uint64(cdcTaskOptionsMap["EndTS"])
	if err != nil {
		return err
	}

	dat := time.Now().UTC()

	//TODO: make it better
	//Currently just for test
	insertSql := getSqlForNewCdcTask(
		uint64(accInfo.GetTenantID()),
		cdcId,
		create.TaskName,
		create.SourceUri,
		"FIXME",
		create.SinkUri,
		create.SinkType,
		"FIXME",
		"",
		"",
		"",
		create.Tables,
		"FIXME",
		"FIXME",
		SASCommon,
		SASCommon,
		startTs,
		endTs,
		cdcTaskOptionsMap["ConfigFile"],
		dat,
		SyncLoading,
		0, //FIXME
		"FIXME",
		"FIXME",
	)

	if _, err = ts.AddCdcTask(ctx, cdcTaskMetadata(cdcId.String()), details, insertSql); err != nil {
		return err
	}
	return nil
}

func RegisterCdcExecutor(
	logger *zap.Logger,
	ts taskservice.TaskService,
	ieFactory func() ie.InternalExecutor,
	attachToTask func(context.Context, uint64, taskservice.ActiveRoutine) error,
	createTxnClient func(bool) (client.TxnClient, client.TimestampWaiter, error),
	cnUUID string,
	fileService fileservice.FileService,
	cnTxnClient client.TxnClient,
	cnEngine engine.Engine,
) func(ctx context.Context, task task.Task) error {
	return func(ctx context.Context, T task.Task) error {
		fmt.Fprintln(os.Stderr, "====>", "cdc task executor")
		ctx1, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		tasks, err := ts.QueryDaemonTask(ctx1,
			taskservice.WithTaskIDCond(taskservice.EQ, T.GetID()),
		)
		if err != nil {
			return err
		}
		if len(tasks) != 1 {
			return moerr.NewInternalError(ctx, "invalid tasks count %d", len(tasks))
		}
		details, ok := tasks[0].Details.Details.(*task.Details_CreateCdc)
		if !ok {
			return moerr.NewInternalError(ctx, "invalid details type")
		}

		fmt.Fprintln(os.Stderr, "====>", "cdc task info 1", tasks[0].String())
		accId := details.CreateCdc.GetAccountId()
		taskId := details.CreateCdc.GetTaskId()
		taskName := details.CreateCdc.GetTaskName()

		fmt.Fprintln(os.Stderr, "====>", "cdc task info 2", accId, taskId, taskName)

		cdc := NewCdcTask(
			logger,
			ieFactory(),
			details.CreateCdc,
			createTxnClient,
			cnUUID,
			fileService,
			cnTxnClient,
			cnEngine,
		)
		cdc.activeRoutine = cdc2.NewCdcActiveRoutine()
		if err := attachToTask(ctx, T.GetID(), cdc); err != nil {
			return err
		}
		err = cdc.Start(ctx, true)

		return err
	}
}

type CdcTask struct {
	logger *zap.Logger
	ie     ie.InternalExecutor

	cnUUID          string
	cnTxnClient     client.TxnClient
	cnEngine        engine.Engine
	fileService     fileservice.FileService
	createTxnClient func(bool) (client.TxnClient, client.TimestampWaiter, error)

	cdcTask      *task.CreateCdcDetails
	cdcTxnClient client.TxnClient
	cdcTsWaiter  client.TimestampWaiter
	cdcEngMp     *mpool.MPool
	cdcEngine    *disttae.CdcEngine
	// tableId -> *disttae.CdcRelation
	cdcTables *sync.Map

	sinkUri string

	activeRoutine *cdc2.ActiveRoutine
	// inputChs are channels between partitioner and decoder; key is tableId
	inputChs map[uint64]chan tools.Pair[*disttae.TableCtx, *disttae.DecoderInput]
	// interChs are channels between decoder and sinker; key is tableId
	interChs map[uint64]chan tools.Pair[*disttae.TableCtx, *cdc2.DecoderOutput]
	// TODO receivedWatermarkUpdater update the watermark of the items received from upstream
	//receivedWatermarkUpdater *cdc2.WatermarkUpdater
	// sunkWatermarkUpdater update the watermark of the items that has been sunk to downstream
	sunkWatermarkUpdater *cdc2.WatermarkUpdater
}

func NewCdcTask(
	logger *zap.Logger,
	ie ie.InternalExecutor,
	cdcTask *task.CreateCdcDetails,
	createTxnClient func(bool) (client.TxnClient, client.TimestampWaiter, error),
	cnUUID string,
	fileService fileservice.FileService,
	cnTxnClient client.TxnClient,
	cnEngine engine.Engine,
) *CdcTask {
	return &CdcTask{
		logger:          logger,
		ie:              ie,
		cdcTask:         cdcTask,
		createTxnClient: createTxnClient,
		cnUUID:          cnUUID,
		fileService:     fileService,
		cnTxnClient:     cnTxnClient,
		cnEngine:        cnEngine,
	}
}

func (cdc *CdcTask) Start(rootCtx context.Context, firstTime bool) (err error) {
	fmt.Fprintln(os.Stderr, "====>cdc start")

	ctx := defines.AttachAccountId(rootCtx, uint32(cdc.cdcTask.AccountId))

	cdcTaskId, _ := uuid.Parse(cdc.cdcTask.TaskId)

	//step1 : get cdc task definition
	sql := getSqlForRetrievingCdcTask(cdc.cdcTask.AccountId, cdcTaskId)
	res := cdc.ie.Query(ctx, sql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		return res.Error()
	}

	if res.RowCount() < 1 {
		return moerr.NewInternalError(ctx, "none cdc task for %d %s", cdc.cdcTask.AccountId, cdc.cdcTask.TaskId)
	} else if res.RowCount() > 1 {
		return moerr.NewInternalError(ctx, "duplicate cdc task for %d %s", cdc.cdcTask.AccountId, cdc.cdcTask.TaskId)
	}

	//sink uri
	cdc.sinkUri, err = res.GetString(ctx, 0, 0)
	if err != nil {
		return err
	}

	//sink_type
	sinkTyp, err := res.GetString(ctx, 0, 1)
	if err != nil {
		return err
	}

	if sinkTyp != MysqlSink && sinkTyp != MatrixoneSink {
		return moerr.NewInternalError(ctx, "unsupported sink type: %s", sinkTyp)
	}

	//sink_password
	sinkPwd, err := res.GetString(ctx, 0, 2)
	if err != nil {
		return err
	}

	//tables
	tables, err := res.GetString(ctx, 0, 3)
	if err != nil {
		return err
	}

	// start_ts_str
	startTsStr, err := res.GetString(ctx, 0, 4)
	if err != nil {
		return err
	}

	// watermark
	watermarkStr, err := res.GetString(ctx, 0, 5)
	if err != nil {
		return err
	}
	if watermarkStr == "" {
		watermarkStr = startTsStr
	}

	watermark, err := cdc2.StrToTimestamp(watermarkStr)
	if err != nil {
		watermark = timestamp.Timestamp{}
	}

	fmt.Fprintln(os.Stderr, "====>", "cdc task row",
		cdc.sinkUri,
		sinkTyp,
		sinkPwd,
		tables,
		"startTs", startTsStr,
		"watermark", watermarkStr)

	var dbId, tblId uint64
	tableList := strings.Split(tables, ",")
	dbTableInfos := make([]*dbTableInfo, 0, len(tableList))
	for _, table := range tableList {
		//get dbid tableid for the table
		seps := strings.Split(table, ".")
		if len(seps) != 2 {
			return moerr.NewInternalError(ctx, "invalid tables format")
		}

		dbName, tblName := seps[0], seps[1]
		sql = getSqlForDbIdAndTableId(cdc.cdcTask.AccountId, dbName, tblName)
		res = cdc.ie.Query(ctx, sql, ie.SessionOverrideOptions{})
		if res.Error() != nil {
			return res.Error()
		}

		if res.RowCount() < 1 {
			return moerr.NewInternalError(ctx, "no table %s:%s", dbName, tblName)
		} else if res.RowCount() > 1 {
			return moerr.NewInternalError(ctx, "duplicate table %s:%s", dbName, tblName)
		}

		if dbId, err = res.GetUint64(ctx, 0, 0); err != nil {
			return err
		}

		if tblId, err = res.GetUint64(ctx, 0, 1); err != nil {
			return err
		}

		dbTableInfos = append(dbTableInfos, &dbTableInfo{
			dbName:  dbName,
			tblName: tblName,
			dbId:    dbId,
			tblId:   tblId,
		})
	}
	//dbid 0, tableid 0 reserved for heartbeat
	//dbTableIds = append(dbTableIds, tools.Pair[uint64, uint64]{
	//	Key:   0,
	//	Value: 0,
	//})

	//step2 : create cdc engine when first start
	if firstTime {
		fs, err := fileservice.Get[fileservice.FileService](cdc.fileService, defines.SharedFileServiceName)
		if err != nil {
			return err
		}

		etlFs, err := fileservice.Get[fileservice.FileService](cdc.fileService, defines.ETLFileServiceName)
		if err != nil {
			return err
		}

		if cdc.cdcTxnClient, cdc.cdcTsWaiter, err = cdc.createTxnClient(false); err != nil {
			return err
		}

		if cdc.cdcEngMp, err = mpool.NewMPool("cdc", 0, mpool.NoFixed); err != nil {
			return err
		}

		inQueue := cdc2.NewMemQ[tools.Pair[*disttae.TableCtx, *disttae.DecoderInput]]()
		cdcEngine := disttae.NewCdcEngine(
			ctx,
			cdc.cnUUID,
			cdc.cdcEngMp,
			fs,
			etlFs,
			cdc.cdcTxnClient,
			cdc.cdcTask.GetTaskId(),
			inQueue,
			cdc.cnEngine,
			cdc.cnTxnClient,
		)
		cdc.cdcEngine = cdcEngine

		if err = disttae.InitLogTailPushModel(ctx, cdcEngine, cdc.cdcTsWaiter); err != nil {
			return err
		}

		timeTick := time.Tick(30 * time.Millisecond)
		timeout := time.After(10 * time.Second)
		for retry := true; retry; {
			select {
			case <-timeTick:
				if cdcEngine.PushClient().IsSubscriberReady() {
					retry = false
				}
			case <-timeout:
				retry = false
			}
		}
		if !cdcEngine.PushClient().IsSubscriberReady() {
			return moerr.NewInternalError(ctx, "cdc pushClient is not ready")
		}
	}

	// step3 : create cdc pipeline
	//cdc.receivedWatermarkUpdater = cdc2.NewWatermarkUpdater(cdc.persistWatermark)
	cdc.sunkWatermarkUpdater = cdc2.NewWatermarkUpdater(cdc.persistWatermark)

	// make channels between partitioner and decoder
	cdc.inputChs = make(map[uint64]chan tools.Pair[*disttae.TableCtx, *disttae.DecoderInput], len(dbTableInfos))
	cdc.interChs = make(map[uint64]chan tools.Pair[*disttae.TableCtx, *cdc2.DecoderOutput], len(dbTableInfos))

	for _, info := range dbTableInfos {
		if err = cdc.addExePipelineForTable(info.tblId, watermark); err != nil {
			return err
		}
	}

	// start watermark updater
	go cdc.sunkWatermarkUpdater.Run(cdc.activeRoutine)

	// make partitioner
	partitioner := cdc2.NewPartitioner(cdc.cdcEngine.InQueue(), cdc.inputChs)
	go partitioner.Run(ctx, cdc.activeRoutine)

	//step4 : subscribe the table
	if firstTime {
		cdc.cdcTables = new(sync.Map)
		for _, info := range dbTableInfos {
			//skip heartbeat
			if info.dbId == 0 || info.tblId == 0 {
				continue
			}

			if err = cdc.subscribeTable(info.dbName, info.tblName, info.dbId, info.tblId); err != nil {
				return err
			}
		}

		// hold
		ch := make(chan int, 1)
		<-ch
	}
	return
}

// Resume cdc task from last recorded watermark
func (cdc *CdcTask) Resume() error {
	// closed in Pause, need renew
	cdc.activeRoutine.Pause = make(chan struct{})
	fmt.Println("=====> it's resume")
	return cdc.Start(context.Background(), false)
}

// Restart cdc task from init watermark
func (cdc *CdcTask) Restart() error {
	// closed in Pause, need renew
	cdc.activeRoutine.Cancel = make(chan struct{})
	cdc.activeRoutine.Pause = make(chan struct{})
	fmt.Println("=====> it's restart")
	return cdc.Start(context.Background(), false)
}

// Pause stops the components after inQueue (partitioner, decoder and sinker)
func (cdc *CdcTask) Pause() error {
	close(cdc.activeRoutine.Pause)
	for _, c := range cdc.inputChs {
		close(c)
	}
	for _, c := range cdc.interChs {
		close(c)
	}
	return nil
}

// Cancel stops partitioner, decoder and sinker, as well as the cdc replayer
func (cdc *CdcTask) Cancel() error {
	close(cdc.activeRoutine.Pause)
	close(cdc.activeRoutine.Cancel)
	for _, c := range cdc.inputChs {
		close(c)
	}
	for _, c := range cdc.interChs {
		close(c)
	}

	cdc.cdcTables.Range(func(k, v any) bool {
		tbl := v.(*disttae.CdcRelation)
		// TODO handle error
		cdc.unsubscribeTable(tbl)
		return true
	})
	cdc.cdcTables = nil
	return nil
}

func handleDropCdc(ses *Session, execCtx *ExecCtx, st *tree.DropCDC) error {
	return updateCdc(execCtx.reqCtx, ses, st)
}

func handlePauseCdc(ses *Session, execCtx *ExecCtx, st *tree.PauseCDC) error {
	return updateCdc(execCtx.reqCtx, ses, st)
}

func handleResumeCdc(ses *Session, execCtx *ExecCtx, st *tree.ResumeCDC) error {
	return updateCdc(execCtx.reqCtx, ses, st)
}

func handleRestartCdc(ses *Session, execCtx *ExecCtx, st *tree.RestartCDC) error {
	return updateCdc(execCtx.reqCtx, ses, st)
}

func updateCdc(ctx context.Context, ses *Session, st tree.Statement) (err error) {
	var targetTaskStatus task.TaskStatus
	var n int
	ts := getGlobalPu().TaskService
	if ts == nil {
		return moerr.NewInternalError(ctx,
			"task service not ready yet, please try again later.")
	}
	switch stmt := st.(type) {
	case *tree.DropCDC:
		targetTaskStatus = task.TaskStatus_CancelRequested
		if stmt.Option.All {
			n, err = ts.UpdateCdcTask(ctx, targetTaskStatus,
				taskservice.WithAccountID(taskservice.EQ, ses.accountId),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		} else {
			n, err = ts.UpdateCdcTask(ctx, targetTaskStatus,
				taskservice.WithAccountID(taskservice.EQ, ses.accountId),
				taskservice.WithTaskName(taskservice.EQ, stmt.Option.TaskName),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		}
	case *tree.PauseCDC:
		targetTaskStatus = task.TaskStatus_PauseRequested
		if stmt.Option.All {
			n, err = ts.UpdateCdcTask(ctx, targetTaskStatus,
				taskservice.WithAccountID(taskservice.EQ, ses.accountId),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		} else {
			n, err = ts.UpdateCdcTask(ctx, targetTaskStatus,
				taskservice.WithAccountID(taskservice.EQ, ses.accountId),
				taskservice.WithTaskName(taskservice.EQ, stmt.Option.TaskName),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		}
	case *tree.RestartCDC:
		targetTaskStatus = task.TaskStatus_RestartRequested
		n, err = ts.UpdateCdcTask(ctx, targetTaskStatus,
			taskservice.WithAccountID(taskservice.EQ, ses.accountId),
			taskservice.WithTaskName(taskservice.EQ, stmt.TaskName),
			taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
		)
	case *tree.ResumeCDC:
		targetTaskStatus = task.TaskStatus_ResumeRequested
		n, err = ts.UpdateCdcTask(ctx, targetTaskStatus,
			taskservice.WithAccountID(taskservice.EQ, ses.accountId),
			taskservice.WithTaskName(taskservice.EQ, stmt.TaskName),
			taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
		)
	}
	if err != nil {
		return err
	}
	if n < 1 {
		return moerr.NewInternalError(ctx, "There is no any cdc task.")
	}
	return
}

func (cdc *CdcTask) persistWatermark(watermark timestamp.Timestamp) error {
	accountId := uint32(cdc.cdcTask.AccountId)
	cdcTaskId, _ := uuid.Parse(cdc.cdcTask.TaskId)
	watermarkStr := cdc2.TimestampToStr(watermark)
	sql := fmt.Sprintf(updatedWatermark, watermarkStr, accountId, cdcTaskId)

	ctx := defines.AttachAccountId(context.Background(), uint32(cdc.cdcTask.AccountId))
	return cdc.ie.Exec(ctx, sql, ie.SessionOverrideOptions{})
}

func (cdc *CdcTask) subscribeTable(dbName, tblName string, dbId, tblId uint64) (err error) {
	cdcTbl := disttae.NewCdcRelation(
		dbName,
		tblName,
		cdc.cdcTask.AccountId,
		dbId,
		tblId,
		cdc.cdcEngine,
	)

	ctx := defines.AttachAccountId(context.Background(), uint32(cdc.cdcTask.AccountId))
	fmt.Fprintf(os.Stderr, "====> cdc subscribeTable %s(%v).%s(%v) before\n", dbName, dbId, tblName, tblId)
	if err = disttae.SubscribeCdcTable(ctx, cdcTbl, dbId, tblId); err != nil {
		return
	}
	fmt.Fprintf(os.Stderr, "====> cdc subscribeTable %s(%v).%s(%v) after\n", dbName, dbId, tblName, tblId)

	cdc.cdcTables.Store(tblId, cdcTbl)
	return
}

func (cdc *CdcTask) unsubscribeTable(cdcTbl *disttae.CdcRelation) (err error) {
	ctx := defines.AttachAccountId(context.Background(), uint32(cdc.cdcTask.AccountId))
	dbId, tblId := cdcTbl.GetDBID(ctx), cdcTbl.GetTableID(ctx)

	fmt.Fprintf(os.Stderr, "====> cdc unsubscribeTable %s(%v).%s(%v) before\n", cdcTbl.GetDBName(), dbId, cdcTbl.GetTableName(), tblId)
	if err = cdc.cdcEngine.UnsubscribeTable(ctx, dbId, tblId); err != nil {
		return
	}
	fmt.Fprintf(os.Stderr, "====> cdc unsubscribeTable %s(%v).%s(%v) after\n", cdcTbl.GetDBName(), dbId, cdcTbl.GetTableName(), tblId)

	cdc.cdcTables.Delete(tblId)
	return
}

func (cdc *CdcTask) addExePipelineForTable(tableId uint64, watermark timestamp.Timestamp) (err error) {
	//                         + == inputCh == > decoder == interCh == > sinker -> remote db    // for table 1
	// 	                       |
	// inQueue -> partitioner -+ == inputCh == > decoder == interCh == > sinker -> remote db    // for table 2
	//	                       |                                                                   ...
	//	                       |                                                                   ...
	// 						   + == inputCh == > decoder == interCh == > sinker -> remote db	// for table n

	ctx := defines.AttachAccountId(context.Background(), uint32(cdc.cdcTask.AccountId))

	// make inputCh for table
	cdc.inputChs[tableId] = make(chan tools.Pair[*disttae.TableCtx, *disttae.DecoderInput])

	// make interCh for table
	cdc.interChs[tableId] = make(chan tools.Pair[*disttae.TableCtx, *cdc2.DecoderOutput])

	// make decoder for table
	decoder := cdc2.NewDecoder(cdc.cdcEngMp, cdc.cdcEngine.FS(), tableId, cdc.inputChs[tableId], cdc.interChs[tableId])
	go decoder.Run(ctx, cdc.activeRoutine)

	// make sinker for table
	sinker, err := cdc2.NewSinker(ctx, cdc.sinkUri, cdc.interChs[tableId], watermark, cdc.sunkWatermarkUpdater.UpdateTableWatermark)
	if err != nil {
		return err
	}
	go sinker.Run(ctx, cdc.activeRoutine)

	// add into watermark updater
	cdc.sunkWatermarkUpdater.UpdateTableWatermark(tableId, watermark)

	return
}

func (cdc *CdcTask) removeExePipelineForTable(tableId uint64) (err error) {
	// close and delete inputCh
	close(cdc.inputChs[tableId])
	delete(cdc.inputChs, tableId)

	// close and delete interChs
	close(cdc.interChs[tableId])
	delete(cdc.interChs, tableId)

	// remove from watermark updater
	cdc.sunkWatermarkUpdater.RemoveTable(tableId)

	return
}
