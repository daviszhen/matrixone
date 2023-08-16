// Copyright 2023 Matrix Origin
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

package client

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
)

// leakChecker is used to detect leak txn which is not committed or aborted.
type leakChecker struct {
	sync.RWMutex
	logger         *log.MOLogger
	actives        []activeTxn
	maxActiveAges  time.Duration
	leakHandleFunc func(txnID []byte, createAt time.Time, createBy string)
	stopper        *stopper.Stopper
}

func newLeakCheck(
	maxActiveAges time.Duration,
	leakHandleFunc func(txnID []byte, createAt time.Time, createBy string)) *leakChecker {
	logger := runtime.DefaultRuntime().Logger()
	return &leakChecker{
		logger:         logger,
		maxActiveAges:  maxActiveAges,
		leakHandleFunc: leakHandleFunc,
		stopper: stopper.NewStopper("txn-leak-checker",
			stopper.WithLogger(logger.RawLogger())),
	}
}

func (lc *leakChecker) start() {
	if err := lc.stopper.RunTask(lc.check); err != nil {
		panic(err)
	}
}

func (lc *leakChecker) close() {
	lc.stopper.Stop()
}

func (lc *leakChecker) txnOpened(
	txnID []byte,
	fromPrepare bool,
	createBy, whoPrepare, prepareSql string, txnMeta txn.TxnMeta) {
	if createBy == "" {
		createBy = "unknown"
	}

	if fromPrepare {
		fmt.Println("xxxx>", "leakchecker", hex.EncodeToString(txnID), fromPrepare, whoPrepare, prepareSql)
	}

	lc.Lock()
	defer lc.Unlock()
	lc.actives = append(lc.actives, activeTxn{
		createBy:    createBy,
		id:          txnID,
		createAt:    time.Now(),
		fromPrepare: fromPrepare,
		whoPrepare:  whoPrepare,
		prepareSql:  prepareSql,
	})
}

func (lc *leakChecker) txnClosed(txnID []byte) {
	lc.Lock()
	defer lc.Unlock()
	values := lc.actives[:0]
	for idx, txn := range lc.actives {
		if bytes.Equal(txn.id, txnID) {
			values = append(values, lc.actives[idx+1:]...)
			break
		}
		values = append(values, txn)
	}
	lc.actives = values
}

func (lc *leakChecker) activeTxn() []*ActiveTxnData {
	lc.Lock()
	defer lc.Unlock()
	res := make([]*ActiveTxnData, 0, len(lc.actives))
	for _, t := range lc.actives {
		res = append(res, &ActiveTxnData{
			FromPrepare: t.fromPrepare,
			Meta:        t.meta,
			WhoPrepare:  t.whoPrepare,
			PrepareSql:  t.prepareSql,
			ID:          t.id,
		})
	}
	return res
}

func (lc *leakChecker) check(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(lc.maxActiveAges):
			lc.doCheck()
		}
	}
}

func (lc *leakChecker) doCheck() {
	lc.RLock()
	defer lc.RUnlock()

	now := time.Now()
	for _, txn := range lc.actives {
		if now.Sub(txn.createAt) >= lc.maxActiveAges {
			lc.leakHandleFunc(txn.id, txn.createAt, txn.createBy)
		}
	}
}

type activeTxn struct {
	createBy    string
	id          []byte
	createAt    time.Time
	fromPrepare bool
	whoPrepare  string
	prepareSql  string
	meta        txn.TxnMeta
}
