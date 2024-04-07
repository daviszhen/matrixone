// Copyright 2021 - 2024 Matrix Origin
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
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// executeStatusStmt run the statement that responses status t
func executeStatusStmt(requestCtx context.Context, ses *Session, execCtx *ExecCtx) (err error) {
	var loadLocalErrGroup *errgroup.Group
	var columns []interface{}

	mrs := ses.GetMysqlResultSet()
	ep := ses.GetExportConfig()
	switch execCtx.stmt.(type) {
	case *tree.Select:
		if ep.needExportToFile() {

			columns, err = execCtx.cw.GetColumns()
			if err != nil {
				logError(ses, ses.GetDebugString(),
					"Failed to get columns from computation handler",
					zap.Error(err))
				return
			}
			for _, c := range columns {
				mysqlc := c.(Column)
				mrs.AddColumn(mysqlc)
			}

			// open new file
			ep.DefaultBufSize = gPu.SV.ExportDataDefaultFlushSize
			initExportFileParam(ep, mrs)
			if err = openNewFile(requestCtx, ep, mrs); err != nil {
				return
			}

			runBegin := time.Now()
			/*
				Start pipeline
				Producing the data row and sending the data row
			*/
			// todo: add trace
			if _, err = execCtx.runner.Run(0); err != nil {
				return
			}

			// only log if run time is longer than 1s
			if time.Since(runBegin) > time.Second {
				logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Run : %s", time.Since(runBegin).String()))
			}

			oq := NewOutputQueue(ses.GetRequestContext(), ses, 0, nil, nil)
			if err = exportAllData(oq); err != nil {
				return
			}
			if err = ep.Writer.Flush(); err != nil {
				return
			}
			if err = ep.File.Close(); err != nil {
				return
			}

			/*
			   Serialize the execution plan by json
			*/
			if cwft, ok := execCtx.cw.(*TxnComputationWrapper); ok {
				_ = cwft.RecordExecPlan(requestCtx)
			}
		}
	default:
		//change privilege
		switch execCtx.stmt.(type) {
		case *tree.DropTable, *tree.DropDatabase, *tree.DropIndex, *tree.DropView, *tree.DropSequence,
			*tree.CreateUser, *tree.DropUser, *tree.AlterUser,
			*tree.CreateRole, *tree.DropRole,
			*tree.Revoke, *tree.Grant,
			*tree.SetDefaultRole, *tree.SetRole:
			ses.InvalidatePrivilegeCache()
		}
		runBegin := time.Now()
		/*
			Step 1: Start
		*/

		if st, ok := execCtx.stmt.(*tree.Load); ok {
			if st.Local {
				loadLocalErrGroup = new(errgroup.Group)
				loadLocalErrGroup.Go(func() error {
					return processLoadLocal(execCtx.proc.Ctx, ses, st.Param, execCtx.loadLocalWriter)
				})
			}
		}

		if execCtx.runResult, err = execCtx.runner.Run(0); err != nil {
			if loadLocalErrGroup != nil { // release resources
				err2 := execCtx.proc.LoadLocalReader.Close()
				if err2 != nil {
					logError(ses, ses.GetDebugString(),
						"processLoadLocal goroutine failed",
						zap.Error(err2))
				}
				err2 = loadLocalErrGroup.Wait() // executor failed, but processLoadLocal is still running, wait for it
				if err2 != nil {
					logError(ses, ses.GetDebugString(),
						"processLoadLocal goroutine failed",
						zap.Error(err2))
				}
			}
			return
		}

		if loadLocalErrGroup != nil {
			if err = loadLocalErrGroup.Wait(); err != nil { //executor success, but processLoadLocal goroutine failed
				return
			}
		}

		// only log if run time is longer than 1s
		if time.Since(runBegin) > time.Second {
			logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Run : %s", time.Since(runBegin).String()))
		}

		echoTime := time.Now()

		logDebug(ses, ses.GetDebugString(), fmt.Sprintf("time of SendResponse %s", time.Since(echoTime).String()))

		/*
			Step 4: Serialize the execution plan by json
		*/
		if cwft, ok := execCtx.cw.(*TxnComputationWrapper); ok {
			_ = cwft.RecordExecPlan(requestCtx)
		}
	}

	return
}
