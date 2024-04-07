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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// response the client
func respClientFunc(requestCtx context.Context,
	ses *Session,
	execCtx *ExecCtx) (err error) {

	switch execCtx.stmt.ResultType() {
	case tree.ResultRow:
		return respResultRow(requestCtx, ses, execCtx)
	case tree.Status:
		return respStatus(requestCtx, ses, execCtx)
	case tree.NoResp:
	case tree.RespItself:
	case tree.Undefined:
		return moerr.NewInternalError(requestCtx, "need set result type for %s", execCtx.sqlOfStmt)
	}

	if ses.GetQueryInExecute() {
		logStatementStatus(requestCtx, ses, execCtx.stmt, success, nil)
	} else {
		logStatementStatus(requestCtx, ses, execCtx.stmt, fail, moerr.NewInternalError(requestCtx, "query is killed"))
	}
	return err
}

func respResultRow(requestCtx context.Context,
	ses *Session,
	execCtx *ExecCtx) (err error) {
	switch execCtx.stmt.(type) {
	case *tree.Select:
		if len(execCtx.proc.SessionInfo.SeqAddValues) != 0 {
			ses.AddSeqValues(execCtx.proc)
		}
		ses.SetSeqLastValue(execCtx.proc)
	}

	//TODO:send eof or ok
	return
}

func respStatus(requestCtx context.Context,
	ses *Session,
	execCtx *ExecCtx) (err error) {
	var rspLen uint64
	if execCtx.runResult != nil {
		rspLen = execCtx.runResult.AffectRows
	}

	switch execCtx.stmt.(type) {
	case *tree.Select:
		//select ... into ...
		if len(execCtx.proc.SessionInfo.SeqAddValues) != 0 {
			ses.AddSeqValues(execCtx.proc)
		}
		ses.SetSeqLastValue(execCtx.proc)

		//TODO: send status

	case *tree.PrepareStmt, *tree.PrepareString:
		if ses.GetCmd() == COM_STMT_PREPARE {
			if err2 := ses.GetMysqlProtocol().SendPrepareResponse(requestCtx, execCtx.prepareStmt); err2 != nil {
				err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
				logStatementStatus(requestCtx, ses, execCtx.stmt, fail, err)
				return err
			}
		} else {
			resp := setResponse(ses, execCtx.isLastStmt, rspLen)
			if err2 := ses.GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
				err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
				logStatementStatus(requestCtx, ses, execCtx.stmt, fail, err)
				return err
			}
		}

	case *tree.Deallocate:
		//we will not send response in COM_STMT_CLOSE command
		if ses.GetCmd() != COM_STMT_CLOSE {
			resp := setResponse(ses, execCtx.isLastStmt, rspLen)
			if err2 := ses.GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
				err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
				logStatementStatus(requestCtx, ses, execCtx.stmt, fail, err)
				return err
			}
		}

	default:
		resp := setResponse(ses, execCtx.isLastStmt, rspLen)
		if _, ok := execCtx.stmt.(*tree.Insert); ok {
			resp.lastInsertId = execCtx.proc.GetLastInsertID()
			if execCtx.proc.GetLastInsertID() != 0 {
				ses.SetLastInsertID(execCtx.proc.GetLastInsertID())
			}
		}
		if len(execCtx.proc.SessionInfo.SeqDeleteKeys) != 0 {
			ses.DeleteSeqValues(execCtx.proc)
		}

		if st, ok := execCtx.stmt.(*tree.CreateTable); ok {
			_ = doGrantPrivilegeImplicitly(requestCtx, ses, st)
		}

		if st, ok := execCtx.stmt.(*tree.DropTable); ok {
			_ = doRevokePrivilegeImplicitly(requestCtx, ses, st)
		}

		if st, ok := execCtx.stmt.(*tree.CreateDatabase); ok {
			_ = insertRecordToMoMysqlCompatibilityMode(requestCtx, ses, execCtx.stmt)
			_ = doGrantPrivilegeImplicitly(requestCtx, ses, st)
		}

		if st, ok := execCtx.stmt.(*tree.DropDatabase); ok {
			_ = deleteRecordToMoMysqlCompatbilityMode(requestCtx, ses, execCtx.stmt)
			_ = doRevokePrivilegeImplicitly(requestCtx, ses, st)
		}

		if err2 := ses.GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
			err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
			logStatementStatus(requestCtx, ses, execCtx.stmt, fail, err)
			return err
		}

	}
	return
}
