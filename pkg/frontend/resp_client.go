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

func setResponse(ses *Session, isLastStmt bool, rspLen uint64) *Response {
	return ses.SetNewResponse(OkResponse, rspLen, int(COM_QUERY), "", isLastStmt)
}

// response the client
func respClientFunc(requestCtx context.Context,
	ses *Session,
	execCtx *ExecCtx) (err error) {

	switch execCtx.stmt.StmtKind().RespType() {
	case tree.STREAM_RESULT_ROW:
		return respStreamResultRow(requestCtx, ses, execCtx)
	case tree.PREBUILD_RESULT_ROW:
		return respPrebuildResultRow(requestCtx, ses, execCtx)
	case tree.NO_RESP:
	case tree.RESP_ITSELF:
	case tree.RESP_STATUS:
		return respStatus(requestCtx, ses, execCtx)
	}

	if ses.GetQueryInExecute() {
		logStatementStatus(requestCtx, ses, execCtx.stmt, success, nil)
	} else {
		logStatementStatus(requestCtx, ses, execCtx.stmt, fail, moerr.NewInternalError(requestCtx, "query is killed"))
	}
	return err
}
