// Copyright 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func handleCreateFunction(ctx context.Context, ses TempInter, cf *tree.CreateFunction) error {
	tenant := ses.GetTenantInfo()
	return InitFunction(ctx, ses.(*Session), tenant, cf)
}

func handleDropFunction(ctx context.Context, ses TempInter, df *tree.DropFunction, proc *process.Process) error {
	return doDropFunction(ctx, ses.(*Session), df, func(path string) error {
		return proc.FileService.Delete(ctx, path)
	})
}

func handleCreateProcedure(ctx context.Context, ses TempInter, cp *tree.CreateProcedure) error {
	tenant := ses.GetTenantInfo()

	return InitProcedure(ctx, ses.(*Session), tenant, cp)
}

func handleDropProcedure(ctx context.Context, ses TempInter, dp *tree.DropProcedure) error {
	return doDropProcedure(ctx, ses.(*Session), dp)
}

func handleCallProcedure(ctx context.Context, ses TempInter, call *tree.CallStmt, proc *process.Process) error {
	proto := ses.GetMysqlProtocol()
	results, err := doInterpretCall(ctx, ses.(*Session), call)
	if err != nil {
		return err
	}

	resp := NewGeneralOkResponse(COM_QUERY, ses.GetServerStatus())

	if len(results) == 0 {
		if err := proto.SendResponse(ses.GetRequestContext(), resp); err != nil {
			return moerr.NewInternalError(ses.GetRequestContext(), "routine send response failed. error:%v ", err)
		}
	} else {
		for i, result := range results {
			mer := NewMysqlExecutionResult(0, 0, 0, 0, result.(*MysqlResultSet))
			resp = ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, i == len(results)-1)
			if err := proto.SendResponse(ses.GetRequestContext(), resp); err != nil {
				return moerr.NewInternalError(ses.GetRequestContext(), "routine send response failed. error:%v ", err)
			}
		}
	}
	return nil
}
