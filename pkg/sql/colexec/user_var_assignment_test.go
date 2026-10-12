// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package colexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestUserVariableAssignmentExecutorPreservesRowReadAfterWrite(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithMPool(mpool.MustNewZero()), testutil.WithFileService(nil))
	proc.Base.IsFrontend = true

	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	input.SetRowCount(2)
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(10), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(20), false, proc.Mp()))
	defer input.Clean(proc.Mp())

	int64Type := types.T_int64.ToType().PlanType()
	assignment := &plan.Expr{
		Typ: int64Type,
		Expr: &plan.Expr_UserVarAssignment{UserVarAssignment: &plan.UserVarAssignment{
			Name: "a",
			Value: &plan.Expr{
				Typ:  int64Type,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}},
			},
		}},
	}
	assignmentExec, err := NewExpressionExecutor(proc, assignment)
	require.NoError(t, err)
	defer assignmentExec.Free()

	result, err := assignmentExec.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(10), vector.GetFixedAtNoTypeCheck[int64](result, 0))
	require.Equal(t, int64(20), vector.GetFixedAtNoTypeCheck[int64](result, 1))

	read := &plan.Expr{
		Typ:  int64Type,
		Expr: &plan.Expr_V{V: &plan.VarRef{Name: "a"}},
	}
	readExec, err := NewExpressionExecutor(proc, read)
	require.NoError(t, err)
	defer readExec.Free()
	readResult, err := readExec.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(10), vector.GetFixedAtNoTypeCheck[int64](readResult, 0))
	require.Equal(t, int64(20), vector.GetFixedAtNoTypeCheck[int64](readResult, 1))

	var committed []process.UserVariableAssignment
	proc.SetUserVariableAssignmentCommitFunc(func(value process.UserVariableAssignment) error {
		committed = append(committed, value)
		return nil
	})
	require.NoError(t, proc.CommitUserVariableAssignments())
	require.Len(t, committed, 1)
	require.Equal(t, int64(20), committed[0].Value)
}
