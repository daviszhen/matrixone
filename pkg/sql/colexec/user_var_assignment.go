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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// UserVarAssignmentExpressionExecutor evaluates := one logical row at a time.
// The row-wise loop is intentional: a later projection (or a later row in a
// self-referential assignment) must see the values staged by earlier rows.
// Publication is owned by the frontend statement boundary, not by this
// executor.
type UserVarAssignmentExpressionExecutor struct {
	mp         *mpool.MPool
	allocation *vector.AllocationAccountSelection
	child      ExpressionExecutor
	name       string
	typ        types.Type
	result     *vector.Vector
	maskedNull *vector.Vector
}

func (expr *UserVarAssignmentExpressionExecutor) Eval(
	proc *process.Process, batches []*batch.Batch, selectList []bool,
) (*vector.Vector, error) {
	if proc == nil || proc.Base == nil || !proc.Base.IsFrontend {
		return nil, moerr.NewNotSupported(nil,
			"user-variable assignment is supported only in frontend SELECT projections")
	}
	rowCount := expressionRowCount(batches)
	if noRowsSelected(selectList, rowCount) {
		if expr.maskedNull == nil {
			var err error
			expr.maskedNull, err = newExpressionConstNull(expr.typ, 1, expr.allocation)
			if err != nil {
				return nil, err
			}
		}
		expr.maskedNull.SetLength(rowCount)
		return expr.maskedNull, nil
	}
	if !allRowsSelected(selectList, rowCount) {
		return nil, moerr.NewNotSupported(proc.Ctx,
			"user-variable assignment does not support a filtered projection")
	}

	if expr.result != nil {
		expr.result.CleanOnlyData()
		expr.result.SetLength(0)
	} else {
		var err error
		expr.result, err = newExpressionOffHeapVector(expr.typ, expr.allocation)
		if err != nil {
			return nil, err
		}
	}
	proc.BeginUserVariableAssignment(expr.name, rowCount)
	previousRow := proc.GetUserVariableAssignmentCurrentRow()
	defer proc.SetUserVariableAssignmentCurrentRow(previousRow)

	for row := 0; row < rowCount; row++ {
		proc.SetUserVariableAssignmentCurrentRow(row)
		childVec, err := expr.child.Eval(proc, batches, nil)
		if err != nil {
			return nil, err
		}
		if childVec == nil || row >= childVec.Length() {
			return nil, moerr.NewInternalError(proc.Ctx,
				"user-variable assignment expression returned an invalid row")
		}
		if err = expr.result.UnionOne(childVec, int64(row), proc.Mp()); err != nil {
			return nil, err
		}
		value := process.UserVariableAssignment{
			Name:         expr.name,
			Type:         childVec.GetType().PlanType(),
			IsBin:        childVec.GetIsBinaryStringAt(row),
			StringDomain: childVec.GetRuntimeStringDomainAt(row),
		}
		if childVec.IsNull(uint64(row)) {
			value.Value = nil
		} else {
			value.Value = vector.GetAny(childVec, row, true)
		}
		proc.StageUserVariableAssignment(value, row)
	}
	return expr.result, nil
}

func (expr *UserVarAssignmentExpressionExecutor) EvalWithoutResultReusing(
	proc *process.Process, batches []*batch.Batch, selectList []bool,
) (*vector.Vector, error) {
	vec, err := expr.Eval(proc, batches, selectList)
	if err == nil && vec == expr.result {
		expr.result = nil
	}
	if err == nil && vec == expr.maskedNull {
		expr.maskedNull = nil
	}
	if err != nil && expr.maskedNull != nil {
		expr.maskedNull.Free(expr.mp)
		expr.maskedNull = nil
	}
	return vec, err
}

func (expr *UserVarAssignmentExpressionExecutor) ResetForNextQuery() {
	if expr.child != nil {
		expr.child.ResetForNextQuery()
	}
	if expr.result != nil {
		expr.result.Free(expr.mp)
		expr.result = nil
	}
	if expr.maskedNull != nil {
		expr.maskedNull.Free(expr.mp)
		expr.maskedNull = nil
	}
}

func (expr *UserVarAssignmentExpressionExecutor) Free() {
	if expr == nil {
		return
	}
	if expr.child != nil {
		expr.child.Free()
		expr.child = nil
	}
	if expr.result != nil {
		expr.result.Free(expr.mp)
		expr.result = nil
	}
	if expr.maskedNull != nil {
		expr.maskedNull.Free(expr.mp)
		expr.maskedNull = nil
	}
}

func (expr *UserVarAssignmentExpressionExecutor) IsColumnExpr() bool { return false }

func (expr *UserVarAssignmentExpressionExecutor) TypeName() string {
	return "user variable assignment " + strings.ToLower(expr.name)
}

func allRowsSelected(selectList []bool, rowCount int) bool {
	if len(selectList) == 0 {
		return true
	}
	if len(selectList) < rowCount {
		return false
	}
	for i := 0; i < rowCount; i++ {
		if !selectList[i] {
			return false
		}
	}
	return true
}

// evalStagedUserVariableValue materializes a scalar overlay value using the
// same conversion path as ordinary user-variable reads.
func evalStagedUserVariableValue(
	proc *process.Process,
	typ types.Type,
	value process.UserVariableAssignment,
	allocation *vector.AllocationAccountSelection,
	rowCount int,
) (*vector.Vector, error) {
	vec, err := util.GenVectorByVarValueWithAllocation(proc, typ, value.Value, allocation)
	if err != nil {
		return nil, err
	}
	vec.SetLength(rowCount)
	vec.SetIsBin(value.IsBin)
	if err = vec.SetRuntimeStringDomainWithMP(value.StringDomain, proc.Mp()); err != nil {
		vec.Free(proc.Mp())
		return nil, err
	}
	if err = vec.SetStringSource(types.StringSourceUserVariable); err != nil {
		vec.Free(proc.Mp())
		return nil, err
	}
	return vec, nil
}

func evalStagedUserVariableRows(
	proc *process.Process,
	typ types.Type,
	rows []process.UserVariableAssignment,
	allocation *vector.AllocationAccountSelection,
) (*vector.Vector, error) {
	result, err := newExpressionOffHeapVector(typ, allocation)
	if err != nil {
		return nil, err
	}
	for _, value := range rows {
		one, oneErr := evalStagedUserVariableValue(proc, typ, value, allocation, 1)
		if oneErr != nil {
			result.Free(proc.Mp())
			return nil, oneErr
		}
		oneErr = result.UnionOne(one, 0, proc.Mp())
		one.Free(proc.Mp())
		if oneErr != nil {
			result.Free(proc.Mp())
			return nil, oneErr
		}
	}
	return result, nil
}
