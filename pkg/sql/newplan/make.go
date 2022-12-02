package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func makePlan2Type(typ *types.Type) *plan.Type {
	return &plan.Type{
		Id:        int32(typ.Oid),
		Width:     typ.Width,
		Precision: typ.Precision,
		Size:      typ.Size,
		Scale:     typ.Scale,
	}
}

// ? plan.Type -> executor.Type
func makeTypeByPlan2Expr(expr *plan.Expr) types.Type {
	var size int32 = 0
	oid := types.T(expr.Typ.Id)
	if oid != types.T_any && oid != types.T_interval {
		size = int32(oid.TypeLen())
	}
	return types.Type{
		Oid:       oid,
		Size:      size,
		Width:     expr.Typ.Width,
		Scale:     expr.Typ.Scale,
		Precision: expr.Typ.Precision,
	}
}

func makePlan2NullConstExprWithType() *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_C{
			C: &plan.Const{
				Isnull: true,
			},
		},
		Typ: &plan.Type{
			Id:          int32(types.T_any),
			NotNullable: false,
		},
	}
}

func makePlan2Int64ConstExpr(v int64) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_I64Val{
			I64Val: v,
		},
	}}
}

func makePlan2Int64ConstExprWithType(v int64) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Int64ConstExpr(v),
		Typ: &plan.Type{
			Id:          int32(types.T_int64),
			NotNullable: true,
			Size:        8,
		},
	}
}

func makeTypeByPlan2Type(typ *plan.Type) types.Type {
	return types.Type{
		Oid:       types.T(typ.Id),
		Size:      typ.Size,
		Width:     typ.Width,
		Scale:     typ.Scale,
		Precision: typ.Precision,
	}
}
