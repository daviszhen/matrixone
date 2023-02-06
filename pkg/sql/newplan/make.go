package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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

func makePlan2StringConstExpr(v string, isBin ...bool) *plan.Expr_C {
	c := &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_Sval{
			Sval: v,
		},
	}}
	if len(isBin) > 0 {
		c.C.IsBin = isBin[0]
	}
	return c
}

func makePlan2StringConstExprWithType(v string, isBin ...bool) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2StringConstExpr(v, isBin...),
		Typ: &plan.Type{
			Id:          int32(types.T_varchar),
			NotNullable: true,
			Size:        4,
			Width:       int32(len(v)),
		},
	}
}

func makePlan2CastExpr(expr *plan.Expr, targetType *plan.Type) (*plan.Expr, error) {
	if isSameColumnType(expr.Typ, targetType) {
		return expr, nil
	}
	targetType.NotNullable = expr.Typ.NotNullable
	t1, t2 := makeTypeByPlan2Expr(expr), makeTypeByPlan2Type(targetType)
	if types.T(expr.Typ.Id) == types.T_any {
		expr.Typ = targetType
		return expr, nil
	}
	id, _, _, err := function.GetFunctionByName("cast", []types.Type{t1, t2})
	if err != nil {
		return nil, err
	}
	t := &plan.Expr{Expr: &plan.Expr_T{T: &plan.TargetType{
		Typ: targetType,
	}}}
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{Obj: id, ObjName: "cast"},
				Args: []*plan.Expr{expr, t},
			},
		},
		Typ: targetType,
	}, nil
}

func makePlan2DecimalExprWithType(v string, isBin ...bool) (*plan.Expr, error) {
	_, scale, err := types.ParseStringToDecimal128WithoutTable(v, isBin...)
	if err != nil {
		return nil, err
	}
	typ := &plan.Type{
		Id:          int32(types.T_decimal128),
		Width:       34,
		Scale:       scale,
		Precision:   34,
		NotNullable: true,
	}
	return appendCastBeforeExpr(makePlan2StringConstExprWithType(v, isBin...), typ)
}

func makePlan2Float64ConstExpr(v float64) *plan.Expr_C {
	return &plan.Expr_C{C: &plan.Const{
		Isnull: false,
		Value: &plan.Const_Dval{
			Dval: v,
		},
	}}
}

func makePlan2Float64ConstExprWithType(v float64) *plan.Expr {
	return &plan.Expr{
		Expr: makePlan2Float64ConstExpr(v),
		Typ: &plan.Type{
			Id:          int32(types.T_float64),
			NotNullable: true,
			Size:        8,
		},
	}
}
