package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func isNullExpr(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}

	switch ef := expr.Expr.(type) {
	case *plan.Expr_C:
		return expr.Typ.Id == int32(types.T_any) && ef.C.Isnull
	default:
		return false
	}
}

func getFunctionObjRef(funcID int64, name string) *plan.ObjectRef {
	return &plan.ObjectRef{
		Obj:     funcID,
		ObjName: name,
	}
}
