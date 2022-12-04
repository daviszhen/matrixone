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

func convertValueIntoBool(name string, args []*plan.Expr, isLogic bool) error {
	if !isLogic && (len(args) != 2 || (args[0].Typ.Id != int32(types.T_bool) && args[1].Typ.Id != int32(types.T_bool))) {
		return nil
	}
	for _, arg := range args {
		if arg.Typ.Id == int32(types.T_bool) {
			continue
		}
		switch ex := arg.Expr.(type) {
		case *plan.Expr_C:
			switch value := ex.C.Value.(type) {
			case *plan.Const_I64Val:
				if value.I64Val == 0 {
					ex.C.Value = &plan.Const_Bval{Bval: false}
				} else {
					ex.C.Value = &plan.Const_Bval{Bval: true}
				}
				arg.Typ.Id = int32(types.T_bool)
			}
		}
	}
	return nil
}

func isSameColumnType(t1 *plan.Type, t2 *plan.Type) bool {
	if t1.Id != t2.Id {
		return false
	}
	if t1.Width == t2.Width && t1.Precision == t2.Precision && t1.Size == t2.Size && t1.Scale == t2.Scale {
		return true
	}
	return true
}
