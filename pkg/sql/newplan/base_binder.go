package newplan

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"go/constant"
)

func (b *baseBinder) baseBindExpr(astExpr tree.Expr, depth int32, isRoot bool) (expr *plan.Expr, err error) {
	switch exprImpl := astExpr.(type) {
	case *tree.NumVal:
		if d, ok := b.impl.(*DefaultBinder); ok {
			expr, err = b.bindNumVal(exprImpl, d.typ)
		} else {
			expr, err = b.bindNumVal(exprImpl, nil)
		}
	case *tree.ParenExpr:
	case *tree.OrExpr:
	case *tree.NotExpr:
	case *tree.AndExpr:
	case *tree.UnaryExpr:
	case *tree.BinaryExpr:
	case *tree.ComparisonExpr:
	case *tree.FuncExpr:
	case *tree.RangeCond:
	case *tree.UnresolvedName:
	case *tree.CastExpr:
	case *tree.IsNullExpr:
	case *tree.IsNotNullExpr:
	case *tree.Tuple:
	case *tree.CaseExpr:
	case *tree.IntervalExpr:
	case *tree.XorExpr:
	case *tree.Subquery:
	case *tree.DefaultVal:
	case *tree.MaxValue:
	case *tree.VarExpr:
	case *tree.ParamExpr:
	case *tree.StrVal:
	case *tree.ExprList:
	case *tree.UnqualifiedStar:
		err = errors.New("", "unqualified star should only appear in SELECT clause")
	default:
		err = errors.New("", fmt.Sprintf("expr '%+v' is not supported now", exprImpl))
	}
	return
}

func (b *baseBinder) baseBindParam(astExpr *tree.ParamExpr, depth int32, isRoot bool) (expr *plan.Expr, err error) {
}

func (b *baseBinder) baseBindVar(astExpr *tree.VarExpr, depth int32, isRoot bool) (expr *plan.Expr, err error) {
}

func (b *baseBinder) baseBindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (expr *plan.Expr, err error) {
}

func (b *baseBinder) baseBindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {}

func (b *baseBinder) bindCaseExpr(astExpr *tree.CaseExpr, depth int32, isRoot bool) (*plan.Expr, error) {
}

func (b *baseBinder) bindRangeCond(astExpr *tree.RangeCond, depth int32, isRoot bool) (*plan.Expr, error) {
}

func (b *baseBinder) bindUnaryExpr(astExpr *tree.UnaryExpr, depth int32, isRoot bool) (*plan.Expr, error) {
}

func (b *baseBinder) bindBinaryExpr(astExpr *tree.BinaryExpr, depth int32, isRoot bool) (*plan.Expr, error) {
}

func (b *baseBinder) bindComparisonExpr(astExpr *tree.ComparisonExpr, depth int32, isRoot bool) (*plan.Expr, error) {
}

func (b *baseBinder) bindFuncExpr(astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
}

func (b *baseBinder) bindFuncExprImplByAstExpr(name string, astArgs []tree.Expr, depth int32) (*plan.Expr, error) {
}

func bindFuncExprImplByPlanExpr(name string, args []*plan.Expr) (*plan.Expr, error) {}

func (b *baseBinder) bindNumVal(astExpr *tree.NumVal, typ *plan.Type) (*plan.Expr, error) {
	getStringExpr := func(val string) *plan.Expr {
		return &plan.Expr{
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Isnull: false,
					Value: &plan.Const_Sval{
						Sval: val,
					},
				},
			},
			Typ: &plan.Type{
				Id:       int32(types.T_varchar),
				Nullable: false,
				Size:     4,
				Width:    int32(len(val)),
			},
		}
	}

	returnDecimalExpr := func(val string) (*plan.Expr, error) {
		if typ != nil {
			return appendCastBeforeExpr(getStringExpr(val), typ)
		}

		_, scale, err := types.ParseStringToDecimal128WithoutTable(val)
		if err != nil {
			return nil, err
		}
		typ := &plan.Type{
			Id:        int32(types.T_decimal128),
			Width:     34,
			Scale:     scale,
			Precision: 34,
			Nullable:  false,
		}
		return appendCastBeforeExpr(getStringExpr(val), typ)
	}

	switch astExpr.ValType {
	case tree.P_null:
		return &plan.Expr{
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Isnull: true,
				},
			},
			Typ: &plan.Type{
				Id:       int32(types.T_any),
				Nullable: true,
			},
		}, nil
	case tree.P_bool:
		val := constant.BoolVal(astExpr.Value)
		return &plan.Expr{
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Isnull: false,
					Value: &plan.Const_Bval{
						Bval: val,
					},
				},
			},
			Typ: &plan.Type{
				Id:       int32(types.T_bool),
				Nullable: false,
				Size:     1,
			},
		}, nil
	}
}

func appendCastBeforeExpr(expr *plan.Expr, toType *plan.Type) (*plan.Expr, error) {}

func resetDateFunctionArgs(dateExpr *plan.Expr, intervalExpr *plan.Expr) ([]*plan.Expr, error) {}

func resetDateFunctionArgs2(dateExpr *plan.Expr, intervalExpr *plan.Expr) ([]*plan.Expr, error) {}
