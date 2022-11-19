package newplan

import (
	"go/constant"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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
		err = moerr.NewInternalError("not implement 1")
	case *tree.OrExpr:
		err = moerr.NewInternalError("not implement 2")
	case *tree.NotExpr:
		err = moerr.NewInternalError("not implement 3")
	case *tree.AndExpr:
		err = moerr.NewInternalError("not implement 4")
	case *tree.UnaryExpr:
		err = moerr.NewInternalError("not implement 5")
	case *tree.BinaryExpr:
		err = moerr.NewInternalError("not implement 6")
	case *tree.ComparisonExpr:
		err = moerr.NewInternalError("not implement 7")
	case *tree.FuncExpr:
		err = moerr.NewInternalError("not implement 8")
	case *tree.RangeCond:
		err = moerr.NewInternalError("not implement 9")
	case *tree.UnresolvedName:
		expr, err = b.impl.BindColRef(exprImpl, depth, isRoot)
	case *tree.CastExpr:
		err = moerr.NewInternalError("not implement 11")
	case *tree.IsNullExpr:
		err = moerr.NewInternalError("not implement 12")
	case *tree.IsNotNullExpr:
		err = moerr.NewInternalError("not implement 13")
	case *tree.Tuple:
		err = moerr.NewInternalError("not implement 14")
	case *tree.CaseExpr:
		err = moerr.NewInternalError("not implement 15")
	case *tree.IntervalExpr:
		err = moerr.NewInternalError("not implement 16")
	case *tree.XorExpr:
		err = moerr.NewInternalError("not implement 17")
	case *tree.Subquery:
		err = moerr.NewInternalError("not implement 18")
	case *tree.DefaultVal:
		err = moerr.NewInternalError("not implement 19")
	case *tree.MaxValue:
		err = moerr.NewInternalError("not implement 20")
	case *tree.VarExpr:
		err = moerr.NewInternalError("not implement 21")
	case *tree.ParamExpr:
		err = moerr.NewInternalError("not implement 22")
	case *tree.StrVal:
		err = moerr.NewInternalError("not implement 23")
	case *tree.ExprList:
		err = moerr.NewInternalError("not implement 24")
	case *tree.UnqualifiedStar:
		err = moerr.NewInvalidInput("SELECT clause contains unqualified star")
	default:
		err = moerr.NewNYI("expr '%+v'", exprImpl)
	}
	return
}

func (b *baseBinder) baseBindParam(astExpr *tree.ParamExpr, depth int32, isRoot bool) (expr *plan.Expr, err error) {
	return nil, moerr.NewInternalError("not implement 25")
}

func (b *baseBinder) baseBindVar(astExpr *tree.VarExpr, depth int32, isRoot bool) (expr *plan.Expr, err error) {
	return nil, moerr.NewInternalError("not implement 26")
}

func (b *baseBinder) baseBindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (expr *plan.Expr, err error) {
	if b.ctx == nil {
		return nil, moerr.NewInvalidInput("ambigous column reference '%v'", astExpr.Parts[0])
	}

	col := astExpr.Parts[0]
	table := astExpr.Parts[1]
	name := tree.String(astExpr, dialect.MYSQL)

	relPos := NotFound
	colPos := NotFound
	var typ *plan.Type

	if len(table) == 0 {
		if binding, ok := b.ctx.bindingByCol[col]; ok {
			if binding != nil {
				relPos = binding.tag
				colPos = binding.colIdByName[col]
				typ = binding.types[colPos]
				table = binding.table
			} else {
				return nil, moerr.NewInvalidInput("ambiguous column reference '%v'", name)
			}
		} else {
			err = moerr.NewInvalidInput("column %s does not exist", name)
		}
	} else {
		if binding, ok := b.ctx.bindingByTable[table]; ok {
			colPos = binding.FindColumn(col)
			if colPos == AmbiguousName {
				return nil, moerr.NewInvalidInput("ambiguous column reference '%v'", name)
			}

			if colPos != NotFound {
				typ = binding.types[colPos]
				relPos = binding.tag
			} else {
				err = moerr.NewInvalidInput("column '%s' does not exist", name)
			}
		} else {
			err = moerr.NewInvalidInput("missing FROM-clause entry for table '%v'", table)
		}
	}

	if colPos != NotFound {
		b.boundCols = append(b.boundCols, table+"."+col)

		expr = &plan.Expr{
			Typ: typ,
		}

		if depth == 0 {
			expr.Expr = &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: relPos,
					ColPos: colPos,
				},
			}
		} else {
			expr.Expr = &plan.Expr_Corr{
				Corr: &plan.CorrColRef{
					RelPos: relPos,
					ColPos: colPos,
					Depth:  depth,
				},
			}
		}

		return
	}

	parent := b.ctx.parent
	for parent != nil && parent.binder == nil {
		parent = parent.parent
	}

	if parent == nil {
		return
	}

	expr, err = parent.binder.BindColRef(astExpr, depth+1, isRoot)
	if err == nil {
		b.ctx.isCorrelated = true
	}

	return
}

func (b *baseBinder) baseBindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInternalError("not implement 28")
}

func (b *baseBinder) bindCaseExpr(astExpr *tree.CaseExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInternalError("not implement 29")
}

func (b *baseBinder) bindRangeCond(astExpr *tree.RangeCond, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInternalError("not implement 30")
}

func (b *baseBinder) bindUnaryExpr(astExpr *tree.UnaryExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInternalError("not implement 31")
}

func (b *baseBinder) bindBinaryExpr(astExpr *tree.BinaryExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInternalError("not implement 32")
}

func (b *baseBinder) bindComparisonExpr(astExpr *tree.ComparisonExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInternalError("not implement 33")
}

func (b *baseBinder) bindFuncExpr(astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInternalError("not implement 34")
}

func (b *baseBinder) bindFuncExprImplByAstExpr(name string, astArgs []tree.Expr, depth int32) (*plan.Expr, error) {
	return nil, moerr.NewInternalError("not implement 35")
}

func bindFuncExprImplByPlanExpr(name string, args []*plan.Expr) (*plan.Expr, error) {
	return nil, moerr.NewInternalError("not implement 36")
}

func (b *baseBinder) bindNumVal(astExpr *tree.NumVal, typ *plan.Type) (*plan.Expr, error) {
	//getStringExpr := func(val string) *plan.Expr {
	//	return &plan.Expr{
	//		Expr: &plan.Expr_C{
	//			C: &plan.Const{
	//				Isnull: false,
	//				Value: &plan.Const_Sval{
	//					Sval: val,
	//				},
	//			},
	//		},
	//		Typ: &plan.Type{
	//			Id:       int32(types.T_varchar),
	//			Nullable: false,
	//			Size:     4,
	//			Width:    int32(len(val)),
	//		},
	//	}
	//}

	//returnDecimalExpr := func(val string) (*plan.Expr, error) {
	//	if typ != nil {
	//		return appendCastBeforeExpr(getStringExpr(val), typ)
	//	}
	//
	//	_, scale, err := types.ParseStringToDecimal128WithoutTable(val)
	//	if err != nil {
	//		return nil, err
	//	}
	//	typ := &plan.Type{
	//		Id:        int32(types.T_decimal128),
	//		Width:     34,
	//		Scale:     scale,
	//		Precision: 34,
	//		Nullable:  false,
	//	}
	//	return appendCastBeforeExpr(getStringExpr(val), typ)
	//}

	switch astExpr.ValType {
	case tree.P_null:
		return &plan.Expr{
			Expr: &plan.Expr_C{
				C: &plan.Const{
					Isnull: true,
				},
			},
			Typ: &plan.Type{
				Id:          int32(types.T_any),
				NotNullable: true,
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
				Id:          int32(types.T_bool),
				NotNullable: false,
				Size:        1,
			},
		}, nil
	default:
		return nil, moerr.NewInvalidInput("unsupport value '%s'", astExpr.String())
	}
}

func appendCastBeforeExpr(expr *plan.Expr, toType *plan.Type) (*plan.Expr, error) {
	return nil, moerr.NewInternalError("not implement 37")
}

func resetDateFunctionArgs(dateExpr *plan.Expr, intervalExpr *plan.Expr) ([]*plan.Expr, error) {
	return nil, moerr.NewInternalError("not implement 38")
}

func resetDateFunctionArgs2(dateExpr *plan.Expr, intervalExpr *plan.Expr) ([]*plan.Expr, error) {
	return nil, moerr.NewInternalError("not implement 39")
}
