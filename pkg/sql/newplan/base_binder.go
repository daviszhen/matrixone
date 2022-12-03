package newplan

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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
		expr, err = b.impl.BindExpr(exprImpl.Expr, depth, isRoot)
	case *tree.OrExpr:
		err = moerr.NewInternalError("not implement 2")
	case *tree.NotExpr:
		err = moerr.NewInternalError("not implement 3")
	case *tree.AndExpr:
		err = moerr.NewInternalError("not implement 4")
	case *tree.UnaryExpr:
		err = moerr.NewInternalError("not implement 5")
	case *tree.BinaryExpr:
		expr, err = b.bindBinaryExpr(exprImpl, depth, isRoot)
	case *tree.ComparisonExpr:
		err = moerr.NewInternalError("not implement 7")
	case *tree.FuncExpr:
		expr, err = b.bindFuncExpr(exprImpl, depth, isRoot)
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
	switch astExpr.Op {
	case tree.PLUS:
		return b.bindFuncExprImplByAstExpr("+", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.MINUS:
		return b.bindFuncExprImplByAstExpr("-", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.MULTI:
		return b.bindFuncExprImplByAstExpr("*", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.MOD:
		return b.bindFuncExprImplByAstExpr("%", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.DIV:
		return b.bindFuncExprImplByAstExpr("/", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.INTEGER_DIV:
		return b.bindFuncExprImplByAstExpr("div", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.BIT_XOR:
		return b.bindFuncExprImplByAstExpr("^", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.BIT_OR:
		return b.bindFuncExprImplByAstExpr("|", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.BIT_AND:
		return b.bindFuncExprImplByAstExpr("&", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.LEFT_SHIFT:
		return b.bindFuncExprImplByAstExpr("<<", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.RIGHT_SHIFT:
		return b.bindFuncExprImplByAstExpr(">>", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	}
	return nil, moerr.NewNYI("'%v' operator", astExpr.Op.ToString())
}

func (b *baseBinder) bindComparisonExpr(astExpr *tree.ComparisonExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewInternalError("not implement 33")
}

func (b *baseBinder) bindFuncExpr(astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	funcRef, ok := astExpr.Func.FunctionReference.(*tree.UnresolvedName)
	if !ok {
		return nil, moerr.NewNYI("function expr '%v'", astExpr)
	}
	funcName := funcRef.Parts[0]
	if function.GetFunctionIsAggregateByName(funcName) {
		return b.impl.BindAggFunc(funcName, astExpr, depth, isRoot)
	} else if function.GetFunctionIsWinfunByName(funcName) {
		return b.impl.BindWinFunc(funcName, astExpr, depth, isRoot)
	}

	return b.bindFuncExprImplByAstExpr(funcName, astExpr.Exprs, depth)
}

func (b *baseBinder) bindFuncExprImplByAstExpr(name string, astArgs []tree.Expr, depth int32) (*plan.Expr, error) {
	args := make([]*plan.Expr, len(astArgs))
	for idx, arg := range astArgs {
		expr, err := b.impl.BindExpr(arg, depth, false)
		if err != nil {
			return nil, err
		}
		args[idx] = expr
	}
	return bindFuncExprImplByPlanExpr(name, args)
}

func bindFuncExprImplByPlanExpr(name string, args []*plan.Expr) (*plan.Expr, error) {
	var err error
	switch name {
	case "-":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg("operator - need two args", len(args))
		}
		if isNullExpr(args[0]) {
			return args[0], nil
		}
		if isNullExpr(args[1]) {
			return args[1], nil
		}
	case "*":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(fmt.Sprintf("operator %s need two args", name), len(args))
		}
		if isNullExpr(args[0]) {
			return args[0], nil
		}
		if isNullExpr(args[1]) {
			return args[1], nil
		}
	}
	argsLength := len(args)
	argsType := make([]types.Type, argsLength)
	for idx, expr := range args {
		argsType[idx] = makeTypeByPlan2Expr(expr)
	}

	var funcID int64
	var returnType types.Type
	var argsCastType []types.Type

	funcID, returnType, argsCastType, err = function.GetFunctionByName(name, argsType)
	if err != nil {
		return nil, err
	}
	if function.GetFunctionIsAggregateByName(name) {
		if constExpr, ok := args[0].Expr.(*plan.Expr_C); ok && constExpr.C.Isnull {
			args[0].Typ = makePlan2Type(&returnType)
		}
	}

	if len(argsCastType) != 0 {
		if len(argsCastType) != argsLength {
			return nil, moerr.NewInvalidArg("cast types length not match args length", "")
		}
		for idx, castType := range argsCastType {
			if !argsType[idx].Eq(castType) && castType.Oid != types.T_any {
				typ := makePlan2Type(&castType)
				args[idx], err = appendCastBeforeExpr(args[idx], typ)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	if function.GetFunctionAppendHideArgByID(funcID) {
		args = append(args, makePlan2NullConstExprWithType())
	}

	Typ := makePlan2Type(&returnType)
	Typ.NotNullable = function.DeduceNotNullable(funcID, args)
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(funcID, name),
				Args: args,
			},
		},
		Typ: Typ,
	}, nil
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
	case tree.P_int64:
		val, ok := constant.Int64Val(astExpr.Value)
		if !ok {
			return nil, moerr.NewInvalidInput("invalid int value '%s'", astExpr.Value.String())
		}
		expr := makePlan2Int64ConstExprWithType(val)
		if typ != nil && typ.Id == int32(types.T_varchar) {
			return appendCastBeforeExpr(expr, typ)
		}
		return expr, nil
	default:
		return nil, moerr.NewInvalidInput("unsupport value '%s'", astExpr.String())
	}
}

func appendCastBeforeExpr(expr *plan.Expr, toType *plan.Type, isBin ...bool) (*plan.Expr, error) {
	if expr.Typ.Id == int32(types.T_any) {
		return expr, nil
	}
	toType.NotNullable = expr.Typ.NotNullable
	argsType := []types.Type{
		makeTypeByPlan2Expr(expr),
		makeTypeByPlan2Type(toType),
	}
	funcID, _, _, err := function.GetFunctionByName("cast", argsType)
	if err != nil {
		return nil, err
	}
	typ := *toType
	if len(isBin) == 2 && isBin[0] && isBin[1] {
		typ.Id = int32(types.T_uint64)
	}
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(funcID, "cast"),
				Args: []*plan.Expr{expr, {
					Expr: &plan.Expr_T{
						T: &plan.TargetType{
							Typ: &typ,
						},
					},
				}},
			},
		},
		Typ: &typ,
	}, nil
}

func resetDateFunctionArgs(dateExpr *plan.Expr, intervalExpr *plan.Expr) ([]*plan.Expr, error) {
	return nil, moerr.NewInternalError("not implement 38")
}

func resetDateFunctionArgs2(dateExpr *plan.Expr, intervalExpr *plan.Expr) ([]*plan.Expr, error) {
	return nil, moerr.NewInternalError("not implement 39")
}
