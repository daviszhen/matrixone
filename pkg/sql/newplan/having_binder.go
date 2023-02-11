package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// BindExpr ->If astExpr in GroupByAst -> (groupTag,colPos)
//
//	If astExpr in AggregateByAst -> (aggregateTag,colPos)
//
// BindColRef -> Only inside Agg Func, Do base.colRef
// BindAggFunc -> need update aggregateByAst,aggregates
// Except Win,Subquery
func NewHavingBinder(qb *QueryBuilder, ctx *BindContext) *HavingBinder {
	b := &HavingBinder{
		insideAgg: false,
	}
	b.builder = qb
	b.ctx = ctx
	b.impl = b

	return b
}

// If astExpr in GroupByAst -> (groupTag,colPos)
// If astExpr in AggregateByAst -> (aggregateTag,colPos)
// Virtual ColRef (groupTag,groups[colPos]),(aggregateTag,aggregates[colPos])
func (hb *HavingBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	astStr := tree.String(astExpr, dialect.MYSQL)

	if !hb.insideAgg {
		//RelPos has been changed to groupTag
		if colPos, ok := hb.ctx.groupByAst[astStr]; ok {
			return &plan.Expr{
				Typ: hb.ctx.groups[colPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: hb.ctx.groupTag,
						ColPos: colPos,
					},
				},
			}, nil
		}
	}

	if colPos, ok := hb.ctx.aggregateByAst[astStr]; ok {
		if !hb.insideAgg {
			//RelPos has been changed to aggregateTag
			return &plan.Expr{
				Typ: hb.ctx.aggregates[colPos].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: hb.ctx.aggregateTag,
						ColPos: colPos,
					},
				},
			}, nil
		} else {
			return nil, moerr.NewInvalidInput("nestted aggregate function")
		}
	}

	return hb.baseBindExpr(astExpr, depth, isRoot)
}

// Only inside Agg Func, Do baseBindColRef
func (hb *HavingBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	if hb.insideAgg {
		expr, err := hb.baseBindColRef(astExpr, depth, isRoot)
		if err != nil {
			return nil, err
		}

		if _, ok := expr.Expr.(*plan.Expr_Corr); ok {
			return nil, moerr.NewNYI("correlated columns in aggregate function")
		}

		return expr, nil
	} else {
		return nil, moerr.NewSyntaxError("column %q must appear in the GROUP BY clause or be used in an aggregate function", tree.String(astExpr, dialect.MYSQL))
	}
}

// BindAggFunc -> need update aggregateByAst,aggregates
// Virtual ColRef (aggregateTag,aggregates[colPos])
func (hb *HavingBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	if hb.insideAgg {
		return nil, moerr.NewSyntaxError("aggregate function %s calls cannot be nested", funcName)
	}

	hb.insideAgg = true
	expr, err := hb.bindFuncExprImplByAstExpr(funcName, astExpr.Exprs, depth)
	if err != nil {
		return nil, err
	}
	if astExpr.Type == tree.FUNC_TYPE_DISTINCT {
		expr.GetF().Func.Obj = int64(int64(uint64(expr.GetF().Func.Obj) | function.Distinct))
	}
	hb.insideAgg = false

	colPos := int32(len(hb.ctx.aggregates))
	astStr := tree.String(astExpr, dialect.MYSQL)
	hb.ctx.aggregateByAst[astStr] = colPos
	hb.ctx.aggregates = append(hb.ctx.aggregates, expr)

	return &plan.Expr{
		Typ: expr.Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: hb.ctx.aggregateTag,
				ColPos: colPos,
			},
		},
	}, nil
}

func (hb *HavingBinder) BindWinFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	//TODO implement me
	panic("implement me")
}

func (hb *HavingBinder) BindSubquery(subquery *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return hb.baseBindSubquery(subquery, isRoot)
}
