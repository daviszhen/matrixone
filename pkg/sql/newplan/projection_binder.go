package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewProjectionBinder(qb *QueryBuilder, ctx *BindContext, havingBinder *HavingBinder) *ProjectionBinder {
	b := &ProjectionBinder{
		havingBinder: havingBinder,
	}
	b.builder = qb
	b.ctx = ctx
	b.impl = b

	return b
}

func (b *ProjectionBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	astStr := tree.String(astExpr, dialect.MYSQL)

	if colPos, ok := b.ctx.groupByAst[astStr]; ok {
		return &plan.Expr{
			Typ: b.ctx.groups[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: b.ctx.groupTag,
					ColPos: colPos,
				},
			},
		}, nil
	}

	if colPos, ok := b.ctx.aggregateByAst[astStr]; ok {
		return &plan.Expr{
			Typ: b.ctx.aggregates[colPos].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: b.ctx.aggregateTag,
					ColPos: colPos,
				},
			},
		}, nil
	}

	return b.baseBindExpr(astExpr, depth, isRoot)
}

func (b *ProjectionBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	return b.baseBindColRef(astExpr, depth, isRoot)
}

func (b *ProjectionBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return b.havingBinder.BindAggFunc(funcName, astExpr, depth, isRoot)
}

func (b *ProjectionBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewNYI("window functions")
}

func (b *ProjectionBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return b.baseBindSubquery(astExpr, isRoot)
}
