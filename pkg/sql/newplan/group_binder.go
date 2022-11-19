package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewGroupBinder(qb *QueryBuilder, ctx *BindContext) *GroupBinder {
	b := &GroupBinder{}
	b.builder = qb
	b.ctx = ctx
	b.impl = b

	return b
}

func (gb *GroupBinder) BindExpr(expr tree.Expr, i int32, b bool) (*plan.Expr, error) {
	bindedExpr, err := gb.baseBindExpr(expr, i, b)
	if err != nil {
		return nil, err
	}
	if b {
		astStr := tree.String(expr, dialect.MYSQL)
		if _, ok := gb.ctx.groupByAst[astStr]; ok {
			return nil /*?*/, nil
		}

		gb.ctx.groupByAst[astStr] = int32(len(gb.ctx.groups))
		gb.ctx.groups = append(gb.ctx.groups, bindedExpr)
	}
	return bindedExpr, err
}

func (gb *GroupBinder) BindColRef(name *tree.UnresolvedName, i int32, b bool) (*plan.Expr, error) {
	bindedRef, err := gb.baseBindColRef(name, i, b)
	if err != nil {
		return nil, err
	}

	if _, ok := bindedRef.Expr.(*plan.Expr_Corr); ok {
		return nil, moerr.NewNYI("correlated columns in GROUP BY clause")
	}
	return bindedRef, nil
}

func (gb *GroupBinder) BindAggFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInput("GROUP BY clause cannot contain aggregate functions")
}

func (gb *GroupBinder) BindWinFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return nil, moerr.NewInvalidInput("GROUP BY clause cannot contain window functions")
}

func (gb *GroupBinder) BindSubquery(subquery *tree.Subquery, b bool) (*plan.Expr, error) {
	return nil, moerr.NewNYI("subquery in GROUP BY clause")
}
