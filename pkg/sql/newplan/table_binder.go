package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewTableBinder(qb *QueryBuilder, ctx *BindContext) *TableBinder {
	b := &TableBinder{}
	b.builder = qb
	b.ctx = ctx
	b.impl = b
	return b
}

func (tb *TableBinder) BindExpr(expr tree.Expr, i int32, b bool) (*plan.Expr, error) {
	return tb.baseBindExpr(expr, i, b)
}

func (tb *TableBinder) BindColRef(name *tree.UnresolvedName, i int32, b bool) (*plan.Expr, error) {
	return tb.baseBindColRef(name, i, b)
}

func (tb *TableBinder) BindAggFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError("aggregate function %s not allowed", s)
}

func (tb *TableBinder) BindWinFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError("window function %s not allowed", s)
}

func (tb *TableBinder) BindSubquery(subquery *tree.Subquery, b bool) (*plan.Expr, error) {
	return nil, moerr.NewNYI("subquery in JOIN condition")
}
