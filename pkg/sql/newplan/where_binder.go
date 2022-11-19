package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewWhereBinder(qb *QueryBuilder, ctx *BindContext) *WhereBinder {
	wb := &WhereBinder{}
	wb.builder = qb
	wb.ctx = ctx
	wb.impl = wb

	return wb
}

func (wb *WhereBinder) BindExpr(expr tree.Expr, i int32, b bool) (*plan.Expr, error) {
	return wb.baseBindExpr(expr, i, b)
}

func (wb *WhereBinder) BindColRef(name *tree.UnresolvedName, i int32, b bool) (*plan.Expr, error) {
	return wb.baseBindColRef(name, i, b)
}

func (wb *WhereBinder) BindAggFunc(funcName string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError("aggregate function %s not allowed in WHERE clause", funcName)
}

func (wb *WhereBinder) BindWinFunc(funcName string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError("window function %s not allowed in WHERE clause", funcName)
}

func (wb *WhereBinder) BindSubquery(subquery *tree.Subquery, b bool) (*plan.Expr, error) {
	return wb.baseBindSubquery(subquery, b)
}
