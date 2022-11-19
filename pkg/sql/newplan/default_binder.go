package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewDefaultBinder(qb *QueryBuilder, ctx *BindContext, typ *plan.Type) *DefaultBinder {
	b := &DefaultBinder{typ: typ}
	b.builder = qb
	b.ctx = ctx
	b.impl = b
	return b
}

func (db *DefaultBinder) BindExpr(expr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	return db.baseBindExpr(expr, depth, isRoot)
}

func (db *DefaultBinder) BindColRef(name *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	return db.baseBindColRef(name, depth, isRoot)
}

func (db *DefaultBinder) BindAggFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return nil, errors.New(errno.GroupingError, "aggregate functions not allowed here")
}

func (db *DefaultBinder) BindWinFunc(s string, expr *tree.FuncExpr, i int32, b bool) (*plan.Expr, error) {
	return nil, errors.New(errno.WindowingError, "window functions not allowed here")
}

func (db *DefaultBinder) BindSubquery(subquery *tree.Subquery, b bool) (*plan.Expr, error) {
	return nil, errors.New(errno.WindowingError, "subquery in JOIN condition not yet supported")
}
