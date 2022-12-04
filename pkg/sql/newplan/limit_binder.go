package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// BindExpr -> baseBindExpr
func NewLimitBinder(qb *QueryBuilder, ctx *BindContext) *LimitBinder {
	lb := &LimitBinder{}
	lb.builder = qb
	lb.ctx = ctx
	lb.impl = lb
	return lb
}

func (b *LimitBinder) BindExpr(astExpr tree.Expr, depth int32, isRoot bool) (*plan.Expr, error) {
	switch astExpr.(type) {
	case *tree.VarExpr, *tree.UnqualifiedStar:
		return nil, moerr.NewSyntaxError("unsupported expr in limit clause")
	}

	expr, err := b.baseBindExpr(astExpr, depth, isRoot)
	if err != nil {
		return nil, err
	}

	if expr.Typ.Id != int32(types.T_int64) {
		if expr.Typ.Id == int32(types.T_varchar) {
			targetType := types.T_int64.ToType()
			planTargetType := makePlan2Type(&targetType)
			var err error
			expr, err = appendCastBeforeExpr(expr, planTargetType)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, moerr.NewSyntaxError("only int64 support in limit/offset clause")
		}
	}

	return expr, nil
}

func (b *LimitBinder) BindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError("column not allowed in limit clause")
}

func (b *LimitBinder) BindAggFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError("aggregate function not allowed in limit clause")
}

func (b *LimitBinder) BindWinFunc(funcName string, astExpr *tree.FuncExpr, depth int32, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError("window function not allowed in limit clause")
}

func (b *LimitBinder) BindSubquery(astExpr *tree.Subquery, isRoot bool) (*plan.Expr, error) {
	return nil, moerr.NewSyntaxError("subquery not allowed in limit clause")
}
