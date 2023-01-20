package plan

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_bindFuncExprImplByPlanExpr_ceil(t *testing.T) {
	ctx := context.Background()
	_, err := bindFuncExprImplByPlanExpr(ctx, "ceil", nil)
	assert.Error(t, err)

	qb := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext())
	bc := &BindContext{}
	pb := NewProjectionBinder(qb, bc, nil)

	ast, err := parsers.ParseOne(ctx, dialect.MYSQL, `select "1.234"`)
	assert.NoError(t, err)
	sel := ast.(*tree.Select)
	sc := sel.Select.(*tree.SelectClause)
	numExpr, err := pb.BindExpr(sc.Exprs[0].Expr, 0, true)
	assert.NoError(t, err)
	ceilExpr, err := bindFuncExprImplByPlanExpr(ctx, "ceil", []*Expr{numExpr})
	assert.NoError(t, err)
	funcExpr := ceilExpr.GetF().GetArgs()[0].GetF()
	assert.Equal(t, funcExpr.GetFunc().GetObjName(), "cast")
	assert.Equal(t, funcExpr.GetArgs()[0].GetC().GetSval(), "1.234")
	typ := types.T_float64.ToType()
	planTyp := makePlan2Type(&typ)
	assert.Equal(t, funcExpr.GetArgs()[1].GetT().GetTyp().GetId(), planTyp.GetId())
}
