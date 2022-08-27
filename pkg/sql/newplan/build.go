package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

type Binder interface {
	BindExpr(tree.Expr, int32, bool) (*plan.Expr, error)
	BindColRef(*tree.UnresolvedName, int32, bool) (*plan.Expr, error)
	BindAggFunc(string, *tree.FuncExpr, int32, bool) (*plan.Expr, error)
	BindWinFunc(string, *tree.FuncExpr, int32, bool) (*plan.Expr, error)
	BindSubquery(*tree.Subquery, bool) (*plan.Expr, error)
}

type BindContext struct {
	binder          Binder
	parent          *BindContext
	defaultDatabase string
}

type QueryBuilder struct {
	qry     *plan.Query
	compCtx plan2.CompilerContext

	ctxByNode    []*BindContext
	nameByColRef map[[2]int32]string

	nextTag int32
}

func runBuildSelectByBinder(stmtType plan.Query_StatementType, ctx plan2.CompilerContext, stmt *tree.Select) (*plan.Plan, error) {
	builder := NewQueryBuilder(stmtType, ctx)
	bc := NewBindContext(nil)
	rootId, err := builder.buildSelect(stmt, bc, true)
	builder.qry.Steps = append(builder.qry.Steps, rootId)
	if err != nil {
		return nil, err
	}
	query, err := builder.createQuery()
	if err != nil {
		return nil, err
	}
	return &plan.Plan{Plan: &plan.Plan_Query{
		Query: query,
	}}, err
}

func BuildPlan(ctx plan2.CompilerContext, stmt tree.Statement) (*plan.Plan, error) {
	switch stmt := stmt.(type) {
	case *tree.Select:
		return runBuildSelectByBinder(plan.Query_SELECT, ctx, stmt)
	}
	return nil, nil
}
