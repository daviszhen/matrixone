package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

func NewQueryBuilder(queryType plan.Query_StatementType, ctx plan2.CompilerContext) *QueryBuilder {
	return &QueryBuilder{
		qry:          &plan.Query{StmtType: queryType},
		compCtx:      ctx,
		ctxByNode:    []*BindContext{},
		nameByColRef: make(map[[2]int32]string),
		nextTag:      0,
	}
}

func (qb *QueryBuilder) buildSelect(stmt *tree.Select, ctx *BindContext, isRoot bool) (int32, error) {
	var clause *tree.SelectClause
	switch selectClause := stmt.Select.(type) {
	case *tree.SelectClause:
		clause = selectClause
	}

	_, err := qb.buildFrom(clause.From.Tables, ctx)
	if err != nil {
		return 0, err
	}
	panic("TODO")
	return 0, nil
}

func (qb *QueryBuilder) buildFrom(stmt tree.TableExprs, ctx *BindContext) (int32, error) {
	panic("TODO")
	return 0, nil
}

func (qb *QueryBuilder) createQuery() (*plan.Query, error) {
	panic("TODO")
	return nil, nil
}
