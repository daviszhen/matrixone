package newplan

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
)

const (
	NotFound      int32 = math.MaxInt32
	AmbiguousName       = math.MaxInt32
)

type Binder interface {
	BindExpr(tree.Expr, int32, bool) (*plan.Expr, error)
	BindColRef(*tree.UnresolvedName, int32, bool) (*plan.Expr, error)
	BindAggFunc(string, *tree.FuncExpr, int32, bool) (*plan.Expr, error)
	BindWinFunc(string, *tree.FuncExpr, int32, bool) (*plan.Expr, error)
	BindSubquery(*tree.Subquery, bool) (*plan.Expr, error)
}

var _ Binder = &DefaultBinder{}
var _ Binder = &TableBinder{}
var _ Binder = &WhereBinder{}
var _ Binder = &GroupBinder{}
var _ Binder = &HavingBinder{}

type baseBinder struct {
	builder   *QueryBuilder
	ctx       *BindContext
	impl      Binder
	boundCols []string
}

type DefaultBinder struct {
	baseBinder
	typ *plan.Type
}

type TableBinder struct {
	baseBinder
}

type WhereBinder struct {
	baseBinder
}

type GroupBinder struct {
	baseBinder
}

type HavingBinder struct {
	baseBinder
	insideAgg bool
}

type ProjectionBinder struct {
	baseBinder
	havingBinder *HavingBinder
}

type OrderBinder struct {
	*ProjectionBinder
	selectList tree.SelectExprs
}

type LimitBinder struct {
	baseBinder
}

type BindContext struct {
	binder          Binder
	parent          *BindContext
	defaultDatabase string
	hasSingleRow    bool

	bindings       []*Binding
	bindingByTag   map[int32]*Binding
	bindingByTable map[string]*Binding
	bindingByCol   map[string]*Binding

	bindingTree *BindingTreeNode

	headings []string
	aliasMap map[string]int32

	groupTag     int32
	aggregateTag int32
	projectTag   int32

	groupByAst map[string]int32
	groups     []*plan.Expr

	aggregateByAst map[string]int32
	aggregates     []*plan.Expr

	projects      []*plan.Expr
	projectByExpr map[string]int32

	isDistinct   bool
	isCorrelated bool

	results []*plan.Expr

	resultTag int32
}

type NameTuple struct {
	table string
	col   string
}

type BindingTreeNode struct {
	using   []NameTuple
	binding *Binding
	left    *BindingTreeNode
	right   *BindingTreeNode
}

type QueryBuilder struct {
	qry     *plan.Query
	compCtx plan2.CompilerContext

	ctxByNode    []*BindContext
	nameByColRef map[[2]int32]string

	nextTag int32
}

type Binding struct {
	tag         int32
	nodeId      int32
	table       string
	cols        []string
	types       []*plan.Type
	refCnts     []uint
	colIdByName map[string]int32
}

func (b *Binding) FindColumn(col string) int32 {
	if id, ok := b.colIdByName[col]; ok {
		return id
	}

	return NotFound
}

func NewBind(tag, nodeID int32, table string, cols []string, types []*plan.Type) *Binding {
	binding := &Binding{
		tag:     tag,
		nodeId:  nodeID,
		table:   table,
		cols:    cols,
		types:   types,
		refCnts: make([]uint, len(cols)),
	}

	binding.colIdByName = make(map[string]int32)
	for i, col := range cols {
		if _, ok := binding.colIdByName[col]; ok {
			binding.colIdByName[col] = AmbiguousName
		} else {
			binding.colIdByName[col] = int32(i)
		}
	}

	return binding
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

func (bc *BindContext) mergeContexts(left, right *BindContext) error {
	panic("TODO")
}
