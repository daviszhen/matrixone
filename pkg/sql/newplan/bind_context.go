package newplan

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"sync/atomic"
)

var (
	bindCtxCounter atomic.Uint32
)

func getBindCtxCounter() uint32 {
	return bindCtxCounter.Add(1)
}

func NewBindContext(parent *BindContext) *BindContext {
	bc := &BindContext{
		parent:         parent,
		id:             getBindCtxCounter(),
		bindingByTag:   make(map[int32]*Binding),
		bindingByTable: make(map[string]*Binding),
		bindingByCol:   make(map[string]*Binding),
		aliasMap:       make(map[string]int32),
		groupByAst:     make(map[string]int32),
		aggregateByAst: make(map[string]int32),
		projectByExpr:  make(map[string]int32),
	}
	fmt.Println("NewBindCountext", bc.id)
	if parent != nil {
		bc.defaultDatabase = parent.defaultDatabase
	}
	return bc
}

// make every column name have table prefix.
// astExpr - unqualified
// selectList - qualified select list
func (bc *BindContext) qualifyColumnNames(astExpr tree.Expr, selectList tree.SelectExprs, expandAlias bool) (tree.Expr, error) {
	var err error
	switch exprImpl := astExpr.(type) {
	case *tree.ParenExpr:
		astExpr, err = bc.qualifyColumnNames(exprImpl.Expr, selectList, expandAlias)

	case *tree.OrExpr:
		return nil, moerr.NewInternalError("not implement qualifyColumnNames 1")
	case *tree.NotExpr:
		return nil, moerr.NewInternalError("not implement qualifyColumnNames 2")
	case *tree.AndExpr:
		return nil, moerr.NewInternalError("not implement qualifyColumnNames 3")
	case *tree.UnaryExpr:
		return nil, moerr.NewInternalError("not implement qualifyColumnNames 4")
	case *tree.ComparisonExpr:
		exprImpl.Left, err = bc.qualifyColumnNames(exprImpl.Left, selectList, expandAlias)
		if err != nil {
			return nil, err
		}

		exprImpl.Right, err = bc.qualifyColumnNames(exprImpl.Right, selectList, expandAlias)
	case *tree.RangeCond:
		return nil, moerr.NewInternalError("not implement qualifyColumnNames 6")
	case *tree.UnresolvedName:
		if !exprImpl.Star && exprImpl.NumParts == 1 {
			col := exprImpl.Parts[0]
			if expandAlias { //orderBy use the alias in project list first
				if colPos, ok := bc.aliasMap[col]; ok {
					astExpr = selectList[colPos].Expr
					break
				}
			}

			if binding, ok := bc.bindingByCol[col]; ok {
				if binding != nil {
					exprImpl.NumParts = 2
					exprImpl.Parts[1] = binding.table
				} else {
					return nil, moerr.NewInvalidInput("ambiguouse column reference to '%s'", col)
				}
			}
		}
	case *tree.BinaryExpr:
		exprImpl.Left, err = bc.qualifyColumnNames(exprImpl.Left, selectList, expandAlias)
		if err != nil {
			return nil, err
		}

		exprImpl.Right, err = bc.qualifyColumnNames(exprImpl.Right, selectList, expandAlias)

	case *tree.FuncExpr:
		for i := range exprImpl.Exprs {
			exprImpl.Exprs[i], err = bc.qualifyColumnNames(exprImpl.Exprs[i], selectList, expandAlias)
			if err != nil {
				return nil, err
			}
		}
	case *tree.CastExpr:
		return nil, moerr.NewInternalError("not implement qualifyColumnNames 7")
	case *tree.IsNullExpr:
		return nil, moerr.NewInternalError("not implement qualifyColumnNames 8")
	case *tree.IsNotNullExpr:
		return nil, moerr.NewInternalError("not implement qualifyColumnNames 9")
	case *tree.Tuple:
		return nil, moerr.NewInternalError("not implement qualifyColumnNames 10")
	case *tree.CaseExpr:
		return nil, moerr.NewInternalError("not implement qualifyColumnNames 11")
	case *tree.XorExpr:
		return nil, moerr.NewInternalError("not implement qualifyColumnNames 12")
	}

	return astExpr, err
}

func (bc *BindContext) rootTag() int32 {
	if bc.resultTag > 0 {
		return bc.resultTag
	} else {
		return bc.projectTag
	}
}

func (bc *BindContext) unfoldStar(table string) ([]tree.SelectExpr, []string, error) {
	if len(table) == 0 {
		// unfold *
		var exprs []tree.SelectExpr
		var names []string

		bc.doUnfoldStar(bc.bindingTree, make(map[string]any), &exprs, &names)

		return exprs, names, nil
	} else {
		// unfold tbl.*
		binding, ok := bc.bindingByTable[table]
		if !ok {
			return nil, nil, moerr.NewInvalidInput("missing FROM-clause entry for table '%s'", table)
		}

		exprs := make([]tree.SelectExpr, len(binding.cols))
		names := make([]string, len(binding.cols))

		for i, col := range binding.cols {
			expr, _ := tree.NewUnresolvedName(table, col)
			exprs[i] = tree.SelectExpr{Expr: expr}
			names[i] = col
		}

		return exprs, names, nil
	}
}

func (bc *BindContext) doUnfoldStar(root *BindingTreeNode, visitedUsingCols map[string]any, exprs *[]tree.SelectExpr, names *[]string) {
	if root == nil {
		return
	}
	if root.binding != nil {
		for _, col := range root.binding.cols {
			if _, ok := visitedUsingCols[col]; !ok {
				expr, _ := tree.NewUnresolvedName(root.binding.table, col)
				*exprs = append(*exprs, tree.SelectExpr{Expr: expr})
				*names = append(*names, col)
			}
		}

		return
	}

	var handledUsingCols []string

	for _, using := range root.using {
		if _, ok := visitedUsingCols[using.col]; !ok {
			handledUsingCols = append(handledUsingCols, using.col)
			visitedUsingCols[using.col] = nil

			expr, _ := tree.NewUnresolvedName(using.table, using.col)
			*exprs = append(*exprs, tree.SelectExpr{Expr: expr})
			*names = append(*names, using.col)
		}
	}

	bc.doUnfoldStar(root.left, visitedUsingCols, exprs, names)
	bc.doUnfoldStar(root.right, visitedUsingCols, exprs, names)

	for _, col := range handledUsingCols {
		delete(visitedUsingCols, col)
	}
}
