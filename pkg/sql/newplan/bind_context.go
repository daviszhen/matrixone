package newplan

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func NewBindContext(parent *BindContext) *BindContext {
	bc := &BindContext{
		parent:         parent,
		bindingByTag:   make(map[int32]*Binding),
		bindingByTable: make(map[string]*Binding),
		bindingByCol:   make(map[string]*Binding),
		aliasMap:       make(map[string]int32),
		groupByAst:     make(map[string]int32),
		aggregateByAst: make(map[string]int32),
		projectByExpr:  make(map[string]int32),
	}
	if parent != nil {
		bc.defaultDatabase = parent.defaultDatabase
	}
	return bc
}

func (bc *BindContext) qualifyColumnNames(astExpr tree.Expr, selectList tree.SelectExprs, expandAlias bool) (tree.Expr, error) {
	var err error
	switch exprImpl := astExpr.(type) {
	case *tree.UnresolvedName:
		if !exprImpl.Star && exprImpl.NumParts == 1 {
			col := exprImpl.Parts[0]
			if expandAlias {
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
	}

	return astExpr, err
}
