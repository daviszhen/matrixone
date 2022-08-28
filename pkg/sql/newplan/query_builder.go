package newplan

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
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
	if len(stmt) == 1 {
		return qb.buildTable(stmt[0], ctx)
	}
	return 0, nil
}

func (qb *QueryBuilder) buildTable(stmt tree.TableExpr, ctx *BindContext) (nodeID int32, err error) {
	switch tbl := stmt.(type) {
	case *tree.TableName:
		schema := string(tbl.SchemaName)
		table := string(tbl.ObjectName)
		if len(table) == 0 || table == "dual" {
			nodeID = qb.appendNode(&plan.Node{NodeType: plan.Node_VALUE_SCAN}, ctx)
			ctx.hasSingleRow = true
			break
		}

		if len(schema) == 0 {
			panic("TODO")
		}

		obj, tableDef := qb.compCtx.Resolve(schema, table)
		if tableDef == nil {
			return 0, errors.New("", fmt.Sprintf("table %q does not exist", table))
		}

		tableDef.Name2ColIndex = map[string]int32{}
		for i := 0; i < len(tableDef.Cols); i++ {
			tableDef.Name2ColIndex[tableDef.Cols[i].Name] = int32(i)
		}

		nodeType := plan.Node_TABLE_SCAN
		if tableDef.TableType == catalog.SystemExternalRel {
			nodeType = plan.Node_EXTERNAL_SCAN
		}

		viewDefString := ""
		for _, def := range tableDef.Defs {
			if viewDef, ok := def.Def.(*plan.TableDef_DefType_View); ok {
				viewDefString = viewDef.View.View
				break
			}
		}
		if viewDefString != "" {
			panic("TODO")
		}

		nodeID = qb.appendNode(&plan.Node{
			NodeType:    nodeType,
			Cost:        qb.compCtx.Cost(obj, nil),
			ObjRef:      obj,
			TableDef:    tableDef,
			BindingTags: []int32{qb.genNewTag()},
		}, ctx)
	case *tree.JoinTableExpr:
		return qb.buildJoinTable(tbl, ctx)
	case *tree.ParenTableExpr:
		return qb.buildTable(tbl.Expr, ctx)
	case *tree.AliasedTableExpr:
		if _, ok := tbl.Expr.(*tree.Select); ok {
			if tbl.As.Alias == "" {
				return 0, errors.New("", fmt.Sprintf("subquery in FROM must have an alias: %T", stmt))
			}
		}

		nodeID, err = qb.buildTable(tbl.Expr, ctx)
		if err != nil {
			return
		}

		err = qb.addBinding(nodeID, tbl.As, ctx)

		return
	default:
		return 0, errors.New("", fmt.Sprintf("unsupport table expr: %T", stmt))
	}

	return
}

func (qb *QueryBuilder) genNewTag() int32 {
	qb.nextTag++
	return qb.nextTag
}

func (qb *QueryBuilder) addBinding(nodeID int32, alias tree.AliasClause, ctx *BindContext) error {
	node := qb.qry.Nodes[nodeID]

	if node.NodeType == plan.Node_VALUE_SCAN {
		return nil
	}

	var cols []string
	var types []*plan.Type
	var binding *Binding
	if node.NodeType == plan.Node_TABLE_SCAN || node.NodeType == plan.Node_MATERIAL_SCAN || node.NodeType == plan.Node_EXTERNAL_SCAN {
		if len(alias.Cols) > len(node.TableDef.Cols) {
			return errors.New("", fmt.Sprintf("table %q has %d columns available but %d columns specified", alias.Alias, len(node.TableDef.Cols), len(alias.Cols)))
		}

		var table string
		if alias.Alias != "" {
			table = string(alias.Alias)
		} else {
			table = node.TableDef.Name
		}

		if _, ok := ctx.bindingByTable[table]; ok {
			return errors.New("", fmt.Sprintf("table name %q specified more than once", table))
		}

		cols = make([]string, len(node.TableDef.Cols))
		types = make([]*plan.Type, len(node.TableDef.Cols))

		tag := node.BindingTags[0]

		for i, col := range node.TableDef.Cols {
			if i < len(alias.Cols) {
				cols[i] = string(alias.Cols[i])
			} else {
				cols[i] = col.Name
			}
			types[i] = col.Typ
			name := table + "." + cols[i]
			//<tag,columnIdx> -> table.columnName
			qb.nameByColRef[[2]int32{tag, int32(i)}] = name
		}

		binding = NewBind(tag, nodeID, table, cols, types)
	} else {
		panic("TODO")
	}

	ctx.bindings = append(ctx.bindings, binding)
	//tag -> binding
	ctx.bindingByTag[binding.tag] = binding
	//table -> binding
	ctx.bindingByTable[binding.table] = binding

	//columnName -> binding
	for _, col := range cols {
		if _, ok := ctx.bindingByCol[col]; ok {
			ctx.bindingByCol[col] = nil
		} else {
			ctx.bindingByCol[col] = binding
		}
	}

	ctx.bindingTree = &BindingTreeNode{
		binding: binding,
	}

	return nil
}

func (qb *QueryBuilder) createQuery() (*plan.Query, error) {
	panic("TODO")
	return nil, nil
}

func (qb *QueryBuilder) appendNode(node *plan.Node, ctx *BindContext) int32 {
	nodeId := int32(len(qb.qry.Nodes))
	node.NodeId = nodeId
	qb.qry.Nodes = append(qb.qry.Nodes, node)
	qb.ctxByNode = append(qb.ctxByNode, ctx)

	switch node.NodeType {
	default:
		//TODO
	}
	return nodeId
}

func (qb *QueryBuilder) buildJoinTable(tbl *tree.JoinTableExpr, ctx *BindContext) (int32, error) {
	var joinType plan.Node_JoinFlag
	switch tbl.JoinType {
	case tree.JOIN_TYPE_CROSS, tree.JOIN_TYPE_INNER, tree.JOIN_TYPE_NATURAL:
		joinType = plan.Node_INNER
	case tree.JOIN_TYPE_LEFT, tree.JOIN_TYPE_NATURAL_LEFT:
		joinType = plan.Node_LEFT
	case tree.JOIN_TYPE_RIGHT, tree.JOIN_TYPE_NATURAL_RIGHT:
		joinType = plan.Node_RIGHT
	case tree.JOIN_TYPE_FULL:
		joinType = plan.Node_OUTER
	}

	leftCtx := NewBindContext(ctx)
	rightCtx := NewBindContext(ctx)

	leftChildID, err := qb.buildTable(tbl.Left, leftCtx)
	if err != nil {
		return 0, err
	}

	rightChildID, err := qb.buildTable(tbl.Right, rightCtx)
	if err != nil {
		return 0, err
	}

	err = ctx.mergeContexts(leftCtx, rightCtx)
	if err != nil {
		return 0, err
	}

	nodeID := qb.appendNode(&plan.Node{NodeType: plan.Node_JOIN, Children: []int32{leftChildID, rightChildID}, JoinType: joinType}, ctx)
	node := qb.qry.Nodes[nodeID]

	ctx.binder = NewTableBinder(qb, ctx)

	panic("TODO")
}
