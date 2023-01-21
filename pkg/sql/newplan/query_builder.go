package newplan

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
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
	astOrderBy := stmt.OrderBy
	astLimit := stmt.Limit

	var clause *tree.SelectClause
	switch selectClause := stmt.Select.(type) {
	case *tree.SelectClause:
		clause = selectClause
	}

	nodeID, err := qb.buildFrom(clause.From.Tables, ctx)
	if err != nil {
		return 0, err
	}

	ctx.binder = NewWhereBinder(qb, ctx)

	//make every column for select list have table prefix
	var selectList tree.SelectExprs
	for _, selectExpr := range clause.Exprs {
		switch expr := selectExpr.Expr.(type) {
		case tree.UnqualifiedStar:
			panic("do not implement")
		case *tree.UnresolvedName:
			if expr.Star {
				panic("do not implement")
			} else {
				if len(selectExpr.As) > 0 {
					ctx.headings = append(ctx.headings, string(selectExpr.As))
				} else {
					ctx.headings = append(ctx.headings, expr.Parts[0])
				}

				newExpr, err := ctx.qualifyColumnNames(expr, nil, false)
				if err != nil {
					return 0, err
				}

				selectList = append(selectList, tree.SelectExpr{
					Expr: newExpr,
					As:   selectExpr.As,
				})
			}
		default:
			if len(selectExpr.As) > 0 {
				ctx.headings = append(ctx.headings, string(selectExpr.As))
			} else {
				for {
					if parenExpr, ok := expr.(*tree.ParenExpr); ok {
						expr = parenExpr.Expr
					} else {
						break
					}
				}
				ctx.headings = append(ctx.headings, tree.String(expr, dialect.MYSQL))
			}

			newExpr, err := ctx.qualifyColumnNames(expr, nil, false)
			if err != nil {
				return 0, err
			}

			selectList = append(selectList, tree.SelectExpr{
				Expr: newExpr,
				As:   selectExpr.As,
			})
		}
	}

	if len(selectList) == 0 {
		return 0, moerr.NewParseError("No tables used")
	}

	if clause.Where != nil {
		//why split ?
		whereList, err := splitAndBindCondition(clause.Where.Expr, ctx)
		if err != nil {
			return 0, err
		}

		var newFilterList []*plan.Expr
		var expr *plan.Expr

		//do nothing in tpch.q1
		for _, where := range whereList {
			nodeID, expr, err = qb.flattenSubqueries(nodeID, where, ctx)
			if err != nil {
				return 0, err
			}

			if expr != nil {
				newFilterList = append(newFilterList, expr)
			}
		}

		nodeID = qb.appendNode(&plan.Node{
			NodeType:   plan.Node_FILTER,
			Children:   []int32{nodeID},
			FilterList: newFilterList,
		}, ctx)
	}

	ctx.groupTag = qb.genNewTag()
	ctx.aggregateTag = qb.genNewTag()
	ctx.projectTag = qb.genNewTag()

	if clause.GroupBy != nil {
		groupBinder := NewGroupBinder(qb, ctx)
		for _, groupExpr := range clause.GroupBy {
			groupExpr, err = ctx.qualifyColumnNames(groupExpr, nil, false)
			if err != nil {
				return 0, err
			}

			_, err = groupBinder.BindExpr(groupExpr, 0, true)
			if err != nil {
				return 0, err
			}
		}
	}

	var havingList []*plan.Expr
	havingBinder := NewHavingBinder(qb, ctx)
	if clause.Having != nil {
		ctx.binder = havingBinder
		havingList, err = splitAndBindCondition(clause.Having.Expr, ctx)
		if err != nil {
			return 0, err
		}
	}

	projectionBinder := NewProjectionBinder(qb, ctx, havingBinder)
	ctx.binder = projectionBinder
	for i, selectExpr := range selectList {
		astExpr, err := ctx.qualifyColumnNames(selectExpr.Expr, nil, false)
		if err != nil {
			return 0, err
		}

		expr, err := projectionBinder.BindExpr(astExpr, 0, true)
		if err != nil {
			return 0, err
		}

		qb.nameByColRef[[2]int32{ctx.projectTag, int32(i)}] = tree.String(astExpr, dialect.MYSQL)

		alias := string(selectExpr.As)
		if len(alias) > 0 {
			ctx.aliasMap[alias] = int32(len(ctx.projects))
		}
		ctx.projects = append(ctx.projects, expr)
	}

	resultLen := len(ctx.projects)
	for i, proj := range ctx.projects {
		exprStr := proj.String()
		if _, ok := ctx.projectByExpr[exprStr]; !ok {
			ctx.projectByExpr[exprStr] = int32(i)
		}
	}
	ctx.isDistinct = clause.Distinct

	var orderBys []*plan.OrderBySpec
	if astOrderBy != nil {
		//astOrderBy is different from selectList
		//ast in astOrderBy have not been qualified.
		orderBinder := NewOrderBinder(projectionBinder, selectList)
		orderBys = make([]*plan.OrderBySpec, 0, len(astOrderBy))

		for _, order := range astOrderBy {
			expr, err := orderBinder.BindExpr(order.Expr)
			if err != nil {
				return 0, err
			}

			orderBy := &plan.OrderBySpec{
				Expr: expr,
				Flag: plan.OrderBySpec_INTERNAL,
			}

			switch order.Direction {
			case tree.Ascending:
				orderBy.Flag |= plan.OrderBySpec_ASC
			case tree.Descending:
				orderBy.Flag |= plan.OrderBySpec_DESC
			}

			switch order.NullsPosition {
			case tree.NullsFirst:
				orderBy.Flag |= plan.OrderBySpec_NULLS_FIRST
			case tree.NullsLast:
				orderBy.Flag |= plan.OrderBySpec_NULLS_LAST
			}

			orderBys = append(orderBys, orderBy)
		}
	}

	var limitExpr *plan.Expr
	var offsetExpr *plan.Expr
	if astLimit != nil {
		limitBinder := NewLimitBinder(qb, ctx)
		if astLimit.Offset != nil {
			offsetExpr, err = limitBinder.BindExpr(astLimit.Offset, 0, true)
			if err != nil {
				return 0, err
			}
		}

		if astLimit.Count != nil {
			limitExpr, err = limitBinder.BindExpr(astLimit.Count, 0, true)
			if err != nil {
				return 0, err
			}

			if cExpr, ok := limitExpr.Expr.(*plan.Expr_C); ok {
				if c, ok := cExpr.C.Value.(*plan.Const_I64Val); ok {
					ctx.hasSingleRow = c.I64Val == 1
				}
			}
		}
	}

	if (len(ctx.groups) > 0 || len(ctx.aggregates) > 0) && len(projectionBinder.boundCols) > 0 {
		mode, err := qb.compCtx.ResolveVariable("sql_mode", true, false)
		if err != nil {
			return 0, err
		}

		if strings.Contains(mode.(string), "ONLY_FULL_GROUP_BY") {
			return 0, moerr.NewSyntaxError("column %q must appear in the GROUP BY clause or be used in an aggregate function", projectionBinder.boundCols[0])
		}

		// for i, proj := range ctx.projects {
		// 	//TODO
		// }
	}

	//?
	if len(ctx.groups) == 0 && len(ctx.aggregates) > 0 {
		ctx.hasSingleRow = true
	}

	// with groupby or aggregate
	if len(ctx.groups) > 0 || len(ctx.aggregates) > 0 {
		nodeID = qb.appendNode(&plan.Node{
			NodeType:    plan.Node_AGG,
			Children:    []int32{nodeID},
			GroupBy:     ctx.groups,
			AggList:     ctx.aggregates,
			BindingTags: []int32{ctx.groupTag, ctx.aggregateTag},
		}, ctx)

		if len(havingList) > 0 {
			var newFilterList []*plan.Expr
			var expr *plan.Expr
			for _, cond := range havingList {
				nodeID, expr, err = qb.flattenSubqueries(nodeID, cond, ctx)
				if err != nil {
					return 0, err
				}

				if expr != nil {
					newFilterList = append(newFilterList, expr)
				}
			}

			nodeID = qb.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{nodeID},
				FilterList: newFilterList,
			}, ctx)
		}

		for name, id := range ctx.groupByAst {
			qb.nameByColRef[[2]int32{ctx.groupTag, id}] = name
		}

		for name, id := range ctx.aggregateByAst {
			qb.nameByColRef[[2]int32{ctx.aggregateTag, id}] = name
		}
	}

	for i, proj := range ctx.projects {
		nodeID, proj, err = qb.flattenSubqueries(nodeID, proj, ctx)
		if err != nil {
			return 0, err
		}

		if proj == nil {
			return 0, moerr.NewNYI("non-scalar subquery in SELECT clause")
		}

		ctx.projects[i] = proj
	}

	fmt.Println("projectlist", ctx.projects)

	nodeID = qb.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		ProjectList: ctx.projects,
		Children:    []int32{nodeID},
		BindingTags: []int32{ctx.projectTag},
	}, ctx)

	if clause.Distinct {
		nodeID = qb.appendNode(&plan.Node{
			NodeType: plan.Node_DISTINCT,
			Children: []int32{nodeID},
		}, ctx)
	}

	if len(orderBys) > 0 {
		nodeID = qb.appendNode(&plan.Node{
			NodeType: plan.Node_SORT,
			Children: []int32{nodeID},
			OrderBy:  orderBys,
		}, ctx)
	}

	//current Node with Limit & Offset
	if limitExpr != nil || offsetExpr != nil {
		node := qb.qry.Nodes[nodeID]
		node.Limit = limitExpr
		node.Offset = offsetExpr
	}

	//Last One is not PROJECT
	if qb.qry.Nodes[nodeID].NodeType != plan.Node_PROJECT {
		//resultLen is the count of projects
		//Virtual ColRef
		for i := 0; i < resultLen; i++ {
			ctx.results = append(ctx.results, &plan.Expr{
				Typ: ctx.projects[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: ctx.projectTag,
						ColPos: int32(i),
					},
				},
			})
		}

		//new tag
		ctx.resultTag = qb.genNewTag()
		nodeID = qb.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: ctx.results,
			Children:    []int32{nodeID},
			BindingTags: []int32{ctx.resultTag},
		}, ctx)
	} else {
		ctx.results = ctx.projects
	}

	if isRoot {
		//select expr list
		qb.qry.Headings = append(qb.qry.Headings, ctx.headings...)
	}

	return nodeID, nil
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
			schema = ctx.defaultDatabase
		}

		obj, tableDef := qb.compCtx.Resolve(schema, table)
		if tableDef == nil {
			return 0, moerr.NewParseError("table %q does not exist", table)
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
			BindingTags: []int32{qb.genNewTag()}, //new tag for Node_TABLE_SCAN or Node_EXTERNAL_SCAN
		}, ctx)
	case *tree.JoinTableExpr:
		if tbl.Right == nil {
			return qb.buildTable(tbl.Left, ctx)
		}
		return qb.buildJoinTable(tbl, ctx)
	case *tree.ParenTableExpr:
		return qb.buildTable(tbl.Expr, ctx)
	case *tree.AliasedTableExpr:
		if _, ok := tbl.Expr.(*tree.Select); ok {
			if tbl.As.Alias == "" {
				return 0, moerr.NewSyntaxError("subquery in FROM must have an alias: %T", stmt)
			}
		}

		nodeID, err = qb.buildTable(tbl.Expr, ctx)
		if err != nil {
			return
		}

		//why here ?
		err = qb.addBinding(nodeID, tbl.As, ctx)

		return
	default:
		return 0, moerr.NewParseError("unsupport table expr: %T", stmt)
	}

	return
}

func (qb *QueryBuilder) genNewTag() int32 {
	qb.nextTag++
	return qb.nextTag
}

// tag -> binding
// table name or alias -> binding
// column -> binding
func (qb *QueryBuilder) addBinding(nodeID int32, alias tree.AliasClause, ctx *BindContext) error {
	node := qb.qry.Nodes[nodeID]
	fmt.Println("addBinding", nodeID)
	if node.NodeType == plan.Node_VALUE_SCAN {
		return nil
	}

	var cols []string
	var types []*plan.Type
	var binding *Binding
	if node.NodeType == plan.Node_TABLE_SCAN || node.NodeType == plan.Node_MATERIAL_SCAN || node.NodeType == plan.Node_EXTERNAL_SCAN {
		if len(alias.Cols) > len(node.TableDef.Cols) {
			return moerr.NewSyntaxError("table %q has %d columns available but %d columns specified", alias.Alias, len(node.TableDef.Cols), len(alias.Cols))
		}

		var table string
		//alias
		if alias.Alias != "" {
			table = string(alias.Alias)
		} else {
			table = node.TableDef.Name
		}

		if _, ok := ctx.bindingByTable[table]; ok {
			return moerr.NewSyntaxError("table name %q specified more than once", table)
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
			//<binding tag,columnIdx> -> (table name or alias).columnName
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

type ColRefRemapping struct {
	globalToLocal map[[2]int32][2]int32
	localToGlobal [][2]int32
}

func (m *ColRefRemapping) addColRef(colRef [2]int32) { //globalRef
	m.globalToLocal[colRef] = [2]int32{0, int32(len(m.localToGlobal))} // global colRef -> [0, index of the localToGlobal] = global colRef
	m.localToGlobal = append(m.localToGlobal, colRef)
}

func (qb *QueryBuilder) remapExpr(expr *plan.Expr, colMap map[[2]int32][2]int32) error {
	switch ne := expr.Expr.(type) {
	case *plan.Expr_Col:
		mapId := [2]int32{ne.Col.RelPos, ne.Col.ColPos} //global colRef
		if ids, ok := colMap[mapId]; ok {
			ne.Col.RelPos = ids[0]
			ne.Col.ColPos = ids[1]
			ne.Col.Name = qb.nameByColRef[mapId]
		} else {
			return moerr.NewParseError("can't find column %v in context's map %v", mapId, colMap)
		}

	case *plan.Expr_F:
		for _, arg := range ne.F.GetArgs() {
			err := qb.remapExpr(arg, colMap)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func PrintColRefCnt(tip string, colRefCnt map[[2]int32]int) {
	for k, v := range colRefCnt {
		fmt.Println("-->", tip, k, v)
	}
}

func (qb *QueryBuilder) remapAllColRefs(nodeID int32, colRefCnt map[[2]int32]int) (*ColRefRemapping, error) {
	node := qb.qry.Nodes[nodeID]

	//PrintColRefCnt("enter "+node.NodeType.String(), colRefCnt)
	//
	//defer func() {
	//	PrintColRefCnt("exit "+node.NodeType.String(), colRefCnt)
	//}()

	remapping := &ColRefRemapping{
		globalToLocal: make(map[[2]int32][2]int32),
	}

	switch node.NodeType {
	case plan.Node_TABLE_SCAN,
		plan.Node_MATERIAL_SCAN,
		plan.Node_EXTERNAL_SCAN,
		plan.Node_TABLE_FUNCTION:
		for _, expr := range node.FilterList {
			increaseRefCnt(expr, colRefCnt)
		}

		internalRemapping := &ColRefRemapping{
			globalToLocal: make(map[[2]int32][2]int32),
		}

		tag := node.BindingTags[0]
		newTableDef := &plan.TableDef{
			Name:          node.TableDef.Name,
			Defs:          node.TableDef.Defs,
			TableType:     node.TableDef.TableType,
			Createsql:     node.TableDef.Createsql,
			Name2ColIndex: node.TableDef.Name2ColIndex,
			CompositePkey: node.TableDef.CompositePkey,
			TblFunc:       node.TableDef.TblFunc,
			IndexInfos:    node.TableDef.IndexInfos,
		}

		for i, col := range node.TableDef.Cols {
			globalRef := [2]int32{tag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			internalRemapping.addColRef(globalRef) //need globalRef
			newTableDef.Cols = append(newTableDef.Cols, col)
		}

		if len(newTableDef.Cols) == 0 {
			internalRemapping.addColRef([2]int32{tag, 0})
			newTableDef.Cols = append(newTableDef.Cols, node.TableDef.Cols[0])
		}

		node.TableDef = newTableDef

		for _, expr := range node.FilterList { //colRef in filter do not be transmitted up.
			decreaseRefCnt(expr, colRefCnt)
			err := qb.remapExpr(expr, internalRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}
		}

		for i, col := range node.TableDef.Cols {
			if colRefCnt[internalRemapping.localToGlobal[i]] == 0 { //colRef of filters were removed.
				continue
			}

			remapping.addColRef(internalRemapping.localToGlobal[i]) //reserve needed colRef

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0, //0
						ColPos: int32(i),
						Name:   qb.nameByColRef[internalRemapping.localToGlobal[i]],
					},
				},
			})
		}

		if len(node.ProjectList) == 0 {
			if len(node.TableDef.Cols) == 0 {
				globalRef := [2]int32{tag, 0}
				remapping.addColRef(globalRef)

				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.TableDef.Cols[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 0,
							ColPos: 0,
							Name:   qb.nameByColRef[globalRef],
						},
					},
				})
			} else {
				remapping.addColRef(internalRemapping.localToGlobal[0])
				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.TableDef.Cols[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 0,
							ColPos: 0,
							Name:   qb.nameByColRef[internalRemapping.localToGlobal[0]],
						},
					},
				})
			}
		}

		if node.NodeType == plan.Node_TABLE_FUNCTION {
			return nil, moerr.NewInternalError("not implement qb 1")
		}
	case plan.Node_INTERSECT,
		plan.Node_INTERSECT_ALL,
		plan.Node_UNION,
		plan.Node_UNION_ALL,
		plan.Node_MINUS,
		plan.Node_MINUS_ALL:
		return nil, moerr.NewInternalError("not implement qb 2")
	case plan.Node_JOIN:
		for _, expr := range node.OnList {
			increaseRefCnt(expr, colRefCnt)
		}

		internalMap := make(map[[2]int32][2]int32)

		leftID := node.Children[0]
		leftRemapping, err := qb.remapAllColRefs(leftID, colRefCnt)
		if err != nil {
			return nil, err
		}

		for k, v := range leftRemapping.globalToLocal {
			internalMap[k] = v
		}

		rightID := node.Children[1]
		rightRemapping, err := qb.remapAllColRefs(rightID, colRefCnt)
		if err != nil {
			return nil, err
		}

		for k, v := range rightRemapping.globalToLocal {
			internalMap[k] = [2]int32{1, v[1]}
		}

		for _, expr := range node.OnList {
			decreaseRefCnt(expr, colRefCnt)
			err := qb.remapExpr(expr, internalMap)
			if err != nil {
				return nil, err
			}
		}

		childProjList := qb.qry.Nodes[leftID].ProjectList
		for i, globalRef := range leftRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   qb.nameByColRef[globalRef],
					},
				},
			})
		}

		if node.JoinType == plan.Node_MARK {
			globalRef := [2]int32{node.BindingTags[0], 0}
			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: &plan.Type{
					Id:          int32(types.T_bool),
					NotNullable: false,
					Size:        1,
				},
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -1,
						ColPos: 0,
						Name:   qb.nameByColRef[globalRef],
					},
				},
			})

			break
		}

		if node.JoinType != plan.Node_SEMI && node.JoinType != plan.Node_ANTI {
			childProjList = qb.qry.Nodes[rightID].ProjectList
			for i, globalRef := range rightRemapping.localToGlobal {
				if colRefCnt[globalRef] == 0 {
					continue
				}

				remapping.addColRef(globalRef)

				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: childProjList[i].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 1,
							ColPos: int32(i),
							Name:   qb.nameByColRef[globalRef],
						},
					},
				})
			}
		}

		if len(node.ProjectList) == 0 && len(leftRemapping.localToGlobal) > 0 {
			globalRef := leftRemapping.localToGlobal[0]
			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: qb.qry.Nodes[leftID].ProjectList[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
						Name:   qb.nameByColRef[globalRef],
					},
				},
			})
		}
	case plan.Node_AGG:
		for _, expr := range node.GroupBy {
			increaseRefCnt(expr, colRefCnt)
		}

		for _, expr := range node.AggList {
			increaseRefCnt(expr, colRefCnt)
		}

		childRemapping, err := qb.remapAllColRefs(node.Children[0], colRefCnt)
		if err != nil {
			return nil, err
		}

		groupTag := node.BindingTags[0]
		aggregateTag := node.BindingTags[1]

		for idx, expr := range node.GroupBy { //colRef in groupby do not be transmitted up
			decreaseRefCnt(expr, colRefCnt)
			err := qb.remapExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{groupTag, int32(idx)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -1,
						ColPos: int32(idx),
						Name:   qb.nameByColRef[globalRef],
					},
				},
			})
		}

		for idx, expr := range node.AggList { //colRef in groupby do not be transmitted up
			decreaseRefCnt(expr, colRefCnt)
			err := qb.remapExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{aggregateTag, int32(idx)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: expr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -2,
						ColPos: int32(idx),
						Name:   qb.nameByColRef[globalRef],
					},
				},
			})
		}

		if len(node.ProjectList) == 0 {
			if len(node.GroupBy) > 0 {
				globalRef := [2]int32{groupTag, 0}
				remapping.addColRef(globalRef)

				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.GroupBy[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: -1,
							ColPos: 0,
							Name:   qb.nameByColRef[globalRef],
						},
					},
				})
			} else {
				globalRef := [2]int32{aggregateTag, 0}
				remapping.addColRef(globalRef)

				node.ProjectList = append(node.ProjectList, &plan.Expr{
					Typ: node.AggList[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: -2,
							ColPos: 0,
							Name:   qb.nameByColRef[globalRef],
						},
					},
				})
			}

		}

	case plan.Node_SORT:
		for _, orderBy := range node.OrderBy {
			increaseRefCnt(orderBy.Expr, colRefCnt)
		}

		childRemapping, err := qb.remapAllColRefs(node.Children[0], colRefCnt)
		if err != nil {
			return nil, err
		}

		for _, orderBy := range node.OrderBy {
			decreaseRefCnt(orderBy.Expr, colRefCnt)
			err := qb.remapExpr(orderBy.Expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}
		}

		childProjList := qb.qry.Nodes[node.Children[0]].ProjectList
		for i, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)
			node.ProjectList = append(node.ProjectList,
				&plan.Expr{
					Typ: childProjList[i].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 0,
							ColPos: int32(i),
							Name:   qb.nameByColRef[globalRef],
						},
					},
				})
		}

		if len(node.ProjectList) == 0 && len(childRemapping.localToGlobal) > 0 {
			globalRef := childRemapping.localToGlobal[0]
			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
						Name:   qb.nameByColRef[globalRef],
					},
				},
			})
		}

	case plan.Node_FILTER:
		for _, expr := range node.FilterList {
			increaseRefCnt(expr, colRefCnt)
		}

		childRemapping, err := qb.remapAllColRefs(node.Children[0], colRefCnt)
		if err != nil {
			return nil, err
		}

		for _, expr := range node.FilterList {
			decreaseRefCnt(expr, colRefCnt)
			err := qb.remapExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}
		}

		childProjList := qb.qry.Nodes[node.Children[0]].ProjectList
		for i, globalRef := range childRemapping.localToGlobal {
			if colRefCnt[globalRef] == 0 {
				continue
			}

			remapping.addColRef(globalRef)

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[i].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(i),
						Name:   qb.nameByColRef[globalRef],
					},
				},
			})
		}

		if len(node.ProjectList) == 0 {
			if len(childRemapping.localToGlobal) > 0 {
				remapping.addColRef(childRemapping.localToGlobal[0])
			}

			node.ProjectList = append(node.ProjectList, &plan.Expr{
				Typ: childProjList[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
					},
				},
			})
		}

	case plan.Node_PROJECT, plan.Node_MATERIAL:
		projectTag := node.BindingTags[0]
		var neededProj []int32 //needed project expr
		for i, expr := range node.ProjectList {
			globalRef := [2]int32{projectTag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			neededProj = append(neededProj, int32(i))
			increaseRefCnt(expr, colRefCnt)
		}

		if len(neededProj) == 0 {
			increaseRefCnt(node.ProjectList[0], colRefCnt)
			neededProj = append(neededProj, 0)
		}

		childRemapping, err := qb.remapAllColRefs(node.Children[0], colRefCnt)
		if err != nil {
			return nil, err
		}

		var newProjList []*plan.Expr
		for _, needed := range neededProj {
			expr := node.ProjectList[needed]
			decreaseRefCnt(expr, colRefCnt)
			err := qb.remapExpr(expr, childRemapping.globalToLocal)
			if err != nil {
				return nil, err
			}

			globalRef := [2]int32{projectTag, needed}
			remapping.addColRef(globalRef)

			newProjList = append(newProjList, expr)
		}

		node.ProjectList = newProjList
	case plan.Node_DISTINCT:
		return nil, moerr.NewInternalError("not implement qb 7")
	case plan.Node_VALUE_SCAN:
		return nil, moerr.NewInternalError("not implement qb 8")
	default:
		return nil, moerr.NewInternalError("unsupport node type")
	}

	node.BindingTags = nil

	return remapping, nil
}

func (qb *QueryBuilder) createQuery() (*plan.Query, error) {
	for i, rootId := range qb.qry.Steps {
		rootId, _ = qb.pushdownFilters(rootId, nil)
		rootId = qb.determineJoinOrder(rootId)
		rootId = qb.pushdownSemiAntiJoins(rootId)
		qb.qry.Steps[i] = rootId

		colRefCnt := make(map[[2]int32]int) //relPos,colPos -> counter
		rootNode := qb.qry.Nodes[rootId]
		resultTag := rootNode.BindingTags[0]
		for i := range rootNode.ProjectList {
			colRefCnt[[2]int32{resultTag, int32(i)}] = 1
		}

		_, err := qb.remapAllColRefs(rootId, colRefCnt)
		if err != nil {
			return nil, err
		}
	}
	return qb.qry, nil
}

func (qb *QueryBuilder) appendNode(node *plan.Node, ctx *BindContext) int32 {
	nodeId := int32(len(qb.qry.Nodes))
	node.NodeId = nodeId
	qb.qry.Nodes = append(qb.qry.Nodes, node)
	qb.ctxByNode = append(qb.ctxByNode, ctx)

	switch node.NodeType {
	case plan.Node_JOIN:
		leftCost := qb.qry.Nodes[node.Children[0]].Cost
		rightCost := qb.qry.Nodes[node.Children[1]].Cost

		switch node.JoinType {
		case plan.Node_INNER:
			card := leftCost.Card * rightCost.Card
			if len(node.OnList) > 0 {
				card *= 0.1
			}
			node.Cost = &plan.Cost{
				Card: card,
			}

		default:
			panic("not implement appendNode")
		}
	case plan.Node_AGG:
		if len(node.GroupBy) > 0 {
			childCost := qb.qry.Nodes[node.Children[0]].Cost
			node.Cost = &plan.Cost{
				Card: childCost.Card * 0.1,
			}
		} else {
			node.Cost = &plan.Cost{
				Card: 1,
			}
		}
	default:
		if len(node.Children) > 0 {
			childCost := qb.qry.Nodes[node.Children[0]].Cost
			node.Cost = &plan.Cost{
				Card: childCost.Card,
			}
		} else if node.Cost == nil {
			node.Cost = &plan.Cost{
				Card: 1,
			}
		}
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

	nodeID := qb.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{leftChildID, rightChildID},
		JoinType: joinType,
	}, ctx)
	//node := qb.qry.Nodes[nodeID]

	ctx.binder = NewTableBinder(qb, ctx)

	switch tbl.Cond.(type) {
	case *tree.OnJoinCond:
		panic("buildJoinTable not implement 1")

	case *tree.UsingJoinCond:
		panic("buildJoinTable not implement 2")

	default:
		if tbl.JoinType == tree.JOIN_TYPE_NATURAL ||
			tbl.JoinType == tree.JOIN_TYPE_NATURAL_LEFT ||
			tbl.JoinType == tree.JOIN_TYPE_NATURAL_RIGHT {
			panic("buildJoinTable not implement 3")
		}
	}

	return nodeID, nil
}

func splitAndBindCondition(astExpr tree.Expr, ctx *BindContext) ([]*plan.Expr, error) {
	conjuncts := splitAstConjunction(astExpr)
	exprs := make([]*plan.Expr, len(conjuncts))

	for i, conjunct := range conjuncts {
		name, err := ctx.qualifyColumnNames(conjunct, nil, false)
		if err != nil {
			return nil, err
		}

		expr, err := ctx.binder.BindExpr(name, 0, true)
		if err != nil {
			return nil, err
		}

		if expr.GetSub() == nil {
			//add CAST
			expr, err = makePlan2CastExpr(expr, &plan.Type{Id: int32(types.T_bool)})
			if err != nil {
				return nil, err
			}
		}
		exprs[i] = expr
	}

	return exprs, nil
}

func splitAstConjunction(astExpr tree.Expr) []tree.Expr {
	var exprs []tree.Expr
	switch t := astExpr.(type) {
	case nil:
	case *tree.AndExpr:
		exprs = append(exprs, splitAstConjunction(t.Left)...)
		exprs = append(exprs, splitAstConjunction(t.Right)...)
	case *tree.ParenExpr:
		exprs = append(exprs, splitAstConjunction(t.Expr)...)
	default:
		exprs = append(exprs, astExpr)
	}
	return exprs
}

func (qb *QueryBuilder) flattenSubqueries(nodeID int32, expr *plan.Expr, ctx *BindContext) (int32, *plan.Expr, error) {
	var err error
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			nodeID, exprImpl.F.Args[i], err = qb.flattenSubqueries(nodeID, arg, ctx)
			if err != nil {
				return 0, nil, err
			}
		}
	case *plan.Expr_Sub:
		nodeID, expr, err = qb.flattenSubquery(nodeID, exprImpl.Sub, ctx)
	}
	return nodeID, expr, err
}

func (qb *QueryBuilder) flattenSubquery(nodeID int32, subquery *plan.SubqueryRef, ctx *BindContext) (int32, *plan.Expr, error) {
	return 0, nil, moerr.NewInternalError("flattenSubquery is not implemented")
}

func (qb *QueryBuilder) pushdownFilters(nodeID int32, filters []*plan.Expr) (int32, []*plan.Expr) {
	node := qb.qry.Nodes[nodeID]
	var canPushdown, cantPushdown []*plan.Expr
	switch node.NodeType {
	case plan.Node_AGG:
		groupTag := node.BindingTags[0]
		aggregateTag := node.BindingTags[1]

		for _, filter := range filters { //?
			if !containsTag(filter, aggregateTag) { //push down the filter without agg function
				canPushdown = append(canPushdown, replaceColRefs(filter, groupTag, node.GroupBy))
			} else {
				cantPushdown = append(cantPushdown, filter)
			}
		}

		childID, cantPushdownChild := qb.pushdownFilters(node.Children[0], canPushdown)

		if len(cantPushdownChild) > 0 {
			childID = qb.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{node.Children[0]},
				FilterList: cantPushdownChild,
			}, nil)
		}

		node.Children[0] = childID
	case plan.Node_FILTER:
		canPushdown = filters
		for _, filter := range node.FilterList {
			canPushdown = append(canPushdown, splitPlanConjunction(applyDistributivity(filter))...)
		}

		childID, cantPushdownChild := qb.pushdownFilters(node.Children[0], canPushdown)

		var extraFilters []*plan.Expr
		for _, filter := range cantPushdownChild {
			switch exprImpl := filter.Expr.(type) {
			case *plan.Expr_F:
				if exprImpl.F.Func.ObjName == "or" {
					keys := checkDNF(filter)
					//input :(c1=1 and c2=1) or (c1=2 and c3=2) ; return:(c1=1 or c1=2), (c2=1 or c2=2)
					for _, key := range keys {
						extraFilter := walkThroughDNF(filter, key)
						if extraFilter != nil {
							extraFilters = append(extraFilters, DeepCopyExpr(extraFilter))
						}
					}
				}
			}
		}
		qb.pushdownFilters(node.Children[0], extraFilters)

		if len(cantPushdownChild) > 0 {
			node.Children[0] = childID
			node.FilterList = cantPushdownChild
		} else {
			nodeID = childID
		}
	case plan.Node_JOIN:
		leftTags := make(map[int32]*Binding)
		for _, tag := range qb.enumerateTags(node.Children[0]) {
			leftTags[tag] = nil
		}

		rightTags := make(map[int32]*Binding)
		for _, tag := range qb.enumerateTags(node.Children[1]) {
			rightTags[tag] = nil
		}

		if node.JoinType == plan.Node_INNER {
			for _, cond := range node.OnList {
				filters = append(filters, splitPlanConjunction(applyDistributivity(cond))...)
			}

			node.OnList = nil
		}

		var leftPushdown, rightPushdown []*plan.Expr
		var turnInner bool

		joinSides := make([]int8, len(filters))

		for i, filter := range filters {
			canTurnInner := true

			joinSides[i] = getJoinSide(filter, leftTags, rightTags)
			if f, ok := filter.Expr.(*plan.Expr_F); ok {
				for _, arg := range f.F.Args {
					if getJoinSide(arg, leftTags, rightTags) == JoinSideBoth {
						canTurnInner = false
						break
					}
				}
			}

			if joinSides[i]&JoinSideRight != 0 && canTurnInner && node.JoinType == plan.Node_LEFT && rejectsNull(filter) {
				panic("pushdownFilters not implement 1")
			}

			// TODO: FULL OUTER join should be handled here. However we don't have FULL OUTER join now.
		}

		if turnInner {
			panic("pushdownFilters not implement 2")
		} else if node.JoinType == plan.Node_LEFT {
			panic("pushdownFilters not implement 3")
		}

		for i, filter := range filters {
			switch joinSides[i] {
			case JoinSideNone:
				if c, ok := filter.Expr.(*plan.Expr_C); ok {
					if c, ok := c.C.Value.(*plan.Const_Bval); ok {
						if c.Bval {
							break
						}
					}
				}

				switch node.JoinType {
				case plan.Node_INNER:
					leftPushdown = append(leftPushdown, DeepCopyExpr(filter))
					rightPushdown = append(rightPushdown, filter)

				case plan.Node_LEFT, plan.Node_SEMI, plan.Node_ANTI, plan.Node_SINGLE:
					leftPushdown = append(leftPushdown, filter)

				default:
					cantPushdown = append(cantPushdown, filter)
				}

			case JoinSideLeft:
				if node.JoinType != plan.Node_OUTER {
					leftPushdown = append(leftPushdown, filter)
				} else {
					cantPushdown = append(cantPushdown, filter)
				}

			case JoinSideRight:
				if node.JoinType == plan.Node_INNER {
					rightPushdown = append(rightPushdown, filter)
				} else {
					cantPushdown = append(cantPushdown, filter)
				}

			case JoinSideBoth:
				if node.JoinType == plan.Node_INNER {
					if f, ok := filter.Expr.(*plan.Expr_F); ok {
						if f.F.Func.ObjName == "=" {
							if getJoinSide(f.F.Args[0], leftTags, rightTags) != JoinSideBoth {
								if getJoinSide(f.F.Args[1], leftTags, rightTags) != JoinSideBoth {
									node.OnList = append(node.OnList, filter)
									break
								}
							}
						}
					}
				}

				cantPushdown = append(cantPushdown, filter)
			}
		}

		childID, cantPushdownChild := qb.pushdownFilters(node.Children[0], leftPushdown)

		if len(cantPushdownChild) > 0 {
			childID = qb.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{node.Children[0]},
				FilterList: cantPushdownChild,
			}, nil)
		}

		node.Children[0] = childID

		childID, cantPushdownChild = qb.pushdownFilters(node.Children[1], rightPushdown)

		if len(cantPushdownChild) > 0 {
			childID = qb.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{node.Children[1]},
				FilterList: cantPushdownChild,
			}, nil)
		}

		node.Children[1] = childID
	case plan.Node_UNION, plan.Node_UNION_ALL, plan.Node_MINUS, plan.Node_MINUS_ALL, plan.Node_INTERSECT, plan.Node_INTERSECT_ALL:
		panic("pushdownFilter not implement 4")
	case plan.Node_PROJECT:
		child := qb.qry.Nodes[node.Children[0]]
		if (child.NodeType == plan.Node_VALUE_SCAN || child.NodeType == plan.Node_EXTERNAL_SCAN) && child.RowsetData == nil {
			cantPushdown = filters
			break
		}

		projectTag := node.BindingTags[0]

		for _, filter := range filters {
			canPushdown = append(canPushdown, replaceColRefs(filter, projectTag, node.ProjectList))
		}

		childID, cantPushdownChild := qb.pushdownFilters(node.Children[0], canPushdown)
		if len(cantPushdownChild) > 0 {
			childID = qb.appendNode(&plan.Node{
				NodeType:   plan.Node_FILTER,
				Children:   []int32{node.Children[0]},
				FilterList: cantPushdownChild,
			}, nil)
		}

		node.Children[0] = childID
	case plan.Node_TABLE_SCAN, plan.Node_EXTERNAL_SCAN:
		node.FilterList = append(node.FilterList, filters...)
	case plan.Node_TABLE_FUNCTION:
		panic("pushdownFilter not implement 5")
	default:
		if len(node.Children) > 0 {
			childID, cantPushdownChild := qb.pushdownFilters(node.Children[0], filters)
			if len(cantPushdownChild) > 0 {
				childID = qb.appendNode(&plan.Node{
					NodeType:   plan.Node_FILTER,
					Children:   []int32{node.Children[0]},
					FilterList: cantPushdownChild,
				}, nil)

				node.Children[0] = childID
			}
		} else {
			cantPushdown = filters
		}
	}
	return nodeID, cantPushdown
}

func (qb *QueryBuilder) pushdownSemiAntiJoins(nodeID int32) int32 {
	return nodeID
}
