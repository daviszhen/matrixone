// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildLoad(stmt *tree.Load, ctx CompilerContext, isPrepareStmt bool) (*Plan, error) {
	fmt.Fprintln(os.Stderr, "enter buildLoad")
	defer func() {
		fmt.Fprintln(os.Stderr, "exit buildLoad")
	}()
	if stmt.Param.Tail.Lines != nil && stmt.Param.Tail.Lines.StartingBy != "" {
		return nil, moerr.NewBadConfig(ctx.GetContext(), "load operation do not support StartingBy field.")
	}
	if stmt.Param.Tail.Fields != nil && stmt.Param.Tail.Fields.EscapedBy != 0 {
		return nil, moerr.NewBadConfig(ctx.GetContext(), "load operation do not support EscapedBy field.")
	}
	stmt.Param.Local = stmt.Local
	fileName, err := checkFileExist(stmt.Param, ctx)
	if err != nil {
		return nil, err
	}

	if err := InitNullMap(stmt.Param, ctx); err != nil {
		return nil, err
	}
	tblName := string(stmt.Table.ObjectName)
	tblInfo, err := getDmlTableInfo(ctx, tree.TableExprs{stmt.Table}, nil, nil, "insert")
	if err != nil {
		return nil, err
	}
	tableDef := tblInfo.tableDefs[0]
	objRef := tblInfo.objRef[0]

	tableDef.Name2ColIndex = map[string]int32{}
	var externalProject []*Expr
	for i := 0; i < len(tableDef.Cols); i++ {
		idx := int32(i)
		tableDef.Name2ColIndex[tableDef.Cols[i].Name] = idx
		colExpr := &plan.Expr{
			Typ: tableDef.Cols[i].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					ColPos: idx,
					Name:   tblName + "." + tableDef.Cols[i].Name,
				},
			},
		}
		externalProject = append(externalProject, colExpr)
	}

	if err := checkNullMap(stmt, tableDef.Cols, ctx); err != nil {
		return nil, err
	}

	stmt.Param.LoadFile = true
	if stmt.Param.ScanType != tree.INLINE {
		json_byte, err := json.Marshal(stmt.Param)
		if err != nil {
			return nil, err
		}
		tableDef.Createsql = string(json_byte)
	}

	fmt.Fprintln(os.Stderr, "xxxx")

	builder := NewQueryBuilder(plan.Query_SELECT, ctx, isPrepareStmt)
	bindCtx := NewBindContext(builder, nil)
	terminated := ","
	enclosedBy := []byte{0}
	if stmt.Param.Tail.Fields != nil {
		enclosedBy = []byte{stmt.Param.Tail.Fields.EnclosedBy}
		terminated = stmt.Param.Tail.Fields.Terminated
	}
	externalScanNode := &plan.Node{
		NodeType:    plan.Node_EXTERNAL_SCAN,
		Stats:       &plan.Stats{},
		ProjectList: externalProject,
		ObjRef:      objRef,
		TableDef:    tableDef,
		ExternScan: &plan.ExternScan{
			Type:         int32(stmt.Param.ScanType),
			Data:         stmt.Param.Data,
			Format:       stmt.Param.Format,
			IgnoredLines: uint64(stmt.Param.Tail.IgnoredLines),
			EnclosedBy:   enclosedBy,
			Terminated:   terminated,
		},
	}
	lastNodeId := builder.appendNode(externalScanNode, bindCtx)

	projectNode := &plan.Node{
		Children: []int32{lastNodeId},
		NodeType: plan.Node_PROJECT,
		Stats:    &plan.Stats{},
	}
	isInsertWithoutAutoPkCol, err := getProjectNode(stmt, ctx, projectNode, tableDef)
	if err != nil {
		fmt.Fprintln(os.Stderr, "yyyy", err)
		return nil, err
	}
	fmt.Fprintln(os.Stderr, "zzzz")
	if stmt.Param.Parallel && (getCompressType(stmt.Param, fileName) != tree.NOCOMPRESS || stmt.Local) {
		projectNode.ProjectList = makeCastExpr(stmt, fileName, tableDef)
	}
	lastNodeId = builder.appendNode(projectNode, bindCtx)
	builder.qry.LoadTag = true

	//append lock node
	// if lockNodeId, ok := appendLockNode(
	// 	builder,
	// 	bindCtx,
	// 	lastNodeId,
	// 	tableDef,
	// 	true,
	// 	true,
	// 	-1,
	// 	nil,
	// ); ok {
	// 	lastNodeId = lockNodeId
	// }

	// append hidden column to tableDef
	newTableDef := DeepCopyTableDef(tableDef)
	checkInsertPkDup := false
	err = buildInsertPlans(ctx, builder, bindCtx, objRef, newTableDef, lastNodeId, checkInsertPkDup, nil, isInsertWithoutAutoPkCol)
	if err != nil {
		return nil, err
	}
	query := builder.qry
	reduceSinkSinkScanNodes(query)
	query.StmtType = plan.Query_INSERT

	pn := &Plan{
		Plan: &plan.Plan_Query{
			Query: query,
		},
	}
	return pn, nil
}

func checkFileExist(param *tree.ExternParam, ctx CompilerContext) (string, error) {
	if param.Local {
		return "", nil
	}
	if param.ScanType == tree.INLINE {
		return "", nil
	}
	param.Ctx = ctx.GetContext()
	if param.ScanType == tree.S3 {
		if err := InitS3Param(param); err != nil {
			return "", err
		}
	} else {
		if err := InitInfileParam(param); err != nil {
			return "", err
		}
	}
	if len(param.Filepath) == 0 {
		return "", nil
	}

	fileList, _, err := ReadDir(param)
	if err != nil {
		return "", err
	}
	if len(fileList) == 0 {
		return "", moerr.NewInvalidInput(param.Ctx, "the file does not exist in load flow")
	}
	param.Ctx = nil
	return fileList[0], nil
}

func getProjectNode(stmt *tree.Load, ctx CompilerContext, node *plan.Node, tableDef *TableDef) (bool, error) {
	fmt.Fprintln(os.Stderr, "enter getProjectNode")
	tblName := string(stmt.Table.ObjectName)
	colToIndex := make(map[int32]string, 0)
	isInsertWithoutAutoPkCol := false
	if len(stmt.Param.Tail.ColumnList) == 0 {
		for i := 0; i < len(tableDef.Cols); i++ {
			colToIndex[int32(i)] = tableDef.Cols[i].Name
		}
	} else {
		for i, col := range stmt.Param.Tail.ColumnList {
			switch realCol := col.(type) {
			case *tree.UnresolvedName:
				if _, ok := tableDef.Name2ColIndex[realCol.Parts[0]]; !ok {
					return isInsertWithoutAutoPkCol, moerr.NewInternalError(ctx.GetContext(), "column '%s' does not exist", realCol.Parts[0])
				}
				colToIndex[int32(i)] = realCol.Parts[0]
			case *tree.VarExpr:
				//NOTE:variable like '@abc' will be passed by.
			default:
				return isInsertWithoutAutoPkCol, moerr.NewInternalError(ctx.GetContext(), "unsupported column type %v", realCol)
			}
		}
	}
	node.ProjectList = make([]*plan.Expr, len(tableDef.Cols))
	projectVec := make([]*plan.Expr, len(tableDef.Cols))
	for i := 0; i < len(tableDef.Cols); i++ {
		tmp := &plan.Expr{
			Typ: tableDef.Cols[i].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					ColPos: int32(i),
					Name:   tblName + "." + tableDef.Cols[i].Name,
				},
			},
		}
		projectVec[i] = tmp
	}
	for k, v := range colToIndex {
		fmt.Fprintln(os.Stderr, k, v)
	}
	for i := 0; i < len(tableDef.Cols); i++ {
		if v, ok := colToIndex[int32(i)]; ok {
			node.ProjectList[tableDef.Name2ColIndex[v]] = projectVec[i]
		}
	}
	var tmp *plan.Expr
	var err error
	//var err error
	for i := 0; i < len(tableDef.Cols); i++ {
		if node.ProjectList[i] != nil {
			continue
		}

		if tableDef.Cols[i].Default.Expr == nil || tableDef.Cols[i].Default.NullAbility {
			tmp, err = getDefaultExpr(ctx.GetContext(), tableDef.Cols[i])
			if err != nil {
				return false, err
			}
		} else {
			tmp = &plan.Expr{
				Typ:  tableDef.Cols[i].Default.Expr.Typ,
				Expr: tableDef.Cols[i].Default.Expr.Expr,
			}
		}

		//cast default value
		tmp, err = forceCastExpr(ctx.GetContext(), tmp, tableDef.Cols[i].Typ)
		if err != nil {
			return false, err
		}

		node.ProjectList[i] = tmp

		if tableDef.Cols[i].Typ.AutoIncr && tableDef.Cols[i].Name == tableDef.Pkey.PkeyColName {
			isInsertWithoutAutoPkCol = true
		}
	}
	return isInsertWithoutAutoPkCol, nil
}

func InitNullMap(param *tree.ExternParam, ctx CompilerContext) error {
	param.NullMap = make(map[string][]string)

	for i := 0; i < len(param.Tail.Assignments); i++ {
		expr, ok := param.Tail.Assignments[i].Expr.(*tree.FuncExpr)
		if !ok {
			param.Tail.Assignments[i].Expr = nil
			return nil
		}
		if len(expr.Exprs) != 2 {
			param.Tail.Assignments[i].Expr = nil
			return nil
		}

		expr2, ok := expr.Func.FunctionReference.(*tree.UnresolvedName)
		if !ok || expr2.Parts[0] != "nullif" {
			param.Tail.Assignments[i].Expr = nil
			return nil
		}

		expr3, ok := expr.Exprs[0].(*tree.UnresolvedName)
		if !ok {
			return moerr.NewInvalidInput(ctx.GetContext(), "the nullif func first param is not UnresolvedName form")
		}

		expr4, ok := expr.Exprs[1].(*tree.NumVal)
		if !ok {
			return moerr.NewInvalidInput(ctx.GetContext(), "the nullif func second param is not NumVal form")
		}
		for j := 0; j < len(param.Tail.Assignments[i].Names); j++ {
			col := param.Tail.Assignments[i].Names[j].Parts[0]
			if col != expr3.Parts[0] {
				return moerr.NewInvalidInput(ctx.GetContext(), "the nullif func first param must equal to colName")
			}
			param.NullMap[col] = append(param.NullMap[col], strings.ToLower(expr4.String()))
		}
		param.Tail.Assignments[i].Expr = nil
	}
	return nil
}

func checkNullMap(stmt *tree.Load, Cols []*ColDef, ctx CompilerContext) error {
	for k := range stmt.Param.NullMap {
		find := false
		for i := 0; i < len(Cols); i++ {
			if Cols[i].Name == k {
				find = true
			}
		}
		if !find {
			return moerr.NewInvalidInput(ctx.GetContext(), "wrong col name '%s' in nullif function", k)
		}
	}
	return nil
}

func getCompressType(param *tree.ExternParam, filepath string) string {
	if param.CompressType != "" && param.CompressType != tree.AUTO {
		return param.CompressType
	}
	index := strings.LastIndex(filepath, ".")
	if index == -1 {
		return tree.NOCOMPRESS
	}
	tail := string([]byte(filepath)[index+1:])
	switch tail {
	case "gz", "gzip":
		return tree.GZIP
	case "bz2", "bzip2":
		return tree.BZIP2
	case "lz4":
		return tree.LZ4
	default:
		return tree.NOCOMPRESS
	}
}

func makeCastExpr(stmt *tree.Load, fileName string, tableDef *TableDef) []*plan.Expr {
	ret := make([]*plan.Expr, 0)
	stringTyp := &plan.Type{
		Id: int32(types.T_varchar),
	}
	for i := 0; i < len(tableDef.Cols); i++ {
		typ := tableDef.Cols[i].Typ
		expr := &plan.Expr{
			Typ: stringTyp,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: int32(i),
				},
			},
		}

		expr, _ = makePlan2CastExpr(stmt.Param.Ctx, expr, typ)
		ret = append(ret, expr)
	}
	return ret
}
