// Copyright 2021 - 2024 Matrix Origin
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

package frontend

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	txnTrace "github.com/matrixorigin/matrixone/pkg/txn/trace"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func buildPlan(requestCtx context.Context, ses FeSession, ctx plan2.CompilerContext, stmt tree.Statement) (*plan2.Plan, error) {
	var ret *plan2.Plan
	var err error

	txnOp := ctx.GetProcess().TxnOperator
	start := time.Now()
	seq := uint64(0)
	if txnOp != nil {
		seq = txnOp.NextSequence()
		txnTrace.GetService().AddTxnDurationAction(
			txnOp,
			client.BuildPlanEvent,
			seq,
			0,
			0,
			err)
	}

	defer func() {
		cost := time.Since(start)

		if txnOp != nil {
			txnTrace.GetService().AddTxnDurationAction(
				txnOp,
				client.BuildPlanEvent,
				seq,
				0,
				cost,
				err)
		}
		v2.TxnStatementBuildPlanDurationHistogram.Observe(cost.Seconds())
	}()

	stats := statistic.StatsInfoFromContext(requestCtx)
	stats.PlanStart()
	defer stats.PlanEnd()

	isPrepareStmt := false
	if ses != nil {
		var accId uint32
		accId, err = defines.GetAccountId(requestCtx)
		if err != nil {
			return nil, err
		}
		ses.SetAccountId(accId)
		if len(ses.GetSql()) > 8 {
			prefix := strings.ToLower(ses.GetSql()[:8])
			isPrepareStmt = prefix == "execute " || prefix == "prepare "
		}
	}
	if s, ok := stmt.(*tree.Insert); ok {
		if _, ok := s.Rows.Select.(*tree.ValuesClause); ok {
			ret, err = plan2.BuildPlan(ctx, stmt, isPrepareStmt)
			if err != nil {
				return nil, err
			}
		}
	}
	if ret != nil {
		if ses != nil && ses.GetTenantInfo() != nil && !ses.IsBackgroundSession() {
			err = authenticateCanExecuteStatementAndPlan(requestCtx, ses.(*Session), stmt, ret)
			if err != nil {
				return nil, err
			}
		}
		return ret, err
	}
	switch stmt := stmt.(type) {
	case *tree.Select, *tree.ParenSelect, *tree.ValuesStatement,
		*tree.Update, *tree.Delete, *tree.Insert,
		*tree.ShowDatabases, *tree.ShowTables, *tree.ShowSequences, *tree.ShowColumns, *tree.ShowColumnNumber, *tree.ShowTableNumber,
		*tree.ShowCreateDatabase, *tree.ShowCreateTable, *tree.ShowIndex,
		*tree.ExplainStmt, *tree.ExplainAnalyze:
		opt := plan2.NewBaseOptimizer(ctx)
		optimized, err := opt.Optimize(stmt, isPrepareStmt)
		if err != nil {
			return nil, err
		}
		ret = &plan2.Plan{
			Plan: &plan2.Plan_Query{
				Query: optimized,
			},
		}
	default:
		ret, err = plan2.BuildPlan(ctx, stmt, isPrepareStmt)
	}
	if ret != nil {
		ret.IsPrepare = isPrepareStmt
		if ses != nil && ses.GetTenantInfo() != nil && !ses.IsBackgroundSession() {
			err = authenticateCanExecuteStatementAndPlan(requestCtx, ses.(*Session), stmt, ret)
			if err != nil {
				return nil, err
			}
		}
	}
	return ret, err
}

func checkModify(plan2 *plan.Plan, proc *process.Process, ses *Session) bool {
	if plan2 == nil {
		return true
	}
	checkFn := func(db string, tableName string, tableId uint64, version uint32) bool {
		_, tableDef := ses.GetTxnCompileCtx().Resolve(db, tableName)
		if tableDef == nil {
			return true
		}
		if tableDef.Version != version || tableDef.TblId != tableId {
			return true
		}
		return false
	}
	switch p := plan2.Plan.(type) {
	case *plan.Plan_Query:
		for i := range p.Query.Nodes {
			if def := p.Query.Nodes[i].TableDef; def != nil {
				if p.Query.Nodes[i].ObjRef == nil || checkFn(p.Query.Nodes[i].ObjRef.SchemaName, def.Name, def.TblId, def.Version) {
					return true
				}
			}
			if ctx := p.Query.Nodes[i].InsertCtx; ctx != nil {
				if ctx.Ref == nil || checkFn(ctx.Ref.SchemaName, ctx.TableDef.Name, ctx.TableDef.TblId, ctx.TableDef.Version) {
					return true
				}
			}
			if ctx := p.Query.Nodes[i].ReplaceCtx; ctx != nil {
				if ctx.Ref == nil || checkFn(ctx.Ref.SchemaName, ctx.TableDef.Name, ctx.TableDef.TblId, ctx.TableDef.Version) {
					return true
				}
			}
			if ctx := p.Query.Nodes[i].DeleteCtx; ctx != nil {
				if ctx.Ref == nil || checkFn(ctx.Ref.SchemaName, ctx.TableDef.Name, ctx.TableDef.TblId, ctx.TableDef.Version) {
					return true
				}
			}
			if ctx := p.Query.Nodes[i].PreInsertCtx; ctx != nil {
				if ctx.Ref == nil || checkFn(ctx.Ref.SchemaName, ctx.TableDef.Name, ctx.TableDef.TblId, ctx.TableDef.Version) {
					return true
				}
			}
			if ctx := p.Query.Nodes[i].PreInsertCtx; ctx != nil {
				if ctx.Ref == nil || checkFn(ctx.Ref.SchemaName, ctx.TableDef.Name, ctx.TableDef.TblId, ctx.TableDef.Version) {
					return true
				}
			}
			if ctx := p.Query.Nodes[i].OnDuplicateKey; ctx != nil {
				if p.Query.Nodes[i].ObjRef == nil || checkFn(p.Query.Nodes[i].ObjRef.SchemaName, ctx.TableName, ctx.TableId, ctx.TableVersion) {
					return true
				}
			}
		}
	default:
	}
	return false
}

func checkNodeCanCache(p *plan2.Plan) bool {
	if p == nil {
		return true
	}
	if q, ok := p.Plan.(*plan2.Plan_Query); ok {
		for _, node := range q.Query.Nodes {
			if node.NotCacheable {
				return false
			}
			if node.ObjRef != nil && len(node.ObjRef.SubscriptionName) > 0 {
				return false
			}
		}
	}
	return true
}
