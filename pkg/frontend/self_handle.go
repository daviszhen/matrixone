// Copyright 2021 Matrix Origin
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
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/go-errors/errors"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/explain"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/route"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

func handleInFrontend(requestCtx context.Context,
	ses *Session,
	execCtx *ExecCtx,
) (err error) {
	//check transaction states
	switch st := execCtx.stmt.(type) {
	case *tree.BeginTransaction:
		err = ses.TxnBegin()
		if err != nil {
			return
		}
		RecordStatementTxnID(requestCtx, ses)
	case *tree.CommitTransaction:
		err = ses.TxnCommit()
		if err != nil {
			return
		}
	case *tree.RollbackTransaction:
		err = ses.TxnRollback()
		if err != nil {
			return
		}
	case *tree.SetRole:

		ses.InvalidatePrivilegeCache()
		//switch role
		err = handleSwitchRole(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.Use:

		var v interface{}
		v, err = ses.GetGlobalVar("lower_case_table_names")
		if err != nil {
			return
		}
		st.Name.SetConfig(v.(int64))
		//use database
		err = handleChangeDB(requestCtx, ses, st.Name.Compare())
		if err != nil {
			return
		}
		err = changeVersion(requestCtx, ses, st.Name.Compare())
		if err != nil {
			return
		}
	case *tree.MoDump:

		//dump
		err = handleDump(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.PrepareStmt:

		execCtx.prepareStmt, err = handlePrepareStmt(requestCtx, ses, st, execCtx.sqlOfStmt)
		if err != nil {
			return
		}
		err = authenticateUserCanExecutePrepareOrExecute(requestCtx, ses, execCtx.prepareStmt.PrepareStmt, execCtx.prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
		if err != nil {
			ses.RemovePrepareStmt(execCtx.prepareStmt.Name)
			return
		}
	case *tree.PrepareString:

		execCtx.prepareStmt, err = handlePrepareString(requestCtx, ses, st)
		if err != nil {
			return
		}
		err = authenticateUserCanExecutePrepareOrExecute(requestCtx, ses, execCtx.prepareStmt.PrepareStmt, execCtx.prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
		if err != nil {
			ses.RemovePrepareStmt(execCtx.prepareStmt.Name)
			return
		}
	case *tree.CreateConnector:

		err = handleCreateConnector(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.PauseDaemonTask:

		err = handlePauseDaemonTask(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.CancelDaemonTask:

		err = handleCancelDaemonTask(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.ResumeDaemonTask:

		err = handleResumeDaemonTask(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.DropConnector:

		err = handleDropConnector(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.ShowConnectors:

		if err = handleShowConnectors(requestCtx, ses, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.Deallocate:

		err = handleDeallocate(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.Reset:

		err = handleReset(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.SetVar:

		err = handleSetVar(requestCtx, ses, st, execCtx.sqlOfStmt)
		if err != nil {
			return
		}
	case *tree.ShowVariables:

		err = handleShowVariables(ses, st, execCtx.proc, execCtx.isLastStmt)
		if err != nil {
			return
		}
	case *tree.ShowErrors, *tree.ShowWarnings:

		err = handleShowErrors(ses, execCtx.isLastStmt)
		if err != nil {
			return
		}
	case *tree.AnalyzeStmt:

		if err = handleAnalyzeStmt(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.ExplainStmt:

		if err = handleExplainStmt(requestCtx, ses, st); err != nil {
			return
		}
	case *InternalCmdFieldList:

		if err = handleCmdFieldList(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CreatePublication:

		if err = handleCreatePublication(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.AlterPublication:

		if err = handleAlterPublication(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropPublication:

		if err = handleDropPublication(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.ShowSubscriptions:

		if err = handleShowSubscriptions(requestCtx, ses, st, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.CreateStage:

		if err = handleCreateStage(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropStage:

		if err = handleDropStage(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.AlterStage:

		if err = handleAlterStage(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CreateAccount:

		ses.InvalidatePrivilegeCache()
		if err = handleCreateAccount(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropAccount:

		ses.InvalidatePrivilegeCache()
		if err = handleDropAccount(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.AlterAccount:
		ses.InvalidatePrivilegeCache()

		if err = handleAlterAccount(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.AlterDataBaseConfig:
		ses.InvalidatePrivilegeCache()

		if st.IsAccountLevel {
			if err = handleAlterAccountConfig(requestCtx, ses, st); err != nil {
				return
			}
		} else {
			if err = handleAlterDataBaseConfig(requestCtx, ses, st); err != nil {
				return
			}
		}
	case *tree.CreateUser:

		ses.InvalidatePrivilegeCache()
		if err = handleCreateUser(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropUser:

		ses.InvalidatePrivilegeCache()
		if err = handleDropUser(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.AlterUser: //TODO

		ses.InvalidatePrivilegeCache()
		if err = handleAlterUser(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CreateRole:

		ses.InvalidatePrivilegeCache()
		if err = handleCreateRole(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropRole:

		ses.InvalidatePrivilegeCache()
		if err = handleDropRole(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CreateFunction:

		if err = st.Valid(); err != nil {
			return err
		}
		if err = handleCreateFunction(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropFunction:

		if err = handleDropFunction(requestCtx, ses, st, execCtx.proc); err != nil {
			return
		}
	case *tree.CreateProcedure:

		if err = handleCreateProcedure(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropProcedure:

		if err = handleDropProcedure(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CallStmt:

		if err = handleCallProcedure(requestCtx, ses, st, execCtx.proc); err != nil {
			return
		}
	case *tree.Grant:

		ses.InvalidatePrivilegeCache()
		switch st.Typ {
		case tree.GrantTypeRole:
			if err = handleGrantRole(requestCtx, ses, &st.GrantRole); err != nil {
				return
			}
		case tree.GrantTypePrivilege:
			if err = handleGrantPrivilege(requestCtx, ses, &st.GrantPrivilege); err != nil {
				return
			}
		}
	case *tree.Revoke:

		ses.InvalidatePrivilegeCache()
		switch st.Typ {
		case tree.RevokeTypeRole:
			if err = handleRevokeRole(requestCtx, ses, &st.RevokeRole); err != nil {
				return
			}
		case tree.RevokeTypePrivilege:
			if err = handleRevokePrivilege(requestCtx, ses, &st.RevokePrivilege); err != nil {
				return
			}
		}
	case *tree.Kill:

		ses.InvalidatePrivilegeCache()
		if err = handleKill(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.ShowAccounts:

		if err = handleShowAccounts(requestCtx, ses, st, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.ShowCollation:

		if err = handleShowCollation(ses, st, execCtx.proc, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.ShowBackendServers:

		if err = handleShowBackendServers(requestCtx, ses, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.SetTransaction:

		//TODO: handle set transaction
	case *tree.LockTableStmt:

	case *tree.UnLockTableStmt:

	case *tree.BackupStart:

		if err = handleStartBackup(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.EmptyStmt:

		if err = handleEmptyStmt(requestCtx, ses, st); err != nil {
			return
		}
	}
	return
}

func handleShowTableStatus(ses *Session, stmt *tree.ShowTableStatus, proc *process.Process) error {
	var db engine.Database
	var err error

	ctx := ses.requestCtx
	if db, err = gPu.StorageEngine.Database(ctx, stmt.DbName, proc.TxnOperator); err != nil {
		//echo client. no such database
		return moerr.NewBadDB(ctx, stmt.DbName)
	}
	if db.IsSubscription(ctx) {
		subMeta, err := checkSubscriptionValid(ctx, ses, db.GetCreateSql(ctx))
		if err != nil {
			return err
		}

		// as pub account
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(subMeta.AccountId))
		// get db as pub account
		if db, err = ses.GetStorage().Database(ctx, subMeta.DbName, proc.TxnOperator); err != nil {
			return err
		}
	}
	mrs := ses.GetMysqlResultSet()
	for _, row := range ses.data {
		tableName := string(row[0].([]byte))
		r, err := db.Relation(ctx, tableName, nil)
		if err != nil {
			return err
		}
		err = r.UpdateObjectInfos(ctx)
		if err != nil {
			return err
		}
		row[3], err = r.Rows(ctx)
		if err != nil {
			return err
		}
		mrs.AddRow(row)
	}
	if err := ses.GetMysqlProtocol().SendResultSetTextBatchRowSpeedup(mrs, mrs.GetRowCount()); err != nil {
		logError(ses, ses.GetDebugString(),
			"Failed to handle 'SHOW TABLE STATUS'",
			zap.Error(err))
		return err
	}
	return nil
}

func doUse(ctx context.Context, ses TempInter, db string) error {
	if v, ok := ses.(*Session); ok {
		defer RecordStatementTxnID(ctx, v)
	}

	txnHandler := ses.GetTxnHandler()
	var txnCtx context.Context
	var txn TxnOperator
	var err error
	var dbMeta engine.Database
	txnCtx, txn, err = txnHandler.GetTxn()
	if err != nil {
		return err
	}
	//TODO: check meta data
	if dbMeta, err = gPu.StorageEngine.Database(txnCtx, db, txn); err != nil {
		//echo client. no such database
		return moerr.NewBadDB(ctx, db)
	}
	if dbMeta.IsSubscription(ctx) {
		_, err = checkSubscriptionValid(ctx, ses, dbMeta.GetCreateSql(ctx))
		if err != nil {
			return err
		}
	}
	oldDB := ses.GetDatabaseName()
	ses.SetDatabaseName(db)

	logDebugf(ses.GetDebugString(), "User %s change database from [%s] to [%s]", ses.GetUserName(), oldDB, ses.GetDatabaseName())

	return nil
}

func handleChangeDB(requestCtx context.Context, ses TempInter, db string) error {
	return doUse(requestCtx, ses, db)
}

func handleDump(requestCtx context.Context, ses TempInter, dump *tree.MoDump) error {
	return doDumpQueryResult(requestCtx, ses.(*Session), dump.ExportParams)
}

/*
handle "SELECT @@xxx.yyyy"
*/
func handleSelectVariables(ses TempInter, ve *tree.VarExpr, isLastStmt bool) error {
	var err error = nil
	mrs := ses.GetMysqlResultSet()
	proto := ses.GetMysqlProtocol()

	col := new(MysqlColumn)
	col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col.SetName("@@" + ve.Name)
	mrs.AddColumn(col)

	row := make([]interface{}, 1)
	if ve.System {
		if ve.Global {
			val, err := ses.GetGlobalVar(ve.Name)
			if err != nil {
				return err
			}
			row[0] = val
		} else {
			val, err := ses.GetSessionVar(ve.Name)
			if err != nil {
				return err
			}
			row[0] = val
		}
	} else {
		//user defined variable
		_, val, err := ses.GetUserDefinedVar(ve.Name)
		if err != nil {
			return err
		}
		if val != nil {
			row[0] = val.Value
		} else {
			row[0] = nil
		}
	}

	mrs.AddRow(row)

	mer := NewMysqlExecutionResult(0, 0, 0, 0, mrs)
	resp := ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, isLastStmt)

	if err := proto.SendResponse(ses.GetRequestContext(), resp); err != nil {
		return moerr.NewInternalError(ses.GetRequestContext(), "routine send response failed.")
	}
	return err
}

func doCmdFieldList(requestCtx context.Context, ses *Session, icfl *InternalCmdFieldList) error {
	dbName := ses.GetDatabaseName()
	if dbName == "" {
		return moerr.NewNoDB(requestCtx)
	}

	//Get table infos for the database from the cube
	//case 1: there are no table infos for the db
	//case 2: db changed
	//NOTE: it costs too much time.
	//It just reduces the information in the auto-completion (auto-rehash) of the mysql client.
	//var attrs []ColumnInfo
	//
	//if tableInfos == nil || db != dbName {
	//	txnHandler := ses.GetTxnHandler()
	//	eng := ses.GetStorage()
	//	db, err := eng.Database(requestCtx, dbName, txnHandler.GetTxn())
	//	if err != nil {
	//		return err
	//	}
	//
	//	names, err := db.Relations(requestCtx)
	//	if err != nil {
	//		return err
	//	}
	//	for _, name := range names {
	//		table, err := db.Relation(requestCtx, name)
	//		if err != nil {
	//			return err
	//		}
	//
	//		defs, err := table.TableDefs(requestCtx)
	//		if err != nil {
	//			return err
	//		}
	//		for _, def := range defs {
	//			if attr, ok := def.(*engine.AttributeDef); ok {
	//				attrs = append(attrs, &engineColumnInfo{
	//					name: attr.Attr.Name,
	//					typ:  attr.Attr.Type,
	//				})
	//			}
	//		}
	//	}
	//
	//	if tableInfos == nil {
	//		tableInfos = make(map[string][]ColumnInfo)
	//	}
	//	tableInfos[tableName] = attrs
	//}
	//
	//cols, ok := tableInfos[tableName]
	//if !ok {
	//	//just give the empty info when there is no such table.
	//	attrs = make([]ColumnInfo, 0)
	//} else {
	//	attrs = cols
	//}
	//
	//for _, c := range attrs {
	//	col := new(MysqlColumn)
	//	col.SetName(c.GetName())
	//	err = convertEngineTypeToMysqlType(c.GetType(), col)
	//	if err != nil {
	//		return err
	//	}
	//
	//	/*
	//		mysql CMD_FIELD_LIST response: send the column definition per column
	//	*/
	//	err = proto.SendColumnDefinitionPacket(col, int(COM_FIELD_LIST))
	//	if err != nil {
	//		return err
	//	}
	//}
	return nil
}

/*
handle cmd CMD_FIELD_LIST
*/
func handleCmdFieldList(requestCtx context.Context, ses TempInter, icfl *InternalCmdFieldList) error {
	var err error
	proto := ses.GetMysqlProtocol()

	err = doCmdFieldList(requestCtx, ses.(*Session), icfl)
	if err != nil {
		return err
	}

	/*
		mysql CMD_FIELD_LIST response: End after the column has been sent.
		send EOF packet
	*/
	err = proto.sendEOFOrOkPacket(0, ses.GetServerStatus())
	if err != nil {
		return err
	}

	return err
}

func doSetVar(ctx context.Context, ses *Session, sv *tree.SetVar, sql string) error {
	var err error = nil
	var ok bool
	setVarFunc := func(system, global bool, name string, value interface{}, sql string) error {
		var oldValueRaw interface{}
		if system {
			if global {
				err = doCheckRole(ctx, ses)
				if err != nil {
					return err
				}
				err = ses.SetGlobalVar(name, value)
				if err != nil {
					return err
				}
				err = doSetGlobalSystemVariable(ctx, ses, name, value)
				if err != nil {
					return err
				}
			} else {
				if strings.ToLower(name) == "autocommit" {
					oldValueRaw, err = ses.GetSessionVar("autocommit")
					if err != nil {
						return err
					}
				}
				err = ses.SetSessionVar(name, value)
				if err != nil {
					return err
				}
			}

			if strings.ToLower(name) == "autocommit" {
				oldValue, err := valueIsBoolTrue(oldValueRaw)
				if err != nil {
					return err
				}
				newValue, err := valueIsBoolTrue(value)
				if err != nil {
					return err
				}
				err = ses.SetAutocommit(oldValue, newValue)
				if err != nil {
					return err
				}
			}
		} else {
			err = ses.SetUserDefinedVar(name, value, sql)
			if err != nil {
				return err
			}
		}
		return nil
	}
	for _, assign := range sv.Assignments {
		name := assign.Name
		var value interface{}

		value, err = getExprValue(assign.Value, ses)
		if err != nil {
			return err
		}

		if systemVar, ok := gSysVarsDefs[name]; ok {
			if isDefault, ok := value.(bool); ok && isDefault {
				value = systemVar.Default
			}
		}

		//TODO : fix SET NAMES after parser is ready
		if name == "names" {
			//replaced into three system variable:
			//character_set_client, character_set_connection, and character_set_results
			replacedBy := []string{
				"character_set_client", "character_set_connection", "character_set_results",
			}
			for _, rb := range replacedBy {
				err = setVarFunc(assign.System, assign.Global, rb, value, sql)
				if err != nil {
					return err
				}
			}
		} else if name == "syspublications" {
			if !ses.GetTenantInfo().IsSysTenant() {
				return moerr.NewInternalError(ses.GetRequestContext(), "only system account can set system variable syspublications")
			}
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
		} else if name == "clear_privilege_cache" {
			//if it is global variable, it does nothing.
			if !assign.Global {
				//if the value is 'on or off', just invalidate the privilege cache
				ok, err = valueIsBoolTrue(value)
				if err != nil {
					return err
				}

				if ok {
					cache := ses.GetPrivilegeCache()
					if cache != nil {
						cache.invalidate()
					}
				}
				err = setVarFunc(assign.System, assign.Global, name, value, sql)
				if err != nil {
					return err
				}
			}
		} else if name == "enable_privilege_cache" {
			ok, err = valueIsBoolTrue(value)
			if err != nil {
				return err
			}

			//disable privilege cache. clean the cache.
			if !ok {
				cache := ses.GetPrivilegeCache()
				if cache != nil {
					cache.invalidate()
				}
			}
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
		} else if name == "runtime_filter_limit_in" {
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
			runtime.ProcessLevelRuntime().SetGlobalVariables("runtime_filter_limit_in", value)
		} else if name == "runtime_filter_limit_bloom_filter" {
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
			runtime.ProcessLevelRuntime().SetGlobalVariables("runtime_filter_limit_bloom_filter", value)
		} else {
			err = setVarFunc(assign.System, assign.Global, name, value, sql)
			if err != nil {
				return err
			}
		}
	}
	return err
}

/*
handle setvar
*/
func handleSetVar(ctx context.Context, ses TempInter, sv *tree.SetVar, sql string) error {
	err := doSetVar(ctx, ses.(*Session), sv, sql)
	if err != nil {
		return err
	}

	return nil
}

func doShowErrors(ses *Session) error {
	var err error

	levelCol := new(MysqlColumn)
	levelCol.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	levelCol.SetName("Level")

	CodeCol := new(MysqlColumn)
	CodeCol.SetColumnType(defines.MYSQL_TYPE_SHORT)
	CodeCol.SetName("Code")

	MsgCol := new(MysqlColumn)
	MsgCol.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	MsgCol.SetName("Message")

	mrs := ses.GetMysqlResultSet()

	mrs.AddColumn(levelCol)
	mrs.AddColumn(CodeCol)
	mrs.AddColumn(MsgCol)

	info := ses.GetErrInfo()

	for i := info.length() - 1; i >= 0; i-- {
		row := make([]interface{}, 3)
		row[0] = "Error"
		row[1] = info.codes[i]
		row[2] = info.msgs[i]
		mrs.AddRow(row)
	}

	return err
}

func handleShowErrors(ses TempInter, isLastStmt bool) error {
	var err error
	proto := ses.GetMysqlProtocol()
	err = doShowErrors(ses.(*Session))
	if err != nil {
		return err
	}

	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, isLastStmt)

	if err := proto.SendResponse(ses.GetRequestContext(), resp); err != nil {
		return moerr.NewInternalError(ses.GetRequestContext(), "routine send response failed. error:%v ", err)
	}
	return err
}

func doShowVariables(ses *Session, proc *process.Process, sv *tree.ShowVariables) error {
	if sv.Like != nil && sv.Where != nil {
		return moerr.NewSyntaxError(ses.GetRequestContext(), "like clause and where clause cannot exist at the same time")
	}

	var err error = nil

	col1 := new(MysqlColumn)
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col1.SetName("Variable_name")

	col2 := new(MysqlColumn)
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col2.SetName("Value")

	mrs := ses.GetMysqlResultSet()
	mrs.AddColumn(col1)
	mrs.AddColumn(col2)

	var hasLike = false
	var likePattern = ""
	var isIlike = false
	if sv.Like != nil {
		hasLike = true
		if sv.Like.Op == tree.ILIKE {
			isIlike = true
		}
		likePattern = strings.ToLower(sv.Like.Right.String())
	}

	var sysVars map[string]interface{}
	if sv.Global {
		sysVars, err = doGetGlobalSystemVariable(ses.GetRequestContext(), ses)
		if err != nil {
			return err
		}
	} else {
		sysVars = ses.CopyAllSessionVars()
	}

	rows := make([][]interface{}, 0, len(sysVars))
	for name, value := range sysVars {
		if hasLike {
			s := name
			if isIlike {
				s = strings.ToLower(s)
			}
			if !WildcardMatch(likePattern, s) {
				continue
			}
		}
		row := make([]interface{}, 2)
		row[0] = name
		gsv, ok := GSysVariables.GetDefinitionOfSysVar(name)
		if !ok {
			return moerr.NewInternalError(ses.GetRequestContext(), errorSystemVariableDoesNotExist())
		}
		row[1] = value
		if svbt, ok2 := gsv.GetType().(SystemVariableBoolType); ok2 {
			if svbt.IsTrue(value) {
				row[1] = "on"
			} else {
				row[1] = "off"
			}
		}
		rows = append(rows, row)
	}

	if sv.Where != nil {
		bat, err := constructVarBatch(ses, rows)
		if err != nil {
			return err
		}
		binder := plan2.NewDefaultBinder(proc.Ctx, nil, nil, &plan2.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, []string{"variable_name", "value"})
		planExpr, err := binder.BindExpr(sv.Where.Expr, 0, false)
		if err != nil {
			return err
		}

		executor, err := colexec.NewExpressionExecutor(proc, planExpr)
		if err != nil {
			return err
		}
		vec, err := executor.Eval(proc, []*batch.Batch{bat})
		if err != nil {
			executor.Free()
			return err
		}

		bs := vector.MustFixedCol[bool](vec)
		sels := proc.Mp().GetSels()
		for i, b := range bs {
			if b {
				sels = append(sels, int64(i))
			}
		}
		executor.Free()

		bat.Shrink(sels)
		proc.Mp().PutSels(sels)
		v0 := vector.MustStrCol(bat.Vecs[0])
		v1 := vector.MustStrCol(bat.Vecs[1])
		rows = rows[:len(v0)]
		for i := range v0 {
			rows[i][0] = v0[i]
			rows[i][1] = v1[i]
		}
		bat.Clean(proc.Mp())
	}

	//sort by name
	sort.Slice(rows, func(i, j int) bool {
		return rows[i][0].(string) < rows[j][0].(string)
	})

	for _, row := range rows {
		mrs.AddRow(row)
	}

	return err
}

/*
handle show variables
*/
func handleShowVariables(ses TempInter, sv *tree.ShowVariables, proc *process.Process, isLastStmt bool) error {
	proto := ses.GetMysqlProtocol()
	err := doShowVariables(ses.(*Session), proc, sv)
	if err != nil {
		return err
	}
	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, isLastStmt)

	if err := proto.SendResponse(ses.GetRequestContext(), resp); err != nil {
		return moerr.NewInternalError(ses.GetRequestContext(), "routine send response failed. error:%v ", err)
	}
	return err
}

func constructVarBatch(ses *Session, rows [][]interface{}) (*batch.Batch, error) {
	bat := batch.New(true, []string{"Variable_name", "Value"})
	typ := types.New(types.T_varchar, types.MaxVarcharLen, 0)
	cnt := len(rows)
	bat.SetRowCount(cnt)
	v0 := make([]string, cnt)
	v1 := make([]string, cnt)
	for i, row := range rows {
		v0[i] = row[0].(string)
		v1[i] = fmt.Sprintf("%v", row[1])
	}
	bat.Vecs[0] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[0], v0, nil, ses.GetMemPool())
	bat.Vecs[1] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[1], v1, nil, ses.GetMemPool())
	return bat, nil
}

func constructCollationBatch(ses *Session, rows [][]interface{}) (*batch.Batch, error) {
	bat := batch.New(true, []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen", "Pad_attribute"})
	typ := types.New(types.T_varchar, types.MaxVarcharLen, 0)
	longlongTyp := types.New(types.T_int64, 0, 0)
	longTyp := types.New(types.T_int32, 0, 0)
	cnt := len(rows)
	bat.SetRowCount(cnt)
	v0 := make([]string, cnt)
	v1 := make([]string, cnt)
	v2 := make([]int64, cnt)
	v3 := make([]string, cnt)
	v4 := make([]string, cnt)
	v5 := make([]int32, cnt)
	v6 := make([]string, cnt)
	for i, row := range rows {
		v0[i] = row[0].(string)
		v1[i] = row[1].(string)
		v2[i] = row[2].(int64)
		v3[i] = row[3].(string)
		v4[i] = row[4].(string)
		v5[i] = row[5].(int32)
		v6[i] = row[6].(string)
	}
	bat.Vecs[0] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[0], v0, nil, ses.GetMemPool())
	bat.Vecs[1] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[1], v1, nil, ses.GetMemPool())
	bat.Vecs[2] = vector.NewVec(longlongTyp)
	vector.AppendFixedList[int64](bat.Vecs[2], v2, nil, ses.GetMemPool())
	bat.Vecs[3] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[3], v3, nil, ses.GetMemPool())
	bat.Vecs[4] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[4], v4, nil, ses.GetMemPool())
	bat.Vecs[5] = vector.NewVec(longTyp)
	vector.AppendFixedList[int32](bat.Vecs[5], v5, nil, ses.GetMemPool())
	bat.Vecs[6] = vector.NewVec(typ)
	vector.AppendStringList(bat.Vecs[6], v6, nil, ses.GetMemPool())
	return bat, nil
}

func handleAnalyzeStmt(requestCtx context.Context, ses *Session, stmt *tree.AnalyzeStmt) error {
	// rewrite analyzeStmt to `select approx_count_distinct(col), .. from tbl`
	// IMO, this approach is simple and future-proof
	// Although this rewriting processing could have been handled in rewrite module,
	// `handleAnalyzeStmt` can be easily managed by cron jobs in the future
	ctx := tree.NewFmtCtx(dialect.MYSQL)
	ctx.WriteString("select ")
	for i, ident := range stmt.Cols {
		if i > 0 {
			ctx.WriteByte(',')
		}
		ctx.WriteString("approx_count_distinct(")
		ctx.WriteString(string(ident))
		ctx.WriteByte(')')
	}
	ctx.WriteString(" from ")
	stmt.Table.Format(ctx)
	sql := ctx.String()
	//backup the inside statement
	prevInsideStmt := ses.ReplaceDerivedStmt(true)
	defer func() {
		//restore the inside statement
		ses.ReplaceDerivedStmt(prevInsideStmt)
	}()
	return doComQuery(requestCtx, ses, &UserInput{sql: sql})
}
func GetExplainColumns(ctx context.Context, explainColName string) ([]interface{}, error) {
	cols := []*plan2.ColDef{
		{Typ: &plan2.Type{Id: int32(types.T_varchar)}, Name: explainColName},
	}
	columns := make([]interface{}, len(cols))
	var err error = nil
	for i, col := range cols {
		c := new(MysqlColumn)
		c.SetName(col.Name)
		err = convertEngineTypeToMysqlType(ctx, types.T(col.Typ.Id), c)
		if err != nil {
			return nil, err
		}
		columns[i] = c
	}
	return columns, err
}

func getExplainOption(requestCtx context.Context, options []tree.OptionElem) (*explain.ExplainOptions, error) {
	es := explain.NewExplainDefaultOptions()
	if options == nil {
		return es, nil
	} else {
		for _, v := range options {
			if strings.EqualFold(v.Name, "VERBOSE") {
				if strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL" {
					es.Verbose = true
				} else if strings.EqualFold(v.Value, "FALSE") {
					es.Verbose = false
				} else {
					return nil, moerr.NewInvalidInput(requestCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
				}
			} else if strings.EqualFold(v.Name, "ANALYZE") {
				if strings.EqualFold(v.Value, "TRUE") || v.Value == "NULL" {
					es.Analyze = true
				} else if strings.EqualFold(v.Value, "FALSE") {
					es.Analyze = false
				} else {
					return nil, moerr.NewInvalidInput(requestCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
				}
			} else if strings.EqualFold(v.Name, "FORMAT") {
				if strings.EqualFold(v.Value, "TEXT") {
					es.Format = explain.EXPLAIN_FORMAT_TEXT
				} else if strings.EqualFold(v.Value, "JSON") {
					return nil, moerr.NewNotSupported(requestCtx, "Unsupport explain format '%s'", v.Value)
				} else if strings.EqualFold(v.Value, "DOT") {
					return nil, moerr.NewNotSupported(requestCtx, "Unsupport explain format '%s'", v.Value)
				} else {
					return nil, moerr.NewInvalidInput(requestCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
				}
			} else {
				return nil, moerr.NewInvalidInput(requestCtx, "invalid explain option '%s', valud '%s'", v.Name, v.Value)
			}
		}
		return es, nil
	}
}

func buildMoExplainQuery(explainColName string, buffer *explain.ExplainDataBuffer, session *Session, fill func(interface{}, *batch.Batch) error) error {
	bat := batch.New(true, []string{explainColName})
	rs := buffer.Lines
	vs := make([][]byte, len(rs))

	count := 0
	for _, r := range rs {
		str := []byte(r)
		vs[count] = str
		count++
	}
	vs = vs[:count]
	vec := vector.NewVec(types.T_varchar.ToType())
	defer vec.Free(session.GetMemPool())
	vector.AppendBytesList(vec, vs, nil, session.GetMemPool())
	bat.Vecs[0] = vec
	bat.SetRowCount(count)

	err := fill(session, bat)
	if err != nil {
		return err
	}
	// to trigger save result meta
	err = fill(session, nil)
	return err
}

// Note: for pass the compile quickly. We will remove the comments in the future.
func handleExplainStmt(requestCtx context.Context, ses TempInter, stmt *tree.ExplainStmt) error {
	es, err := getExplainOption(requestCtx, stmt.Options)
	if err != nil {
		return err
	}

	//get query optimizer and execute Optimize
	plan0, err := buildPlan(requestCtx, ses.(*Session), ses.GetTxnCompileCtx(), stmt.Statement)
	if err != nil {
		return err
	}
	if plan0.GetQuery() == nil {
		return moerr.NewNotSupported(requestCtx, "the sql query plan does not support explain.")
	}
	// generator query explain
	explainQuery := explain.NewExplainQueryImpl(plan0.GetQuery())

	// build explain data buffer
	buffer := explain.NewExplainDataBuffer()
	err = explainQuery.ExplainPlan(requestCtx, buffer, es)
	if err != nil {
		return err
	}

	protocol := ses.GetMysqlProtocol()

	explainColName := "QUERY PLAN"
	columns, err := GetExplainColumns(requestCtx, explainColName)
	if err != nil {
		return err
	}

	//	Step 1 : send column count and column definition.
	//send column count
	colCnt := uint64(len(columns))
	err = protocol.SendColumnCountPacket(colCnt)
	if err != nil {
		return err
	}
	//send columns
	//column_count * Protocol::ColumnDefinition packets
	cmd := ses.GetCmd()
	mrs := ses.GetMysqlResultSet()
	for _, c := range columns {
		mysqlc := c.(Column)
		mrs.AddColumn(mysqlc)
		//	mysql COM_QUERY response: send the column definition per column
		err := protocol.SendColumnDefinitionPacket(requestCtx, mysqlc, int(cmd))
		if err != nil {
			return err
		}
	}

	//	mysql COM_QUERY response: End after the column has been sent.
	//	send EOF packet
	err = protocol.SendEOFPacketIf(0, ses.GetServerStatus())
	if err != nil {
		return err
	}

	err = buildMoExplainQuery(explainColName, buffer, ses.(*Session), getDataFromPipeline)
	if err != nil {
		return err
	}

	err = protocol.sendEOFOrOkPacket(0, ses.GetServerStatus())
	if err != nil {
		return err
	}
	return nil
}

func doPrepareStmt(ctx context.Context, ses *Session, st *tree.PrepareStmt, sql string) (*PrepareStmt, error) {
	preparePlan, err := buildPlan(ctx, ses, ses.GetTxnCompileCtx(), st)
	if err != nil {
		return nil, err
	}

	prepareStmt := &PrepareStmt{
		Name:                preparePlan.GetDcl().GetPrepare().GetName(),
		Sql:                 sql,
		PreparePlan:         preparePlan,
		PrepareStmt:         st.Stmt,
		getFromSendLongData: make(map[int]struct{}),
	}
	prepareStmt.InsertBat = ses.GetTxnCompileCtx().GetProcess().GetPrepareBatch()
	err = ses.SetPrepareStmt(preparePlan.GetDcl().GetPrepare().GetName(), prepareStmt)

	return prepareStmt, err
}

// handlePrepareStmt
func handlePrepareStmt(ctx context.Context, ses TempInter, st *tree.PrepareStmt, sql string) (*PrepareStmt, error) {
	return doPrepareStmt(ctx, ses.(*Session), st, sql)
}

func doPrepareString(ctx context.Context, ses *Session, st *tree.PrepareString) (*PrepareStmt, error) {
	v, err := ses.GetGlobalVar("lower_case_table_names")
	if err != nil {
		return nil, err
	}
	stmts, err := mysql.Parse(ctx, st.Sql, v.(int64))
	if err != nil {
		return nil, err
	}

	preparePlan, err := buildPlan(ses.GetRequestContext(), ses, ses.GetTxnCompileCtx(), st)
	if err != nil {
		return nil, err
	}
	prepareStmt := &PrepareStmt{
		Name:        preparePlan.GetDcl().GetPrepare().GetName(),
		Sql:         st.Sql,
		PreparePlan: preparePlan,
		PrepareStmt: stmts[0],
	}
	prepareStmt.InsertBat = ses.GetTxnCompileCtx().GetProcess().GetPrepareBatch()
	err = ses.SetPrepareStmt(preparePlan.GetDcl().GetPrepare().GetName(), prepareStmt)
	return prepareStmt, err
}

// handlePrepareString
func handlePrepareString(ctx context.Context, ses TempInter, st *tree.PrepareString) (*PrepareStmt, error) {
	return doPrepareString(ctx, ses.(*Session), st)
}

func doDeallocate(ctx context.Context, ses *Session, st *tree.Deallocate) error {
	deallocatePlan, err := buildPlan(ctx, ses, ses.GetTxnCompileCtx(), st)
	if err != nil {
		return err
	}
	ses.RemovePrepareStmt(deallocatePlan.GetDcl().GetDeallocate().GetName())
	return nil
}

func doReset(ctx context.Context, ses *Session, st *tree.Reset) error {
	return nil
}

// handleDeallocate
func handleDeallocate(ctx context.Context, ses TempInter, st *tree.Deallocate) error {
	return doDeallocate(ctx, ses.(*Session), st)
}

// handleReset
func handleReset(ctx context.Context, ses TempInter, st *tree.Reset) error {
	return doReset(ctx, ses.(*Session), st)
}

func handleCreatePublication(ctx context.Context, ses TempInter, cp *tree.CreatePublication) error {
	return doCreatePublication(ctx, ses.(*Session), cp)
}

func handleAlterPublication(ctx context.Context, ses TempInter, ap *tree.AlterPublication) error {
	return doAlterPublication(ctx, ses.(*Session), ap)
}

func handleDropPublication(ctx context.Context, ses TempInter, dp *tree.DropPublication) error {
	return doDropPublication(ctx, ses.(*Session), dp)
}

func handleCreateStage(ctx context.Context, ses TempInter, cs *tree.CreateStage) error {
	return doCreateStage(ctx, ses.(*Session), cs)
}

func handleAlterStage(ctx context.Context, ses TempInter, as *tree.AlterStage) error {
	return doAlterStage(ctx, ses.(*Session), as)
}

func handleDropStage(ctx context.Context, ses TempInter, ds *tree.DropStage) error {
	return doDropStage(ctx, ses.(*Session), ds)
}

// handleCreateAccount creates a new user-level tenant in the context of the tenant SYS
// which has been initialized.
func handleCreateAccount(ctx context.Context, ses TempInter, ca *tree.CreateAccount) error {
	//step1 : create new account.
	return InitGeneralTenant(ctx, ses.(*Session), ca)
}

// handleDropAccount drops a new user-level tenant
func handleDropAccount(ctx context.Context, ses TempInter, da *tree.DropAccount) error {
	return doDropAccount(ctx, ses.(*Session), da)
}

// handleDropAccount drops a new user-level tenant
func handleAlterAccount(ctx context.Context, ses TempInter, aa *tree.AlterAccount) error {
	return doAlterAccount(ctx, ses.(*Session), aa)
}

// handleAlterDatabaseConfig alter a database's mysql_compatibility_mode
func handleAlterDataBaseConfig(ctx context.Context, ses TempInter, ad *tree.AlterDataBaseConfig) error {
	return doAlterDatabaseConfig(ctx, ses.(*Session), ad)
}

// handleAlterAccountConfig alter a account's mysql_compatibility_mode
func handleAlterAccountConfig(ctx context.Context, ses TempInter, st *tree.AlterDataBaseConfig) error {
	return doAlterAccountConfig(ctx, ses.(*Session), st)
}

// handleCreateUser creates the user for the tenant
func handleCreateUser(ctx context.Context, ses TempInter, cu *tree.CreateUser) error {
	tenant := ses.GetTenantInfo()

	//step1 : create the user
	return InitUser(ctx, ses.(*Session), tenant, cu)
}

// handleDropUser drops the user for the tenant
func handleDropUser(ctx context.Context, ses TempInter, du *tree.DropUser) error {
	return doDropUser(ctx, ses.(*Session), du)
}

func handleAlterUser(ctx context.Context, ses TempInter, au *tree.AlterUser) error {
	return doAlterUser(ctx, ses.(*Session), au)
}

// handleCreateRole creates the new role
func handleCreateRole(ctx context.Context, ses TempInter, cr *tree.CreateRole) error {
	tenant := ses.GetTenantInfo()

	//step1 : create the role
	return InitRole(ctx, ses.(*Session), tenant, cr)
}

// handleDropRole drops the role
func handleDropRole(ctx context.Context, ses TempInter, dr *tree.DropRole) error {
	return doDropRole(ctx, ses.(*Session), dr)
}

func handleCreateFunction(ctx context.Context, ses TempInter, cf *tree.CreateFunction) error {
	tenant := ses.GetTenantInfo()
	return InitFunction(ctx, ses.(*Session), tenant, cf)
}

func handleDropFunction(ctx context.Context, ses TempInter, df *tree.DropFunction, proc *process.Process) error {
	return doDropFunction(ctx, ses.(*Session), df, func(path string) error {
		return proc.FileService.Delete(ctx, path)
	})
}

func handleCreateProcedure(ctx context.Context, ses TempInter, cp *tree.CreateProcedure) error {
	tenant := ses.GetTenantInfo()

	return InitProcedure(ctx, ses.(*Session), tenant, cp)
}

func handleDropProcedure(ctx context.Context, ses TempInter, dp *tree.DropProcedure) error {
	return doDropProcedure(ctx, ses.(*Session), dp)
}

func handleCallProcedure(ctx context.Context, ses TempInter, call *tree.CallStmt, proc *process.Process) error {
	proto := ses.GetMysqlProtocol()
	results, err := doInterpretCall(ctx, ses.(*Session), call)
	if err != nil {
		return err
	}

	resp := NewGeneralOkResponse(COM_QUERY, ses.GetServerStatus())

	if len(results) == 0 {
		if err := proto.SendResponse(ses.GetRequestContext(), resp); err != nil {
			return moerr.NewInternalError(ses.GetRequestContext(), "routine send response failed. error:%v ", err)
		}
	} else {
		for i, result := range results {
			mer := NewMysqlExecutionResult(0, 0, 0, 0, result.(*MysqlResultSet))
			resp = ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, i == len(results)-1)
			if err := proto.SendResponse(ses.GetRequestContext(), resp); err != nil {
				return moerr.NewInternalError(ses.GetRequestContext(), "routine send response failed. error:%v ", err)
			}
		}
	}
	return nil
}

// handleGrantRole grants the role
func handleGrantRole(ctx context.Context, ses TempInter, gr *tree.GrantRole) error {
	return doGrantRole(ctx, ses.(*Session), gr)
}

// handleRevokeRole revokes the role
func handleRevokeRole(ctx context.Context, ses TempInter, rr *tree.RevokeRole) error {
	return doRevokeRole(ctx, ses.(*Session), rr)
}

// handleGrantRole grants the privilege to the role
func handleGrantPrivilege(ctx context.Context, ses TempInter, gp *tree.GrantPrivilege) error {
	return doGrantPrivilege(ctx, ses, gp)
}

// handleRevokePrivilege revokes the privilege from the user or role
func handleRevokePrivilege(ctx context.Context, ses TempInter, rp *tree.RevokePrivilege) error {
	return doRevokePrivilege(ctx, ses, rp)
}

// handleSwitchRole switches the role to another role
func handleSwitchRole(ctx context.Context, ses TempInter, sr *tree.SetRole) error {
	return doSwitchRole(ctx, ses.(*Session), sr)
}

func doKill(ctx context.Context, ses *Session, k *tree.Kill) error {
	var err error
	//true: kill a connection
	//false: kill a query in a connection
	idThatKill := uint64(ses.GetConnectionID())
	if !k.Option.Exist || k.Option.Typ == tree.KillTypeConnection {
		err = gRtMgr.kill(ctx, true, idThatKill, k.ConnectionId, "")
	} else {
		err = gRtMgr.kill(ctx, false, idThatKill, k.ConnectionId, k.StmtOption.StatementId)
	}
	return err
}

// handleKill kill a connection or query
func handleKill(ctx context.Context, ses *Session, k *tree.Kill) error {
	var err error
	proto := ses.GetMysqlProtocol()
	err = doKill(ctx, ses, k)
	if err != nil {
		return err
	}
	resp := NewGeneralOkResponse(COM_QUERY, ses.GetServerStatus())
	if err = proto.SendResponse(ctx, resp); err != nil {
		return moerr.NewInternalError(ctx, "routine send response failed. error:%v ", err)
	}
	return err
}

// handleShowAccounts lists the info of accounts
func handleShowAccounts(ctx context.Context, ses TempInter, sa *tree.ShowAccounts, isLastStmt bool) error {
	var err error
	proto := ses.GetMysqlProtocol()
	err = doShowAccounts(ctx, ses.(*Session), sa)
	if err != nil {
		return err
	}
	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, isLastStmt)

	if err = proto.SendResponse(ctx, resp); err != nil {
		return moerr.NewInternalError(ctx, "routine send response failed. error:%v ", err)
	}
	return err
}

// handleShowCollation lists the info of collation
func handleShowCollation(ses TempInter, sc *tree.ShowCollation, proc *process.Process, isLastStmt bool) error {
	var err error
	proto := ses.GetMysqlProtocol()
	err = doShowCollation(ses.(*Session), proc, sc)
	if err != nil {
		return err
	}
	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, isLastStmt)

	if err := proto.SendResponse(ses.GetRequestContext(), resp); err != nil {
		return moerr.NewInternalError(ses.GetRequestContext(), "routine send response failed. error:%v ", err)
	}
	return err
}

func doShowCollation(ses *Session, proc *process.Process, sc *tree.ShowCollation) error {
	var err error
	var bat *batch.Batch
	// var outputBatches []*batch.Batch

	// Construct the columns.
	col1 := new(MysqlColumn)
	col1.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	col1.SetName("Collation")

	col2 := new(MysqlColumn)
	col2.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	col2.SetName("Charset")

	col3 := new(MysqlColumn)
	col3.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	col3.SetName("Id")

	col4 := new(MysqlColumn)
	col4.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	col4.SetName("Default")

	col5 := new(MysqlColumn)
	col5.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	col5.SetName("Compiled")

	col6 := new(MysqlColumn)
	col6.SetColumnType(defines.MYSQL_TYPE_LONG)
	col6.SetName("Sortlen")

	col7 := new(MysqlColumn)
	col7.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	col7.SetName("Pad_attribute")

	mrs := ses.GetMysqlResultSet()
	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col4)
	mrs.AddColumn(col5)
	mrs.AddColumn(col6)
	mrs.AddColumn(col7)

	var hasLike = false
	var likePattern = ""
	var isIlike = false
	if sc.Like != nil {
		hasLike = true
		if sc.Like.Op == tree.ILIKE {
			isIlike = true
		}
		likePattern = strings.ToLower(sc.Like.Right.String())
	}

	// Construct the rows.
	rows := make([][]interface{}, 0, len(Collations))
	for _, collation := range Collations {
		if hasLike {
			s := collation.collationName
			if isIlike {
				s = strings.ToLower(s)
			}
			if !WildcardMatch(likePattern, s) {
				continue
			}
		}
		row := make([]interface{}, 7)
		row[0] = collation.collationName
		row[1] = collation.charset
		row[2] = collation.id
		row[3] = collation.isDefault
		row[4] = collation.isCompiled
		row[5] = collation.sortLen
		row[6] = collation.padAttribute
		rows = append(rows, row)
	}

	bat, err = constructCollationBatch(ses, rows)
	defer bat.Clean(proc.Mp())
	if err != nil {
		return err
	}

	if sc.Where != nil {
		binder := plan2.NewDefaultBinder(proc.Ctx, nil, nil, &plan2.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}, []string{"collation", "charset", "id", "default", "compiled", "sortlen", "pad_attribute"})
		planExpr, err := binder.BindExpr(sc.Where.Expr, 0, false)
		if err != nil {
			return err
		}

		executor, err := colexec.NewExpressionExecutor(proc, planExpr)
		if err != nil {
			return err
		}
		vec, err := executor.Eval(proc, []*batch.Batch{bat})
		if err != nil {
			executor.Free()
			return err
		}

		bs := vector.MustFixedCol[bool](vec)
		sels := proc.Mp().GetSels()
		for i, b := range bs {
			if b {
				sels = append(sels, int64(i))
			}
		}
		executor.Free()

		bat.Shrink(sels)
		proc.Mp().PutSels(sels)
		v0 := vector.MustStrCol(bat.Vecs[0])
		v1 := vector.MustStrCol(bat.Vecs[1])
		v2 := vector.MustFixedCol[int64](bat.Vecs[2])
		v3 := vector.MustStrCol(bat.Vecs[3])
		v4 := vector.MustStrCol(bat.Vecs[4])
		v5 := vector.MustFixedCol[int32](bat.Vecs[5])
		v6 := vector.MustStrCol(bat.Vecs[6])
		rows = rows[:len(v0)]
		for i := range v0 {
			rows[i][0] = v0[i]
			rows[i][1] = v1[i]
			rows[i][2] = v2[i]
			rows[i][3] = v3[i]
			rows[i][4] = v4[i]
			rows[i][5] = v5[i]
			rows[i][6] = v6[i]
		}
	}

	//sort by name
	sort.Slice(rows, func(i, j int) bool {
		return rows[i][0].(string) < rows[j][0].(string)
	})

	for _, row := range rows {
		mrs.AddRow(row)
	}

	// oq := newFakeOutputQueue(mrs)
	// if err = fillResultSet(oq, bat, ses); err != nil {
	// 	return err
	// }

	ses.SetMysqlResultSet(mrs)
	ses.rs = mysqlColDef2PlanResultColDef(mrs)

	// save query result
	if openSaveQueryResult(ses) {
		if err := saveQueryResult(ses, bat); err != nil {
			return err
		}
		if err := saveQueryResultMeta(ses); err != nil {
			return err
		}
	}

	return err
}

func handleShowSubscriptions(ctx context.Context, ses TempInter, ss *tree.ShowSubscriptions, isLastStmt bool) error {
	var err error
	proto := ses.GetMysqlProtocol()
	err = doShowSubscriptions(ctx, ses.(*Session), ss)
	if err != nil {
		return err
	}
	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, isLastStmt)

	if err = proto.SendResponse(ctx, resp); err != nil {
		return moerr.NewInternalError(ctx, "routine send response failed. error:%v ", err)
	}
	return err
}

func doShowBackendServers(ses *Session) error {
	// Construct the columns.
	col1 := new(MysqlColumn)
	col1.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col1.SetName("UUID")

	col2 := new(MysqlColumn)
	col2.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col2.SetName("Address")

	col3 := new(MysqlColumn)
	col3.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col3.SetName("Work State")

	col4 := new(MysqlColumn)
	col4.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	col4.SetName("Labels")

	mrs := ses.GetMysqlResultSet()
	mrs.AddColumn(col1)
	mrs.AddColumn(col2)
	mrs.AddColumn(col3)
	mrs.AddColumn(col4)

	var filterLabels = func(labels map[string]string) map[string]string {
		var reservedLabels = map[string]struct{}{
			"os_user":      {},
			"os_sudouser":  {},
			"program_name": {},
		}
		for k := range labels {
			if _, ok := reservedLabels[k]; ok || strings.HasPrefix(k, "_") {
				delete(labels, k)
			}
		}
		return labels
	}

	var appendFn = func(s *metadata.CNService) {
		row := make([]interface{}, 4)
		row[0] = s.ServiceID
		row[1] = s.SQLAddress
		row[2] = s.WorkState.String()
		var labelStr string
		for key, value := range s.Labels {
			labelStr += fmt.Sprintf("%s:%s;", key, strings.Join(value.Labels, ","))
		}
		row[3] = labelStr
		mrs.AddRow(row)
	}

	tenant := ses.GetTenantInfo().GetTenant()
	var se clusterservice.Selector
	labels, err := ParseLabel(getLabelPart(ses.GetUserName()))
	if err != nil {
		return err
	}
	labels["account"] = tenant
	se = clusterservice.NewSelector().SelectByLabel(
		filterLabels(labels), clusterservice.Contain)
	if isSysTenant(tenant) {
		u := ses.GetTenantInfo().GetUser()
		// For super use dump and root, we should list all servers.
		if isSuperUser(u) {
			clusterservice.GetMOCluster().GetCNService(
				clusterservice.NewSelectAll(), func(s metadata.CNService) bool {
					appendFn(&s)
					return true
				})
		} else {
			route.RouteForSuperTenant(se, u, nil, appendFn)
		}
	} else {
		route.RouteForCommonTenant(se, nil, appendFn)
	}
	return nil
}

func handleShowBackendServers(ctx context.Context, ses TempInter, isLastStmt bool) error {
	var err error
	proto := ses.GetMysqlProtocol()
	if err := doShowBackendServers(ses.(*Session)); err != nil {
		return err
	}

	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, isLastStmt)
	if err := proto.SendResponse(ctx, resp); err != nil {
		return moerr.NewInternalError(ctx, "routine send response failed, error: %v ", err)
	}
	return err
}

func handleEmptyStmt(ctx context.Context, ses TempInter, stmt *tree.EmptyStmt) error {
	var err error
	proto := ses.GetMysqlProtocol()

	resp := NewGeneralOkResponse(COM_QUERY, ses.GetServerStatus())
	if err = proto.SendResponse(ctx, resp); err != nil {
		return moerr.NewInternalError(ctx, "routine send response failed. error:%v ", err)
	}
	return err
}

func processLoadLocal(ctx context.Context, ses TempInter, param *tree.ExternParam, writer *io.PipeWriter) (err error) {
	proto := ses.GetMysqlProtocol()
	defer func() {
		err2 := writer.Close()
		if err == nil {
			err = err2
		}
	}()
	err = plan2.InitInfileParam(param)
	if err != nil {
		return
	}
	err = proto.sendLocalInfileRequest(param.Filepath)
	if err != nil {
		return
	}
	start := time.Now()
	var msg interface{}
	msg, err = proto.GetTcpConnection().Read(goetty.ReadOptions{})
	if err != nil {
		proto.SetSequenceID(proto.GetSequenceId() + 1)
		if errors.Is(err, errorInvalidLength0) {
			return nil
		}
		if moerr.IsMoErrCode(err, moerr.ErrInvalidInput) {
			err = moerr.NewInvalidInput(ctx, "cannot read '%s' from client,please check the file path, user privilege and if client start with --local-infile", param.Filepath)
		}
		return
	}

	packet, ok := msg.(*Packet)
	if !ok {
		proto.SetSequenceID(proto.GetSequenceId() + 1)
		err = moerr.NewInvalidInput(ctx, "invalid packet")
		return
	}

	proto.SetSequenceID(uint8(packet.SequenceID + 1))
	seq := uint8(packet.SequenceID + 1)
	length := packet.Length
	if length == 0 {
		return
	}
	ses.CountPayload(len(packet.Payload))

	skipWrite := false
	// If inner error occurs(unexpected or expected(ctrl-c)), proc.LoadLocalReader will be closed.
	// Then write will return error, but we need to read the rest of the data and not write it to pipe.
	// So we need a flag[skipWrite] to tell us whether we need to write the data to pipe.
	// https://github.com/matrixorigin/matrixone/issues/6665#issuecomment-1422236478

	_, err = writer.Write(packet.Payload)
	if err != nil {
		skipWrite = true // next, we just need read the rest of the data,no need to write it to pipe.
		//logError(ses, ses.GetDebugString(),
		//	"Failed to load local file",
		//	zap.String("path", param.Filepath),
		//	zap.Error(err))
	}
	epoch, printEvery, minReadTime, maxReadTime, minWriteTime, maxWriteTime := uint64(0), uint64(1024), 24*time.Hour, time.Nanosecond, 24*time.Hour, time.Nanosecond
	for {
		readStart := time.Now()
		msg, err = proto.GetTcpConnection().Read(goetty.ReadOptions{})
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrInvalidInput) {
				seq += 1
				proto.SetSequenceID(seq)
				err = nil
			}
			break
		}
		readTime := time.Since(readStart)
		if readTime > maxReadTime {
			maxReadTime = readTime
		}
		if readTime < minReadTime {
			minReadTime = readTime
		}
		packet, ok = msg.(*Packet)
		if !ok {
			err = moerr.NewInvalidInput(ctx, "invalid packet")
			seq += 1
			proto.SetSequenceID(seq)
			break
		}
		seq = uint8(packet.SequenceID + 1)
		proto.SetSequenceID(seq)
		ses.CountPayload(len(packet.Payload))

		writeStart := time.Now()
		if !skipWrite {
			_, err = writer.Write(packet.Payload)
			if err != nil {
				//logError(ses, ses.GetDebugString(),
				//	"Failed to load local file",
				//	zap.String("path", param.Filepath),
				//	zap.Uint64("epoch", epoch),
				//	zap.Error(err))
				skipWrite = true
			}
			writeTime := time.Since(writeStart)
			if writeTime > maxWriteTime {
				maxWriteTime = writeTime
			}
			if writeTime < minWriteTime {
				minWriteTime = writeTime
			}
		}
		if epoch%printEvery == 0 {
			logDebugf(ses.GetDebugString(), "load local '%s', epoch: %d, skipWrite: %v, minReadTime: %s, maxReadTime: %s, minWriteTime: %s, maxWriteTime: %s,", param.Filepath, epoch, skipWrite, minReadTime.String(), maxReadTime.String(), minWriteTime.String(), maxWriteTime.String())
			minReadTime, maxReadTime, minWriteTime, maxWriteTime = 24*time.Hour, time.Nanosecond, 24*time.Hour, time.Nanosecond
		}
		epoch += 1
	}
	logDebugf(ses.GetDebugString(), "load local '%s', read&write all data from client cost: %s", param.Filepath, time.Since(start))
	return
}
