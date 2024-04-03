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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	gotrace "runtime/trace"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend/constant"
	"go.uber.org/zap"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/explain"
	util2 "github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/sync/errgroup"
)

func createDropDatabaseErrorInfo() string {
	return "CREATE/DROP of database is not supported in transactions"
}

func onlyCreateStatementErrorInfo() string {
	return "Only CREATE of DDL is supported in transactions"
}

func administrativeCommandIsUnsupportedInTxnErrorInfo() string {
	return "administrative command is unsupported in transactions"
}

func unclassifiedStatementInUncommittedTxnErrorInfo() string {
	return "unclassified statement appears in uncommitted transaction"
}

func writeWriteConflictsErrorInfo() string {
	return "Write conflicts detected. Previous transaction need to be aborted."
}

const (
	prefixPrepareStmtName       = "__mo_stmt_id"
	prefixPrepareStmtSessionVar = "__mo_stmt_var"
)

// ExecRequest the server execute the commands from the client following the mysql's routine
func ExecRequest(requestCtx context.Context, ses *Session, req *Request) (resp *Response, err error) {
	//defer func() {
	//	if e := recover(); e != nil {
	//		moe, ok := e.(*moerr.Error)
	//		if !ok {
	//			err = moerr.ConvertPanicError(requestCtx, e)
	//			resp = NewGeneralErrorResponse(COM_QUERY, ses.GetServerStatus(), err)
	//		} else {
	//			resp = NewGeneralErrorResponse(COM_QUERY, ses.GetServerStatus(), moe)
	//		}
	//	}
	//}()

	var span trace.Span
	requestCtx, span = trace.Start(requestCtx, "MysqlCmdExecutor.ExecRequest",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End()

	var sql string
	logDebugf(ses.GetDebugString(), "cmd %v", req.GetCmd())
	ses.SetCmd(req.GetCmd())
	switch req.GetCmd() {
	case COM_QUIT:
		return resp, moerr.GetMysqlClientQuit()
	case COM_QUERY:
		var query = string(req.GetData().([]byte))
		ses.addSqlCount(1)
		logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(SubStringFromBegin(query, int(gPu.SV.LengthOfQueryPrinted))))
		err = doComQuery(requestCtx, ses, &UserInput{sql: query})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_QUERY, ses.GetServerStatus(), err)
		}
		return resp, nil
	case COM_INIT_DB:
		var dbname = string(req.GetData().([]byte))
		ses.addSqlCount(1)
		query := "use `" + dbname + "`"
		err = doComQuery(requestCtx, ses, &UserInput{sql: query})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_INIT_DB, ses.GetServerStatus(), err)
		}

		return resp, nil
	case COM_FIELD_LIST:
		var payload = string(req.GetData().([]byte))
		ses.addSqlCount(1)
		query := makeCmdFieldListSql(payload)
		err = doComQuery(requestCtx, ses, &UserInput{sql: query})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_FIELD_LIST, ses.GetServerStatus(), err)
		}

		return resp, nil
	case COM_PING:
		resp = NewGeneralOkResponse(COM_PING, ses.GetServerStatus())

		return resp, nil

	case COM_STMT_PREPARE:
		ses.SetCmd(COM_STMT_PREPARE)
		sql = string(req.GetData().([]byte))
		ses.addSqlCount(1)

		// rewrite to "Prepare stmt_name from 'xxx'"
		newLastStmtID := ses.GenNewStmtId()
		newStmtName := getPrepareStmtName(newLastStmtID)
		sql = fmt.Sprintf("prepare %s from %s", newStmtName, sql)
		logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))

		err = doComQuery(requestCtx, ses, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_PREPARE, ses.GetServerStatus(), err)
		}
		return resp, nil

	case COM_STMT_EXECUTE:
		ses.SetCmd(COM_STMT_EXECUTE)
		data := req.GetData().([]byte)
		var prepareStmt *PrepareStmt
		sql, prepareStmt, err = parseStmtExecute(requestCtx, ses, data)
		if err != nil {
			return NewGeneralErrorResponse(COM_STMT_EXECUTE, ses.GetServerStatus(), err), nil
		}
		err = doComQuery(requestCtx, ses, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_EXECUTE, ses.GetServerStatus(), err)
		}
		if prepareStmt.params != nil {
			prepareStmt.params.GetNulls().Reset()
			for k := range prepareStmt.getFromSendLongData {
				delete(prepareStmt.getFromSendLongData, k)
			}
		}
		return resp, nil

	case COM_STMT_SEND_LONG_DATA:
		ses.SetCmd(COM_STMT_SEND_LONG_DATA)
		data := req.GetData().([]byte)
		err = parseStmtSendLongData(requestCtx, ses, data)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_SEND_LONG_DATA, ses.GetServerStatus(), err)
			return resp, nil
		}
		return nil, nil

	case COM_STMT_CLOSE:
		data := req.GetData().([]byte)

		// rewrite to "deallocate Prepare stmt_name"
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		stmtName := getPrepareStmtName(stmtID)
		sql = fmt.Sprintf("deallocate prepare %s", stmtName)
		logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))

		err = doComQuery(requestCtx, ses, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_CLOSE, ses.GetServerStatus(), err)
		}
		return resp, nil

	case COM_STMT_RESET:
		data := req.GetData().([]byte)

		//Payload of COM_STMT_RESET
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		stmtName := getPrepareStmtName(stmtID)
		sql = fmt.Sprintf("reset prepare %s", stmtName)
		logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))
		err = doComQuery(requestCtx, ses, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_RESET, ses.GetServerStatus(), err)
		}
		return resp, nil

	case COM_SET_OPTION:
		data := req.GetData().([]byte)
		err := handleSetOption(requestCtx, ses, data)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_SET_OPTION, ses.GetServerStatus(), err)
		}
		return NewGeneralOkResponse(COM_SET_OPTION, ses.GetServerStatus()), nil

	default:
		resp = NewGeneralErrorResponse(req.GetCmd(), ses.GetServerStatus(), moerr.NewInternalError(requestCtx, "unsupported command. 0x%x", req.GetCmd()))
	}
	return resp, nil
}

// execute query
func doComQuery(requestCtx context.Context, ses *Session, input *UserInput) (retErr error) {
	beginInstant := time.Now()
	requestCtx = appendStatementAt(requestCtx, beginInstant)
	input.genSqlSourceType(ses)
	ses.SetShowStmtType(NotShowStatement)
	proto := ses.GetMysqlProtocol()
	ses.SetSql(input.getSql())

	if judgeIsClientBIQuery(input) {
		dialectEquivalentRewrite(input)
	}

	//the ses.GetUserName returns the user_name with the account_name.
	//here,we only need the user_name.
	userNameOnly := rootName
	proc := process.New(
		requestCtx,
		ses.GetMemPool(),
		gPu.TxnClient,
		nil,
		gPu.FileService,
		gPu.LockService,
		gPu.QueryService,
		gPu.HAKeeperClient,
		gPu.UdfService,
		gAicm)
	proc.CopyVectorPool(ses.proc)
	proc.CopyValueScanBatch(ses.proc)
	proc.Id = ses.getNextProcessId()
	proc.Lim.Size = gPu.SV.ProcessLimitationSize
	proc.Lim.BatchRows = gPu.SV.ProcessLimitationBatchRows
	proc.Lim.MaxMsgSize = gPu.SV.MaxMessageSize
	proc.Lim.PartitionRows = gPu.SV.ProcessLimitationPartitionRows
	proc.SessionInfo = process.SessionInfo{
		User:          ses.GetUserName(),
		Host:          gPu.SV.Host,
		ConnectionID:  uint64(proto.ConnectionID()),
		Database:      ses.GetDatabaseName(),
		Version:       makeServerVersion(gPu, serverVersion.Load().(string)),
		TimeZone:      ses.GetTimeZone(),
		StorageEngine: gPu.StorageEngine,
		LastInsertID:  ses.GetLastInsertID(),
		SqlHelper:     ses.GetSqlHelper(),
		Buf:           ses.GetBuffer(),
	}
	proc.SetStmtProfile(&ses.stmtProfile)
	proc.SetResolveVariableFunc(ses.txnCompileCtx.ResolveVariable)
	proc.InitSeq()
	// Copy curvalues stored in session to this proc.
	// Deep copy the map, takes some memory.
	ses.CopySeqToProc(proc)
	if ses.GetTenantInfo() != nil {
		proc.SessionInfo.Account = ses.GetTenantInfo().GetTenant()
		proc.SessionInfo.AccountId = ses.GetTenantInfo().GetTenantID()
		proc.SessionInfo.Role = ses.GetTenantInfo().GetDefaultRole()
		proc.SessionInfo.RoleId = ses.GetTenantInfo().GetDefaultRoleID()
		proc.SessionInfo.UserId = ses.GetTenantInfo().GetUserID()

		if len(ses.GetTenantInfo().GetVersion()) != 0 {
			proc.SessionInfo.Version = ses.GetTenantInfo().GetVersion()
		}
		userNameOnly = ses.GetTenantInfo().GetUser()
	} else {
		var accountId uint32
		accountId, retErr = defines.GetAccountId(requestCtx)
		if retErr != nil {
			return retErr
		}
		proc.SessionInfo.AccountId = accountId
		proc.SessionInfo.UserId = defines.GetUserId(requestCtx)
		proc.SessionInfo.RoleId = defines.GetRoleId(requestCtx)
	}
	var span trace.Span
	requestCtx, span = trace.Start(requestCtx, "MysqlCmdExecutor.doComQuery",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End()

	proc.SessionInfo.User = userNameOnly
	proc.SessionInfo.QueryId = ses.getQueryId(input.isInternal())
	ses.txnCompileCtx.SetProcess(proc)
	ses.proc.SessionInfo = proc.SessionInfo

	statsInfo := statistic.StatsInfo{ParseStartTime: beginInstant}
	requestCtx = statistic.ContextWithStatsInfo(requestCtx, &statsInfo)

	cws, err := GetComputationWrapper(ses.GetDatabaseName(),
		input,
		ses.GetUserName(),
		gPu.StorageEngine,
		proc, ses)

	ParseDuration := time.Since(beginInstant)

	if err != nil {
		statsInfo.ParseDuration = ParseDuration
		var err2 error
		requestCtx, err2 = RecordParseErrorStatement(requestCtx, ses, proc, beginInstant, parsers.HandleSqlForRecord(input.getSql()), input.getSqlSourceTypes(), err)
		if err2 != nil {
			return err2
		}
		retErr = err
		if _, ok := err.(*moerr.Error); !ok {
			retErr = moerr.NewParseError(requestCtx, err.Error())
		}
		logStatementStringStatus(requestCtx, ses, input.getSql(), fail, retErr)
		return retErr
	}

	singleStatement := len(cws) == 1
	if ses.GetCmd() == COM_STMT_PREPARE && !singleStatement {
		return moerr.NewNotSupported(requestCtx, "prepare multi statements")
	}

	defer func() {
		ses.SetMysqlResultSet(nil)
	}()

	canCache := true
	sqlRecord := parsers.HandleSqlForRecord(input.getSql())

	for i, cw := range cws {
		if cwft, ok := cw.(*TxnComputationWrapper); ok {
			if cwft.stmt.GetQueryType() == tree.QueryTypeDDL || cwft.stmt.GetQueryType() == tree.QueryTypeDCL ||
				cwft.stmt.GetQueryType() == tree.QueryTypeOth ||
				cwft.stmt.GetQueryType() == tree.QueryTypeTCL {
				if _, ok := cwft.stmt.(*tree.SetVar); !ok {
					ses.cleanCache()
				}
				canCache = false
			}
		}

		ses.SetMysqlResultSet(&MysqlResultSet{})
		ses.sentRows.Store(int64(0))
		ses.writeCsvBytes.Store(int64(0))
		proto.ResetStatistics() // move from getDataFromPipeline, for record column fields' data
		stmt := cw.GetAst()
		sqlType := input.getSqlSourceType(i)
		var err2 error
		requestCtx, err2 = RecordStatement(requestCtx, ses, proc, cw, beginInstant, sqlRecord[i], sqlType, singleStatement)
		if err2 != nil {
			return err2
		}

		statsInfo.Reset()
		//average parse duration
		statsInfo.ParseDuration = time.Duration(ParseDuration.Nanoseconds() / int64(len(cws)))

		tenant := ses.GetTenantNameWithStmt(stmt)
		//skip PREPARE statement here
		if ses.GetTenantInfo() != nil && !IsPrepareStatement(stmt) {
			err = authenticateUserCanExecuteStatement(requestCtx, ses, stmt)
			if err != nil {
				logStatementStatus(requestCtx, ses, stmt, fail, err)
				return err
			}
		}

		/*
				if it is in an active or multi-statement transaction, we check the type of the statement.
				Then we decide that if we can execute the statement.

			If we check the active transaction, it will generate the case below.
			case:
			set autocommit = 0;  <- no active transaction
			                     <- no active transaction
			drop table test1;    <- no active transaction, no error
			                     <- has active transaction
			drop table test1;    <- has active transaction, error
			                     <- has active transaction
		*/
		if ses.InActiveTransaction() {
			err = canExecuteStatementInUncommittedTransaction(requestCtx, ses, stmt)
			if err != nil {
				logStatementStatus(requestCtx, ses, stmt, fail, err)
				return err
			}
		}

		// update UnixTime for new query, which is used for now() / CURRENT_TIMESTAMP
		proc.UnixTime = time.Now().UnixNano()
		if ses.proc != nil {
			ses.proc.UnixTime = proc.UnixTime
		}
		execCtx := &ExecCtx{
			stmt:       stmt,
			isLastStmt: i >= len(cws)-1,
			tenant:     tenant,
			userName:   userNameOnly,
			sqlOfStmt:  sqlRecord[i],
			cw:         cw,
			proc:       proc,
			proto:      proto,
		}
		err = executeStmtWithTxn(requestCtx, ses, execCtx)
		if err != nil {
			return err
		}
	} // end of for

	if canCache && !ses.isCached(input.getSql()) {
		plans := make([]*plan.Plan, len(cws))
		stmts := make([]tree.Statement, len(cws))
		for i, cw := range cws {
			if cwft, ok := cw.(*TxnComputationWrapper); ok {
				if checkNodeCanCache(cwft.plan) {
					plans[i] = cwft.plan
					stmts[i] = cwft.stmt
				} else {
					cwft.Free()
					return nil
				}
			}
		}
		ses.cachePlan(input.getSql(), stmts, plans)
	}

	return nil
}

func executeStmtWithTxn(requestCtx context.Context,
	ses *Session,
	execCtx *ExecCtx,
) (err error) {
	// defer transaction state management.
	defer func() {
		err = finishTxnFunc(requestCtx, ses, err, execCtx)
	}()

	// statement management
	_, txnOp, err := ses.GetTxnHandler().GetTxnOperator()
	if err != nil {
		return err
	}

	//non derived statement
	if txnOp != nil && !ses.IsDerivedStmt() {
		//startStatement has been called
		ok, _ := ses.GetTxnHandler().calledStartStmt()
		if !ok {
			txnOp.GetWorkspace().StartStatement()
			ses.GetTxnHandler().enableStartStmt(txnOp.Txn().ID)
		}
	}

	// defer Start/End Statement management, called after finishTxnFunc()
	defer func() {
		// move finishTxnFunc() out to another defer so that if finishTxnFunc
		// paniced, the following is still called.
		var err3 error
		_, txnOp, err3 = ses.GetTxnHandler().GetTxnOperator()
		if err3 != nil {
			logError(ses, ses.GetDebugString(), err3.Error())
			return
		}
		//non derived statement
		if txnOp != nil && !ses.IsDerivedStmt() {
			//startStatement has been called
			ok, id := ses.GetTxnHandler().calledStartStmt()
			if ok && bytes.Equal(txnOp.Txn().ID, id) {
				txnOp.GetWorkspace().EndStatement()
			}
		}
		ses.GetTxnHandler().disableStartStmt()
	}()
	return executeStmt(requestCtx, ses, execCtx)
}

func executeStmt(requestCtx context.Context,
	ses *Session,
	execCtx *ExecCtx,
) (err error) {
	var span trace.Span
	requestCtx, span = trace.Start(requestCtx, "MysqlCmdExecutor.executeStmt",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End(trace.WithStatementExtra(ses.GetTxnId(), ses.GetStmtId(), ses.GetSqlOfStmt()))

	ses.SetQueryInProgress(true)
	ses.SetQueryStart(time.Now())
	ses.SetQueryInExecute(true)
	defer ses.SetQueryEnd(time.Now())
	defer ses.SetQueryInProgress(false)

	// per statement profiler
	requestCtx, endStmtProfile := fileservice.NewStatementProfiler(requestCtx)
	if endStmtProfile != nil {
		defer endStmtProfile(func() string {
			// use sql string as file name suffix
			formatCtx := tree.NewFmtCtx(dialect.MYSQL)
			execCtx.stmt.Format(formatCtx)
			sql := formatCtx.String()
			if len(sql) > 128 {
				sql = sql[:128]
			}
			sql = strings.TrimSpace(sql)
			sql = strings.Map(func(r rune) rune {
				if unicode.IsSpace(r) {
					return '-'
				}
				return r
			}, sql)
			return sql
		})
	}

	// record goroutine info when ddl stmt run timeout
	switch execCtx.stmt.(type) {
	case *tree.CreateTable, *tree.DropTable, *tree.CreateDatabase, *tree.DropDatabase:
		_, span := trace.Start(requestCtx, "executeStmtHung",
			trace.WithHungThreshold(time.Minute), // be careful with this options
			trace.WithProfileGoroutine(),
			trace.WithProfileTraceSecs(10*time.Second),
		)
		defer span.End()
	default:
	}

	// end of preamble.

	// deferred functions.

	var cmpBegin time.Time
	var ret interface{}
	var runner ComputationRunner
	var selfHandle bool
	var columns []interface{}
	var mrs *MysqlResultSet
	var loadLocalErrGroup *errgroup.Group

	// XXX XXX
	// I hope I can break the following code into several functions, but I can't.
	// After separating the functions, the system cannot boot, due to mo_account
	// not exists.  No clue why, the closure/capture must do some magic.

	//check transaction states
	switch execCtx.stmt.(type) {
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
	}

	switch st := execCtx.stmt.(type) {
	case *tree.Select:
		if st.Ep != nil {
			if gPu.SV.DisableSelectInto {
				err = moerr.NewSyntaxError(requestCtx, "Unsupport select statement")
				return
			}
			ses.InitExportConfig(st.Ep)
			defer func() {
				ses.ClearExportParam()
			}()
			err = doCheckFilePath(requestCtx, ses, st.Ep)
			if err != nil {
				return
			}
		}
	}

	selfHandle = false

	switch st := execCtx.stmt.(type) {
	case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
		selfHandle = true
	case *tree.SetRole:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		//switch role
		err = handleSwitchRole(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.Use:
		selfHandle = true
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
		selfHandle = true
		//dump
		err = handleDump(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.CreateDatabase:
		err = inputNameIsInvalid(execCtx.proc.Ctx, string(st.Name))
		if err != nil {
			return
		}
		if st.SubscriptionOption != nil && ses.GetTenantInfo() != nil && !ses.GetTenantInfo().IsAdminRole() {
			err = moerr.NewInternalError(execCtx.proc.Ctx, "only admin can create subscription")
			return
		}
		st.Sql = execCtx.sqlOfStmt
	case *tree.DropDatabase:
		err = inputNameIsInvalid(execCtx.proc.Ctx, string(st.Name))
		if err != nil {
			return
		}
		ses.InvalidatePrivilegeCache()
		// if the droped database is the same as the one in use, database must be reseted to empty.
		if string(st.Name) == ses.GetDatabaseName() {
			ses.SetDatabaseName("")
		}
	case *tree.PrepareStmt:
		selfHandle = true
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
		selfHandle = true
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
		selfHandle = true
		err = handleCreateConnector(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.PauseDaemonTask:
		selfHandle = true
		err = handlePauseDaemonTask(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.CancelDaemonTask:
		selfHandle = true
		err = handleCancelDaemonTask(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.ResumeDaemonTask:
		selfHandle = true
		err = handleResumeDaemonTask(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.DropConnector:
		selfHandle = true
		err = handleDropConnector(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.ShowConnectors:
		selfHandle = true
		if err = handleShowConnectors(requestCtx, ses, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.Deallocate:
		selfHandle = true
		err = handleDeallocate(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.Reset:
		selfHandle = true
		err = handleReset(requestCtx, ses, st)
		if err != nil {
			return
		}
	case *tree.SetVar:
		selfHandle = true
		err = handleSetVar(requestCtx, ses, st, execCtx.sqlOfStmt)
		if err != nil {
			return
		}
	case *tree.ShowVariables:
		selfHandle = true
		err = handleShowVariables(ses, st, execCtx.proc, execCtx.isLastStmt)
		if err != nil {
			return
		}
	case *tree.ShowErrors, *tree.ShowWarnings:
		selfHandle = true
		err = handleShowErrors(ses, execCtx.isLastStmt)
		if err != nil {
			return
		}
	case *tree.AnalyzeStmt:
		selfHandle = true
		if err = handleAnalyzeStmt(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.ExplainStmt:
		selfHandle = true
		if err = handleExplainStmt(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.ExplainAnalyze:
		ses.SetData(nil)
	case *tree.ShowTableStatus:
		ses.SetShowStmtType(ShowTableStatus)
		ses.SetData(nil)
	case *InternalCmdFieldList:
		selfHandle = true
		if err = handleCmdFieldList(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CreatePublication:
		selfHandle = true
		if err = handleCreatePublication(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.AlterPublication:
		selfHandle = true
		if err = handleAlterPublication(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropPublication:
		selfHandle = true
		if err = handleDropPublication(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.ShowSubscriptions:
		selfHandle = true
		if err = handleShowSubscriptions(requestCtx, ses, st, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.CreateStage:
		selfHandle = true
		if err = handleCreateStage(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropStage:
		selfHandle = true
		if err = handleDropStage(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.AlterStage:
		selfHandle = true
		if err = handleAlterStage(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CreateAccount:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = handleCreateAccount(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropAccount:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = handleDropAccount(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.AlterAccount:
		ses.InvalidatePrivilegeCache()
		selfHandle = true
		if err = handleAlterAccount(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.AlterDataBaseConfig:
		ses.InvalidatePrivilegeCache()
		selfHandle = true
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
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = handleCreateUser(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropUser:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = handleDropUser(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.AlterUser: //TODO
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = handleAlterUser(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CreateRole:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = handleCreateRole(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropRole:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = handleDropRole(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CreateFunction:
		selfHandle = true
		if err = st.Valid(); err != nil {
			return err
		}
		if err = handleCreateFunction(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropFunction:
		selfHandle = true
		if err = handleDropFunction(requestCtx, ses, st, execCtx.proc); err != nil {
			return
		}
	case *tree.CreateProcedure:
		selfHandle = true
		if err = handleCreateProcedure(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.DropProcedure:
		selfHandle = true
		if err = handleDropProcedure(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.CallStmt:
		selfHandle = true
		if err = handleCallProcedure(requestCtx, ses, st, execCtx.proc); err != nil {
			return
		}
	case *tree.Grant:
		selfHandle = true
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
		selfHandle = true
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
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = handleKill(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.ShowAccounts:
		selfHandle = true
		if err = handleShowAccounts(requestCtx, ses, st, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.ShowCollation:
		selfHandle = true
		if err = handleShowCollation(ses, st, execCtx.proc, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.Load:
		if st.Local {
			execCtx.proc.LoadLocalReader, execCtx.loadLocalWriter = io.Pipe()
		}
	case *tree.ShowBackendServers:
		selfHandle = true
		if err = handleShowBackendServers(requestCtx, ses, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.SetTransaction:
		selfHandle = true
		//TODO: handle set transaction
	case *tree.LockTableStmt:
		selfHandle = true
	case *tree.UnLockTableStmt:
		selfHandle = true
	case *tree.ShowGrants:
		if len(st.Username) == 0 {
			st.Username = execCtx.userName
		}
		if len(st.Hostname) == 0 || st.Hostname == "%" {
			st.Hostname = rootHost
		}
	case *tree.BackupStart:
		selfHandle = true
		if err = handleStartBackup(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.EmptyStmt:
		selfHandle = true
		if err = handleEmptyStmt(requestCtx, ses, st); err != nil {
			return
		}
	}

	if selfHandle {
		return
	}

	cmpBegin = time.Now()

	if ret, err = execCtx.cw.Compile(requestCtx, ses, ses.GetOutputCallback()); err != nil {
		return
	}
	execCtx.stmt = execCtx.cw.GetAst()
	// reset some special stmt for execute statement
	switch st := execCtx.stmt.(type) {
	case *tree.SetVar:
		err = handleSetVar(requestCtx, ses, st, execCtx.sqlOfStmt)
		if err != nil {
			return
		} else {
			return
		}
	case *tree.ShowVariables:
		err = handleShowVariables(ses, st, execCtx.proc, execCtx.isLastStmt)
		if err != nil {
			return
		} else {
			return
		}
	case *tree.ShowErrors, *tree.ShowWarnings:
		err = handleShowErrors(ses, execCtx.isLastStmt)
		if err != nil {
			return
		} else {
			return
		}
	}

	runner = ret.(ComputationRunner)

	// only log if build time is longer than 1s
	if time.Since(cmpBegin) > time.Second {
		logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Build : %s", time.Since(cmpBegin).String()))
	}

	mrs = ses.GetMysqlResultSet()
	ep := ses.GetExportConfig()
	// cw.Compile might rewrite sql, here we fetch the latest version
	switch statement := execCtx.stmt.(type) {
	//produce result set
	case *tree.Select:
		if ep.needExportToFile() {

			columns, err = execCtx.cw.GetColumns()
			if err != nil {
				logError(ses, ses.GetDebugString(),
					"Failed to get columns from computation handler",
					zap.Error(err))
				return
			}
			for _, c := range columns {
				mysqlc := c.(Column)
				mrs.AddColumn(mysqlc)
			}

			// open new file
			ep.DefaultBufSize = gPu.SV.ExportDataDefaultFlushSize
			initExportFileParam(ep, mrs)
			if err = openNewFile(requestCtx, ep, mrs); err != nil {
				return
			}

			runBegin := time.Now()
			/*
				Start pipeline
				Producing the data row and sending the data row
			*/
			// todo: add trace
			if _, err = runner.Run(0); err != nil {
				return
			}

			// only log if run time is longer than 1s
			if time.Since(runBegin) > time.Second {
				logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Run : %s", time.Since(runBegin).String()))
			}

			oq := NewOutputQueue(ses.GetRequestContext(), ses, 0, nil, nil)
			if err = exportAllData(oq); err != nil {
				return
			}
			if err = ep.Writer.Flush(); err != nil {
				return
			}
			if err = ep.File.Close(); err != nil {
				return
			}

			/*
			   Serialize the execution plan by json
			*/
			if cwft, ok := execCtx.cw.(*TxnComputationWrapper); ok {
				_ = cwft.RecordExecPlan(requestCtx)
			}

		} else {
			columns, err = execCtx.cw.GetColumns()
			if err != nil {
				logError(ses, ses.GetDebugString(),
					"Failed to get columns from computation handler",
					zap.Error(err))
				return
			}
			if c, ok := execCtx.cw.(*TxnComputationWrapper); ok {
				ses.rs = &plan.ResultColDef{ResultCols: plan2.GetResultColumnsFromPlan(c.plan)}
			}
			/*
				Step 1 : send column count and column definition.
			*/
			//send column count
			colCnt := uint64(len(columns))
			err = execCtx.proto.SendColumnCountPacket(colCnt)
			if err != nil {
				return
			}
			//send columns
			//column_count * Protocol::ColumnDefinition packets
			cmd := ses.GetCmd()
			for _, c := range columns {
				mysqlc := c.(Column)
				mrs.AddColumn(mysqlc)
				/*
					mysql COM_QUERY response: send the column definition per column
				*/
				err = execCtx.proto.SendColumnDefinitionPacket(requestCtx, mysqlc, int(cmd))
				if err != nil {
					return
				}
			}

			/*
				mysql COM_QUERY response: End after the column has been sent.
				send EOF packet
			*/
			err = execCtx.proto.SendEOFPacketIf(0, ses.GetServerStatus())
			if err != nil {
				return
			}

			runBegin := time.Now()
			/*
				Step 2: Start pipeline
				Producing the data row and sending the data row
			*/
			// todo: add trace
			if _, err = runner.Run(0); err != nil {
				return
			}

			// only log if run time is longer than 1s
			if time.Since(runBegin) > time.Second {
				logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Run : %s", time.Since(runBegin).String()))
			}

			/*
				Step 3: Say goodbye
				mysql COM_QUERY response: End after the data row has been sent.
				After all row data has been sent, it sends the EOF or OK packet.
			*/
			err = execCtx.proto.sendEOFOrOkPacket(0, ses.GetServerStatus())
			if err != nil {
				return
			}

			/*
				Step 4: Serialize the execution plan by json
			*/
			if cwft, ok := execCtx.cw.(*TxnComputationWrapper); ok {
				_ = cwft.RecordExecPlan(requestCtx)
			}
		}

	case *tree.ShowCreateTable, *tree.ShowCreateDatabase, *tree.ShowTables, *tree.ShowSequences, *tree.ShowDatabases, *tree.ShowColumns,
		*tree.ShowProcessList, *tree.ShowStatus, *tree.ShowTableStatus, *tree.ShowGrants, *tree.ShowRolesStmt,
		*tree.ShowIndex, *tree.ShowCreateView, *tree.ShowTarget, *tree.ShowCollation, *tree.ValuesStatement,
		*tree.ExplainFor, *tree.ExplainStmt, *tree.ShowTableNumber, *tree.ShowColumnNumber, *tree.ShowTableValues, *tree.ShowLocks, *tree.ShowNodeList, *tree.ShowFunctionOrProcedureStatus,
		*tree.ShowPublications, *tree.ShowCreatePublications, *tree.ShowStages:
		columns, err = execCtx.cw.GetColumns()
		if err != nil {
			logError(ses, ses.GetDebugString(),
				"Failed to get columns from computation handler",
				zap.Error(err))
			return
		}
		if c, ok := execCtx.cw.(*TxnComputationWrapper); ok {
			ses.rs = &plan.ResultColDef{ResultCols: plan2.GetResultColumnsFromPlan(c.plan)}
		}
		/*
			Step 1 : send column count and column definition.
		*/
		//send column count
		colCnt := uint64(len(columns))
		err = execCtx.proto.SendColumnCountPacket(colCnt)
		if err != nil {
			return
		}
		//send columns
		//column_count * Protocol::ColumnDefinition packets
		cmd := ses.GetCmd()
		for _, c := range columns {
			mysqlc := c.(Column)
			mrs.AddColumn(mysqlc)
			/*
				mysql COM_QUERY response: send the column definition per column
			*/
			err = execCtx.proto.SendColumnDefinitionPacket(requestCtx, mysqlc, int(cmd))
			if err != nil {
				return
			}
		}

		/*
			mysql COM_QUERY response: End after the column has been sent.
			send EOF packet
		*/
		err = execCtx.proto.SendEOFPacketIf(0, ses.GetServerStatus())
		if err != nil {
			return
		}

		runBegin := time.Now()
		/*
			Step 2: Start pipeline
			Producing the data row and sending the data row
		*/
		// todo: add trace
		if _, err = runner.Run(0); err != nil {
			return
		}

		switch ses.GetShowStmtType() {
		case ShowTableStatus:
			if err = handleShowTableStatus(ses, statement.(*tree.ShowTableStatus), execCtx.proc); err != nil {
				return
			}
		}

		// only log if run time is longer than 1s
		if time.Since(runBegin) > time.Second {
			logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Run : %s", time.Since(runBegin).String()))
		}

		/*
			Step 3: Say goodbye
			mysql COM_QUERY response: End after the data row has been sent.
			After all row data has been sent, it sends the EOF or OK packet.
		*/
		err = execCtx.proto.sendEOFOrOkPacket(0, ses.GetServerStatus())
		if err != nil {
			return
		}

		/*
			Step 4: Serialize the execution plan by json
		*/
		if cwft, ok := execCtx.cw.(*TxnComputationWrapper); ok {
			_ = cwft.RecordExecPlan(requestCtx)
		}
	//just status, no result set
	case *tree.CreateTable, *tree.DropTable, *tree.CreateDatabase, *tree.DropDatabase,
		*tree.CreateIndex, *tree.DropIndex,
		*tree.CreateView, *tree.DropView, *tree.AlterView, *tree.AlterTable, *tree.AlterSequence,
		*tree.CreateSequence, *tree.DropSequence,
		*tree.Insert, *tree.Update, *tree.Replace,
		*tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction,
		*tree.SetVar,
		*tree.Load,
		*tree.CreateUser, *tree.DropUser, *tree.AlterUser,
		*tree.CreateRole, *tree.DropRole,
		*tree.Revoke, *tree.Grant,
		*tree.SetDefaultRole, *tree.SetRole, *tree.SetPassword, *tree.CreateStream,
		*tree.Delete, *tree.TruncateTable, *tree.LockTableStmt, *tree.UnLockTableStmt:
		//change privilege
		switch execCtx.stmt.(type) {
		case *tree.DropTable, *tree.DropDatabase, *tree.DropIndex, *tree.DropView, *tree.DropSequence,
			*tree.CreateUser, *tree.DropUser, *tree.AlterUser,
			*tree.CreateRole, *tree.DropRole,
			*tree.Revoke, *tree.Grant,
			*tree.SetDefaultRole, *tree.SetRole:
			ses.InvalidatePrivilegeCache()
		}
		runBegin := time.Now()
		/*
			Step 1: Start
		*/

		if st, ok := execCtx.stmt.(*tree.Load); ok {
			if st.Local {
				loadLocalErrGroup = new(errgroup.Group)
				loadLocalErrGroup.Go(func() error {
					return processLoadLocal(execCtx.proc.Ctx, ses, st.Param, execCtx.loadLocalWriter)
				})
			}
		}

		if execCtx.runResult, err = runner.Run(0); err != nil {
			if loadLocalErrGroup != nil { // release resources
				err2 := execCtx.proc.LoadLocalReader.Close()
				if err2 != nil {
					logError(ses, ses.GetDebugString(),
						"processLoadLocal goroutine failed",
						zap.Error(err2))
				}
				err2 = loadLocalErrGroup.Wait() // executor failed, but processLoadLocal is still running, wait for it
				if err2 != nil {
					logError(ses, ses.GetDebugString(),
						"processLoadLocal goroutine failed",
						zap.Error(err2))
				}
			}
			return
		}

		if loadLocalErrGroup != nil {
			if err = loadLocalErrGroup.Wait(); err != nil { //executor success, but processLoadLocal goroutine failed
				return
			}
		}

		// only log if run time is longer than 1s
		if time.Since(runBegin) > time.Second {
			logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Run : %s", time.Since(runBegin).String()))
		}

		echoTime := time.Now()

		logDebug(ses, ses.GetDebugString(), fmt.Sprintf("time of SendResponse %s", time.Since(echoTime).String()))

		/*
			Step 4: Serialize the execution plan by json
		*/
		if cwft, ok := execCtx.cw.(*TxnComputationWrapper); ok {
			_ = cwft.RecordExecPlan(requestCtx)
		}
	case *tree.ExplainAnalyze:
		explainColName := "QUERY PLAN"
		columns, err = GetExplainColumns(requestCtx, explainColName)
		if err != nil {
			logError(ses, ses.GetDebugString(),
				"Failed to get columns from ExplainColumns handler",
				zap.Error(err))
			return
		}
		/*
			Step 1 : send column count and column definition.
		*/
		//send column count
		colCnt := uint64(len(columns))
		err = execCtx.proto.SendColumnCountPacket(colCnt)
		if err != nil {
			return
		}
		//send columns
		//column_count * Protocol::ColumnDefinition packets
		cmd := ses.GetCmd()
		for _, c := range columns {
			mysqlc := c.(Column)
			mrs.AddColumn(mysqlc)
			/*
				mysql COM_QUERY response: send the column definition per column
			*/
			err = execCtx.proto.SendColumnDefinitionPacket(requestCtx, mysqlc, int(cmd))
			if err != nil {
				return
			}
		}
		/*
			mysql COM_QUERY response: End after the column has been sent.
			send EOF packet
		*/
		err = execCtx.proto.SendEOFPacketIf(0, ses.GetServerStatus())
		if err != nil {
			return
		}

		runBegin := time.Now()
		/*
			Step 1: Start
		*/
		if _, err = runner.Run(0); err != nil {
			return
		}

		// only log if run time is longer than 1s
		if time.Since(runBegin) > time.Second {
			logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Run : %s", time.Since(runBegin).String()))
		}

		if cwft, ok := execCtx.cw.(*TxnComputationWrapper); ok {
			queryPlan := cwft.plan
			// generator query explain
			explainQuery := explain.NewExplainQueryImpl(queryPlan.GetQuery())

			// build explain data buffer
			buffer := explain.NewExplainDataBuffer()
			var option *explain.ExplainOptions
			option, err = getExplainOption(requestCtx, statement.Options)
			if err != nil {
				return
			}

			err = explainQuery.ExplainPlan(requestCtx, buffer, option)
			if err != nil {
				return
			}

			err = buildMoExplainQuery(explainColName, buffer, ses, getDataFromPipeline)
			if err != nil {
				return
			}

			/*
				Step 3: Say goodbye
				mysql COM_QUERY response: End after the data row has been sent.
				After all row data has been sent, it sends the EOF or OK packet.
			*/
			err = execCtx.proto.sendEOFOrOkPacket(0, ses.GetServerStatus())
			if err != nil {
				return
			}
		}
	}
	return
}

type ExecCtx struct {
	prepareStmt *PrepareStmt
	runResult   *util2.RunResult
	//stmt will be replaced by the Execute
	stmt tree.Statement
	//isLastStmt : true denotes the last statement in the query
	isLastStmt bool
	// tenant name
	tenant          string
	userName        string
	sqlOfStmt       string
	cw              ComputationWrapper
	runner          ComputationRunner
	loadLocalWriter *io.PipeWriter
	proc            *process.Process
	proto           MysqlProtocol
}

/*
GetComputationWrapper gets the execs from the computation engine
*/
var GetComputationWrapper = func(db string, input *UserInput, user string, eng engine.Engine, proc *process.Process, ses *Session) ([]ComputationWrapper, error) {
	var cw []ComputationWrapper = nil
	if cached := ses.getCachedPlan(input.getSql()); cached != nil {
		modify := false
		for i, stmt := range cached.stmts {
			tcw := InitTxnComputationWrapper(ses, stmt, proc)
			tcw.plan = cached.plans[i]
			if tcw.plan == nil {
				modify = true
				break
			}
			if checkModify(tcw.plan, proc, ses) {
				modify = true
				break
			}
			cw = append(cw, tcw)
		}
		if modify {
			cw = nil
		} else {
			return cw, nil
		}
	}

	var stmts []tree.Statement = nil
	var cmdFieldStmt *InternalCmdFieldList
	var err error
	// if the input is an option ast, we should use it directly
	if input.getStmt() != nil {
		stmts = append(stmts, input.getStmt())
	} else if isCmdFieldListSql(input.getSql()) {
		cmdFieldStmt, err = parseCmdFieldList(proc.Ctx, input.getSql())
		if err != nil {
			return nil, err
		}
		stmts = append(stmts, cmdFieldStmt)
	} else {
		var v interface{}
		v, err = ses.GetGlobalVar("lower_case_table_names")
		if err != nil {
			v = int64(1)
		}
		stmts, err = parsers.Parse(proc.Ctx, dialect.MYSQL, input.getSql(), v.(int64))
		if err != nil {
			return nil, err
		}
	}

	for _, stmt := range stmts {
		cw = append(cw, InitTxnComputationWrapper(ses, stmt, proc))
	}
	return cw, nil
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

func buildPlan(requestCtx context.Context, ses TempInter, ctx plan2.CompilerContext, stmt tree.Statement) (*plan2.Plan, error) {
	start := time.Now()
	defer func() {
		v2.TxnStatementBuildPlanDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	stats := statistic.StatsInfoFromContext(requestCtx)
	stats.PlanStart()
	defer stats.PlanEnd()

	var ret *plan2.Plan
	var err error
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
	checkFn := func(db string, def *plan.TableDef) bool {
		_, tableDef := ses.GetTxnCompileCtx().Resolve(db, def.Name)
		if tableDef == nil {
			return true
		}
		if tableDef.Version != def.Version || tableDef.TblId != def.TblId {
			return true
		}
		return false
	}
	switch p := plan2.Plan.(type) {
	case *plan.Plan_Query:
		for i := range p.Query.Nodes {
			if def := p.Query.Nodes[i].TableDef; def != nil {
				if p.Query.Nodes[i].ObjRef == nil || checkFn(p.Query.Nodes[i].ObjRef.SchemaName, def) {
					return true
				}
			}
			if ctx := p.Query.Nodes[i].InsertCtx; ctx != nil {
				if ctx.Ref == nil || checkFn(ctx.Ref.SchemaName, ctx.TableDef) {
					return true
				}
			}
			if ctx := p.Query.Nodes[i].ReplaceCtx; ctx != nil {
				if ctx.Ref == nil || checkFn(ctx.Ref.SchemaName, ctx.TableDef) {
					return true
				}
			}
			if ctx := p.Query.Nodes[i].DeleteCtx; ctx != nil {
				if ctx.Ref == nil || checkFn(ctx.Ref.SchemaName, ctx.TableDef) {
					return true
				}
			}
			if ctx := p.Query.Nodes[i].PreInsertCtx; ctx != nil {
				if ctx.Ref == nil || checkFn(ctx.Ref.SchemaName, ctx.TableDef) {
					return true
				}
			}
			if ctx := p.Query.Nodes[i].PreInsertCtx; ctx != nil {
				if ctx.Ref == nil || checkFn(ctx.Ref.SchemaName, ctx.TableDef) {
					return true
				}
			}
			if ctx := p.Query.Nodes[i].OnDuplicateKey; ctx != nil {
				if p.Query.Nodes[i].ObjRef == nil || checkFn(p.Query.Nodes[i].ObjRef.SchemaName, ctx.TableDef) {
					return true
				}
			}
		}
	default:
	}
	return false
}

func parsePrepareStmtID(s string) uint32 {
	if strings.HasPrefix(s, prefixPrepareStmtName) {
		ss := strings.Split(s, "_")
		v, err := strconv.ParseUint(ss[len(ss)-1], 10, 64)
		if err != nil {
			return 0
		}
		return uint32(v)
	}
	return 0
}

func GetPrepareStmtID(ctx context.Context, name string) (int, error) {
	idx := len(prefixPrepareStmtName) + 1
	if idx >= len(name) {
		return -1, moerr.NewInternalError(ctx, "can not get Prepare stmtID")
	}
	return strconv.Atoi(name[idx:])
}

func getPrepareStmtName(stmtID uint32) string {
	return fmt.Sprintf("%s_%d", prefixPrepareStmtName, stmtID)
}

func parseStmtExecute(requestCtx context.Context, ses *Session, data []byte) (string, *PrepareStmt, error) {
	// see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
	pos := 0
	if len(data) < 4 {
		return "", nil, moerr.NewInvalidInput(requestCtx, "sql command contains malformed packet")
	}
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmtName := fmt.Sprintf("%s_%d", prefixPrepareStmtName, stmtID)
	preStmt, err := ses.GetPrepareStmt(stmtName)
	if err != nil {
		return "", nil, err
	}

	sql := fmt.Sprintf("execute %s", stmtName)
	logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))
	err = ses.GetMysqlProtocol().ParseExecuteData(requestCtx, ses.GetTxnCompileCtx().GetProcess(), preStmt, data, pos)
	if err != nil {
		return "", nil, err
	}
	return sql, preStmt, nil
}

func parseStmtSendLongData(requestCtx context.Context, ses *Session, data []byte) error {
	// see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_send_long_data.html
	pos := 0
	if len(data) < 4 {
		return moerr.NewInvalidInput(requestCtx, "sql command contains malformed packet")
	}
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmtName := fmt.Sprintf("%s_%d", prefixPrepareStmtName, stmtID)
	preStmt, err := ses.GetPrepareStmt(stmtName)
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("send long data for stmt %s", stmtName)
	logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))

	err = ses.GetMysqlProtocol().ParseSendLongData(requestCtx, ses.GetTxnCompileCtx().GetProcess(), preStmt, data, pos)
	if err != nil {
		return err
	}
	return nil
}

func handleSetOption(ctx context.Context, ses *Session, data []byte) (err error) {
	if len(data) < 2 {
		return moerr.NewInternalError(ctx, "invalid cmd_set_option data length")
	}
	cap := ses.GetMysqlProtocol().GetCapability()
	switch binary.LittleEndian.Uint16(data[:2]) {
	case 0:
		// MO do not support CLIENT_MULTI_STATEMENTS in prepare, so do nothing here(Like MySQL)
		// cap |= CLIENT_MULTI_STATEMENTS
		// GetSession().GetMysqlProtocol().SetCapability(cap)

	case 1:
		cap &^= CLIENT_MULTI_STATEMENTS
		ses.GetMysqlProtocol().SetCapability(cap)

	default:
		return moerr.NewInternalError(ctx, "invalid cmd_set_option data")
	}

	return nil
}

/*
convert the type in computation engine to the type in mysql.
*/
func convertEngineTypeToMysqlType(ctx context.Context, engineType types.T, col *MysqlColumn) error {
	switch engineType {
	case types.T_any:
		col.SetColumnType(defines.MYSQL_TYPE_NULL)
	case types.T_json:
		col.SetColumnType(defines.MYSQL_TYPE_JSON)
	case types.T_bool:
		col.SetColumnType(defines.MYSQL_TYPE_BOOL)
	case types.T_int8:
		col.SetColumnType(defines.MYSQL_TYPE_TINY)
	case types.T_uint8:
		col.SetColumnType(defines.MYSQL_TYPE_TINY)
		col.SetSigned(false)
	case types.T_int16:
		col.SetColumnType(defines.MYSQL_TYPE_SHORT)
	case types.T_uint16:
		col.SetColumnType(defines.MYSQL_TYPE_SHORT)
		col.SetSigned(false)
	case types.T_int32:
		col.SetColumnType(defines.MYSQL_TYPE_LONG)
	case types.T_uint32:
		col.SetColumnType(defines.MYSQL_TYPE_LONG)
		col.SetSigned(false)
	case types.T_int64:
		col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	case types.T_uint64:
		col.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
		col.SetSigned(false)
	case types.T_float32:
		col.SetColumnType(defines.MYSQL_TYPE_FLOAT)
	case types.T_float64:
		col.SetColumnType(defines.MYSQL_TYPE_DOUBLE)
	case types.T_char:
		col.SetColumnType(defines.MYSQL_TYPE_STRING)
	case types.T_varchar:
		col.SetColumnType(defines.MYSQL_TYPE_VAR_STRING)
	case types.T_array_float32, types.T_array_float64:
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	case types.T_binary:
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	case types.T_varbinary:
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	case types.T_date:
		col.SetColumnType(defines.MYSQL_TYPE_DATE)
	case types.T_datetime:
		col.SetColumnType(defines.MYSQL_TYPE_DATETIME)
	case types.T_time:
		col.SetColumnType(defines.MYSQL_TYPE_TIME)
	case types.T_timestamp:
		col.SetColumnType(defines.MYSQL_TYPE_TIMESTAMP)
	case types.T_decimal64:
		col.SetColumnType(defines.MYSQL_TYPE_DECIMAL)
	case types.T_decimal128:
		col.SetColumnType(defines.MYSQL_TYPE_DECIMAL)
	case types.T_blob:
		col.SetColumnType(defines.MYSQL_TYPE_BLOB)
	case types.T_text:
		col.SetColumnType(defines.MYSQL_TYPE_TEXT)
	case types.T_uuid:
		col.SetColumnType(defines.MYSQL_TYPE_UUID)
	case types.T_TS:
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	case types.T_Blockid:
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	case types.T_enum:
		col.SetColumnType(defines.MYSQL_TYPE_VARCHAR)
	default:
		return moerr.NewInternalError(ctx, "RunWhileSend : unsupported type %d", engineType)
	}
	return nil
}

func convertMysqlTextTypeToBlobType(col *MysqlColumn) {
	if col.ColumnType() == defines.MYSQL_TYPE_TEXT {
		col.SetColumnType(defines.MYSQL_TYPE_BLOB)
	}
}

/*
extract the data from the pipeline.
obj: session
Warning: The pipeline is the multi-thread environment. The getDataFromPipeline will
access the shared data. Be careful when it writes the shared data.
*/
func getDataFromPipeline(obj interface{}, bat *batch.Batch) error {
	_, task := gotrace.NewTask(context.TODO(), "frontend.WriteDataToClient")
	defer task.End()
	ses := obj.(*Session)
	if openSaveQueryResult(ses) {
		if bat == nil {
			if err := saveQueryResultMeta(ses); err != nil {
				return err
			}
		} else {
			if err := saveQueryResult(ses, bat); err != nil {
				return err
			}
		}
	}
	if bat == nil {
		return nil
	}

	begin := time.Now()
	proto := ses.GetMysqlProtocol()

	ec := ses.GetExportConfig()
	oq := NewOutputQueue(ses.GetRequestContext(), ses, len(bat.Vecs), nil, nil)
	row2colTime := time.Duration(0)
	procBatchBegin := time.Now()
	n := bat.Vecs[0].Length()
	requestCtx := ses.GetRequestContext()

	if ec.needExportToFile() {
		initExportFirst(oq)
	}

	for j := 0; j < n; j++ { //row index
		if ec.needExportToFile() {
			select {
			case <-requestCtx.Done():
				return nil
			default:
			}
			continue
		}

		row, err := extractRowFromEveryVector(ses, bat, j, oq, false)
		if err != nil {
			return err
		}
		if oq.showStmtType == ShowTableStatus {
			row2 := make([]interface{}, len(row))
			copy(row2, row)
			ses.AppendData(row2)
		}
	}

	if ec.needExportToFile() {
		oq.rowIdx = uint64(n)
		bat2 := preCopyBat(obj, bat)
		go constructByte(obj, bat2, oq.ep.Index, oq.ep.ByteChan, oq)
	}
	err := oq.flush()
	if err != nil {
		return err
	}

	procBatchTime := time.Since(procBatchBegin)
	tTime := time.Since(begin)
	ses.sentRows.Add(int64(n))
	logDebugf(ses.GetDebugString(), "rowCount %v \n"+
		"time of getDataFromPipeline : %s \n"+
		"processBatchTime %v \n"+
		"row2colTime %v \n"+
		"restTime(=totalTime - row2colTime) %v \n"+
		"protoStats %s",
		n,
		tTime,
		procBatchTime,
		row2colTime,
		tTime-row2colTime,
		proto.GetStats())

	return nil
}

func setResponse(ses *Session, isLastStmt bool, rspLen uint64) *Response {
	return ses.SetNewResponse(OkResponse, rspLen, int(COM_QUERY), "", isLastStmt)
}

// response the client
func respClientFunc(requestCtx context.Context,
	ses *Session,
	execCtx *ExecCtx) (err error) {
	var rspLen uint64
	if execCtx.runResult != nil {
		rspLen = execCtx.runResult.AffectRows
	}

	switch execCtx.stmt.(type) {
	case *tree.Select:
		if len(execCtx.proc.SessionInfo.SeqAddValues) != 0 {
			ses.AddSeqValues(execCtx.proc)
		}
		ses.SetSeqLastValue(execCtx.proc)
	case *tree.CreateTable, *tree.DropTable, *tree.CreateDatabase, *tree.DropDatabase,
		*tree.CreateIndex, *tree.DropIndex, *tree.Insert, *tree.Update, *tree.Replace,
		*tree.CreateView, *tree.DropView, *tree.AlterView, *tree.AlterTable, *tree.Load, *tree.MoDump,
		*tree.CreateSequence, *tree.DropSequence,
		*tree.CreateAccount, *tree.DropAccount, *tree.AlterAccount, *tree.AlterDataBaseConfig, *tree.CreatePublication, *tree.AlterPublication, *tree.DropPublication,
		*tree.CreateFunction, *tree.DropFunction,
		*tree.CreateProcedure, *tree.DropProcedure,
		*tree.CreateUser, *tree.DropUser, *tree.AlterUser,
		*tree.CreateRole, *tree.DropRole, *tree.Revoke, *tree.Grant,
		*tree.SetDefaultRole, *tree.SetRole, *tree.SetPassword, *tree.Delete, *tree.TruncateTable, *tree.Use,
		*tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction,
		*tree.LockTableStmt, *tree.UnLockTableStmt,
		*tree.CreateStage, *tree.DropStage, *tree.AlterStage, *tree.CreateStream, *tree.AlterSequence:
		resp := setResponse(ses, execCtx.isLastStmt, rspLen)
		if _, ok := execCtx.stmt.(*tree.Insert); ok {
			resp.lastInsertId = execCtx.proc.GetLastInsertID()
			if execCtx.proc.GetLastInsertID() != 0 {
				ses.SetLastInsertID(execCtx.proc.GetLastInsertID())
			}
		}
		if len(execCtx.proc.SessionInfo.SeqDeleteKeys) != 0 {
			ses.DeleteSeqValues(execCtx.proc)
		}

		if st, ok := execCtx.stmt.(*tree.CreateTable); ok {
			_ = doGrantPrivilegeImplicitly(requestCtx, ses, st)
		}

		if st, ok := execCtx.stmt.(*tree.DropTable); ok {
			_ = doRevokePrivilegeImplicitly(requestCtx, ses, st)
		}

		if st, ok := execCtx.stmt.(*tree.CreateDatabase); ok {
			_ = insertRecordToMoMysqlCompatibilityMode(requestCtx, ses, execCtx.stmt)
			_ = doGrantPrivilegeImplicitly(requestCtx, ses, st)
		}

		if st, ok := execCtx.stmt.(*tree.DropDatabase); ok {
			_ = deleteRecordToMoMysqlCompatbilityMode(requestCtx, ses, execCtx.stmt)
			_ = doRevokePrivilegeImplicitly(requestCtx, ses, st)
		}

		if err2 := ses.GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
			err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
			logStatementStatus(requestCtx, ses, execCtx.stmt, fail, err)
			return err
		}

	case *tree.PrepareStmt, *tree.PrepareString:
		if ses.GetCmd() == COM_STMT_PREPARE {
			if err2 := ses.GetMysqlProtocol().SendPrepareResponse(requestCtx, execCtx.prepareStmt); err2 != nil {
				err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
				logStatementStatus(requestCtx, ses, execCtx.stmt, fail, err)
				return err
			}
		} else {
			resp := setResponse(ses, execCtx.isLastStmt, rspLen)
			if err2 := ses.GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
				err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
				logStatementStatus(requestCtx, ses, execCtx.stmt, fail, err)
				return err
			}
		}

	case *tree.SetVar, *tree.SetTransaction, *tree.BackupStart, *tree.CreateConnector, *tree.DropConnector,
		*tree.PauseDaemonTask, *tree.ResumeDaemonTask, *tree.CancelDaemonTask:
		resp := setResponse(ses, execCtx.isLastStmt, rspLen)
		if err2 := ses.GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
			return moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
		}
	case *tree.Deallocate:
		//we will not send response in COM_STMT_CLOSE command
		if ses.GetCmd() != COM_STMT_CLOSE {
			resp := setResponse(ses, execCtx.isLastStmt, rspLen)
			if err2 := ses.GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
				err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
				logStatementStatus(requestCtx, ses, execCtx.stmt, fail, err)
				return err
			}
		}

	case *tree.Reset:
		resp := setResponse(ses, execCtx.isLastStmt, rspLen)
		if err2 := ses.GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
			err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
			logStatementStatus(requestCtx, ses, execCtx.stmt, fail, err)
			return err
		}
	}
	if ses.GetQueryInExecute() {
		logStatementStatus(requestCtx, ses, execCtx.stmt, success, nil)
	} else {
		logStatementStatus(requestCtx, ses, execCtx.stmt, fail, moerr.NewInternalError(requestCtx, "query is killed"))
	}
	return err
}

// authenticateUserCanExecuteStatement checks the user can execute the statement
func authenticateUserCanExecuteStatement(requestCtx context.Context, ses *Session, stmt tree.Statement) error {
	requestCtx, span := trace.Debug(requestCtx, "authenticateUserCanExecuteStatement")
	defer span.End()
	if gPu.SV.SkipCheckPrivilege {
		return nil
	}

	if ses.skipAuthForSpecialUser() {
		return nil
	}
	var havePrivilege bool
	var err error
	if ses.GetTenantInfo() != nil {
		ses.SetPrivilege(determinePrivilegeSetOfStatement(stmt))

		// can or not execute in retricted status
		if ses.getRoutine() != nil && ses.getRoutine().isRestricted() && !ses.GetPrivilege().canExecInRestricted {
			return moerr.NewInternalError(requestCtx, "do not have privilege to execute the statement")
		}

		havePrivilege, err = authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase(requestCtx, ses, stmt)
		if err != nil {
			return err
		}

		if !havePrivilege {
			err = moerr.NewInternalError(requestCtx, "do not have privilege to execute the statement")
			return err
		}

		havePrivilege, err = authenticateUserCanExecuteStatementWithObjectTypeNone(requestCtx, ses, stmt)
		if err != nil {
			return err
		}

		if !havePrivilege {
			err = moerr.NewInternalError(requestCtx, "do not have privilege to execute the statement")
			return err
		}
	}
	return err
}

// authenticateCanExecuteStatementAndPlan checks the user can execute the statement and its plan
func authenticateCanExecuteStatementAndPlan(requestCtx context.Context, ses *Session, stmt tree.Statement, p *plan.Plan) error {
	_, task := gotrace.NewTask(context.TODO(), "frontend.authenticateCanExecuteStatementAndPlan")
	defer task.End()
	if gPu.SV.SkipCheckPrivilege {
		return nil
	}

	if ses.skipAuthForSpecialUser() {
		return nil
	}
	yes, err := authenticateUserCanExecuteStatementWithObjectTypeDatabaseAndTable(requestCtx, ses, stmt, p)
	if err != nil {
		return err
	}
	if !yes {
		return moerr.NewInternalError(requestCtx, "do not have privilege to execute the statement")
	}
	return nil
}

// authenticatePrivilegeOfPrepareAndExecute checks the user can execute the Prepare or Execute statement
func authenticateUserCanExecutePrepareOrExecute(requestCtx context.Context, ses *Session, stmt tree.Statement, p *plan.Plan) error {
	_, task := gotrace.NewTask(context.TODO(), "frontend.authenticateUserCanExecutePrepareOrExecute")
	defer task.End()
	if gPu.SV.SkipCheckPrivilege {
		return nil
	}
	err := authenticateUserCanExecuteStatement(requestCtx, ses, stmt)
	if err != nil {
		return err
	}
	err = authenticateCanExecuteStatementAndPlan(requestCtx, ses, stmt, p)
	if err != nil {
		return err
	}
	return err
}

// canExecuteStatementInUncommittedTxn checks the user can execute the statement in an uncommitted transaction
func canExecuteStatementInUncommittedTransaction(requestCtx context.Context, ses TempInter, stmt tree.Statement) error {
	can, err := statementCanBeExecutedInUncommittedTransaction(ses, stmt)
	if err != nil {
		return err
	}
	if !can {
		//is ddl statement
		if IsCreateDropDatabase(stmt) {
			return moerr.NewInternalError(requestCtx, createDropDatabaseErrorInfo())
		} else if IsDDL(stmt) {
			return moerr.NewInternalError(requestCtx, onlyCreateStatementErrorInfo())
		} else if IsAdministrativeStatement(stmt) {
			return moerr.NewInternalError(requestCtx, administrativeCommandIsUnsupportedInTxnErrorInfo())
		} else {
			return moerr.NewInternalError(requestCtx, unclassifiedStatementInUncommittedTxnErrorInfo())
		}
	}
	return nil
}

func transferSessionConnType2StatisticConnType(c ConnType) statistic.ConnType {
	switch c {
	case ConnTypeUnset:
		return statistic.ConnTypeUnknown
	case ConnTypeInternal:
		return statistic.ConnTypeInternal
	case ConnTypeExternal:
		return statistic.ConnTypeExternal
	default:
		panic("unknown connection type")
	}
}

var RecordStatement = func(ctx context.Context, ses *Session, proc *process.Process, cw ComputationWrapper, envBegin time.Time, envStmt, sqlType string, useEnv bool) (context.Context, error) {
	// set StatementID
	var stmID uuid.UUID
	var statement tree.Statement = nil
	var text string
	if cw != nil {
		copy(stmID[:], cw.GetUUID())
		statement = cw.GetAst()

		ses.ast = statement

		execSql := makeExecuteSql(ses, statement)
		if len(execSql) != 0 {
			bb := strings.Builder{}
			bb.WriteString(envStmt)
			bb.WriteString(" // ")
			bb.WriteString(execSql)
			text = SubStringFromBegin(bb.String(), int(gPu.SV.LengthOfQueryPrinted))
		} else {
			text = SubStringFromBegin(envStmt, int(gPu.SV.LengthOfQueryPrinted))
		}
	} else {
		stmID, _ = uuid.NewV7()
		text = SubStringFromBegin(envStmt, int(gPu.SV.LengthOfQueryPrinted))
	}
	ses.SetStmtId(stmID)
	ses.SetStmtType(getStatementType(statement).GetStatementType())
	ses.SetQueryType(getStatementType(statement).GetQueryType())
	ses.SetSqlSourceType(sqlType)
	ses.SetSqlOfStmt(text)

	//note: txn id here may be empty
	if sqlType != constant.InternalSql {
		ses.pushQueryId(types.Uuid(stmID).ToString())
	}

	if !motrace.GetTracerProvider().IsEnable() {
		return ctx, nil
	}
	tenant := ses.GetTenantInfo()
	if tenant == nil {
		tenant, _ = GetTenantInfo(ctx, "internal")
	}
	stm := motrace.NewStatementInfo()
	// set TransactionID
	var txn TxnOperator
	var err error
	if handler := ses.GetTxnHandler(); handler.IsValidTxnOperator() {
		_, txn, err = handler.GetTxnOperator()
		if err != nil {
			return nil, err
		}
		copy(stm.TransactionID[:], txn.Txn().ID)
	}
	// set SessionID
	copy(stm.SessionID[:], ses.GetUUID())
	requestAt := envBegin
	if !useEnv {
		requestAt = time.Now()
	}

	copy(stm.StatementID[:], stmID[:])
	// END> set StatementID
	stm.Account = tenant.GetTenant()
	stm.RoleId = proc.SessionInfo.RoleId
	stm.User = tenant.GetUser()
	stm.Host = ses.protocol.Peer()
	stm.Database = ses.GetDatabaseName()
	stm.Statement = text
	stm.StatementFingerprint = "" // fixme= (Reserved)
	stm.StatementTag = ""         // fixme= (Reserved)
	stm.SqlSourceType = sqlType
	stm.RequestAt = requestAt
	stm.StatementType = getStatementType(statement).GetStatementType()
	stm.QueryType = getStatementType(statement).GetQueryType()
	stm.ConnType = transferSessionConnType2StatisticConnType(ses.connType)
	if sqlType == constant.InternalSql && isCmdFieldListSql(envStmt) {
		// fix original issue #8165
		stm.User = ""
	}
	if sqlType != constant.InternalSql {
		ses.SetTStmt(stm)
	}
	if !stm.IsZeroTxnID() {
		stm.Report(ctx)
	}
	if stm.IsMoLogger() && stm.StatementType == "Load" && len(stm.Statement) > 128 {
		stm.Statement = envStmt[:40] + "..." + envStmt[len(envStmt)-45:]
	}

	return motrace.ContextWithStatement(ctx, stm), nil
}

var RecordParseErrorStatement = func(ctx context.Context, ses *Session, proc *process.Process, envBegin time.Time,
	envStmt []string, sqlTypes []string, err error) (context.Context, error) {
	retErr := moerr.NewParseError(ctx, err.Error())
	/*
		!!!NOTE: the sql may be empty string.
		So, the sqlTypes may be empty slice.
	*/
	sqlType := ""
	if len(sqlTypes) > 0 {
		sqlType = sqlTypes[0]
	} else {
		sqlType = constant.ExternSql
	}
	if len(envStmt) > 0 {
		for i, sql := range envStmt {
			if i < len(sqlTypes) {
				sqlType = sqlTypes[i]
			}
			ctx, err = RecordStatement(ctx, ses, proc, nil, envBegin, sql, sqlType, true)
			if err != nil {
				return nil, err
			}
			motrace.EndStatement(ctx, retErr, 0, 0, 0)
		}
	} else {
		ctx, err = RecordStatement(ctx, ses, proc, nil, envBegin, "", sqlType, true)
		if err != nil {
			return nil, err
		}
		motrace.EndStatement(ctx, retErr, 0, 0, 0)
	}

	tenant := ses.GetTenantInfo()
	if tenant == nil {
		tenant, _ = GetTenantInfo(ctx, "internal")
	}
	incStatementCounter(tenant.GetTenant(), nil)
	incStatementErrorsCounter(tenant.GetTenant(), nil)
	return ctx, nil
}

// RecordStatementTxnID record txnID after TxnBegin or Compile(autocommit=1)
var RecordStatementTxnID = func(ctx context.Context, ses *Session) error {
	var txn TxnOperator
	var err error
	if stm := motrace.StatementFromContext(ctx); ses != nil && stm != nil && stm.IsZeroTxnID() {
		if handler := ses.GetTxnHandler(); handler.IsValidTxnOperator() {
			// simplify the logic of TxnOperator. refer to https://github.com/matrixorigin/matrixone/pull/13436#pullrequestreview-1779063200
			_, txn, err = handler.GetTxnOperator()
			if err != nil {
				return err
			}
			stm.SetTxnID(txn.Txn().ID)
			ses.SetTxnId(txn.Txn().ID)
		}
		stm.Report(ctx)
	}

	// set frontend statement's txn-id
	if upSes := ses.upstream; upSes != nil && upSes.tStmt != nil && upSes.tStmt.IsZeroTxnID() /* not record txn-id */ {
		// background session has valid txn
		if handler := ses.GetTxnHandler(); handler.IsValidTxnOperator() {
			_, txn, err = handler.GetTxnOperator()
			if err != nil {
				return err
			}
			// set upstream (the frontend session) statement's txn-id
			// PS: only skip ONE txn
			if stmt := upSes.tStmt; stmt.NeedSkipTxn() /* normally set by determineUserHasPrivilegeSet */ {
				// need to skip the whole txn, so it records the skipped txn-id
				stmt.SetSkipTxn(false)
				stmt.SetSkipTxnId(txn.Txn().ID)
			} else if txnId := txn.Txn().ID; !stmt.SkipTxnId(txnId) {
				upSes.tStmt.SetTxnID(txnId)
			}
		}
	}
	return nil
}

func incStatementCounter(tenant string, stmt tree.Statement) {
	metric.StatementCounter(tenant, getStatementType(stmt).GetQueryType()).Inc()
}

func incTransactionCounter(tenant string) {
	metric.TransactionCounter(tenant).Inc()
}

func incTransactionErrorsCounter(tenant string, t metric.SQLType) {
	if t == metric.SQLTypeRollback {
		return
	}
	metric.TransactionErrorsCounter(tenant, t).Inc()
}

func incStatementErrorsCounter(tenant string, stmt tree.Statement) {
	metric.StatementErrorsCounter(tenant, getStatementType(stmt).GetQueryType()).Inc()
}

// build plan json when marhal plan error
func buildErrorJsonPlan(buffer *bytes.Buffer, uuid uuid.UUID, errcode uint16, msg string) []byte {
	var bytes [36]byte
	util.EncodeUUIDHex(bytes[:], uuid[:])
	explainData := explain.ExplainData{
		Code:    errcode,
		Message: msg,
		Uuid:    util.UnsafeBytesToString(bytes[:]),
	}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	encoder.Encode(explainData)
	return buffer.Bytes()
}

type jsonPlanHandler struct {
	jsonBytes  []byte
	statsBytes statistic.StatsArray
	stats      motrace.Statistic
	buffer     *bytes.Buffer
}

func NewJsonPlanHandler(ctx context.Context, stmt *motrace.StatementInfo, plan *plan2.Plan) *jsonPlanHandler {
	h := NewMarshalPlanHandler(ctx, stmt, plan)
	jsonBytes := h.Marshal(ctx)
	statsBytes, stats := h.Stats(ctx)
	return &jsonPlanHandler{
		jsonBytes:  jsonBytes,
		statsBytes: statsBytes,
		stats:      stats,
		buffer:     h.handoverBuffer(),
	}
}

func (h *jsonPlanHandler) Stats(ctx context.Context) (statistic.StatsArray, motrace.Statistic) {
	return h.statsBytes, h.stats
}

func (h *jsonPlanHandler) Marshal(ctx context.Context) []byte {
	return h.jsonBytes
}

func (h *jsonPlanHandler) Free() {
	if h.buffer != nil {
		releaseMarshalPlanBufferPool(h.buffer)
		h.buffer = nil
		h.jsonBytes = nil
	}
}

type marshalPlanHandler struct {
	query       *plan.Query
	marshalPlan *explain.ExplainData
	stmt        *motrace.StatementInfo
	uuid        uuid.UUID
	buffer      *bytes.Buffer
}

func NewMarshalPlanHandler(ctx context.Context, stmt *motrace.StatementInfo, plan *plan2.Plan) *marshalPlanHandler {
	// TODO: need mem improvement
	uuid := uuid.UUID(stmt.StatementID)
	stmt.MarkResponseAt()
	if plan == nil || plan.GetQuery() == nil {
		return &marshalPlanHandler{
			query:       nil,
			marshalPlan: nil,
			stmt:        stmt,
			uuid:        uuid,
			buffer:      nil,
		}
	}
	query := plan.GetQuery()
	h := &marshalPlanHandler{
		query:  query,
		stmt:   stmt,
		uuid:   uuid,
		buffer: nil,
	}
	// check longQueryTime, need after StatementInfo.MarkResponseAt
	// MoLogger NOT record ExecPlan
	if stmt.Duration > motrace.GetLongQueryTime() && !stmt.IsMoLogger() {
		h.marshalPlan = explain.BuildJsonPlan(ctx, h.uuid, &explain.MarshalPlanOptions, h.query)
	}
	return h
}

func (h *marshalPlanHandler) Free() {
	h.stmt = nil
	if h.buffer != nil {
		releaseMarshalPlanBufferPool(h.buffer)
		h.buffer = nil
	}
}

func (h *marshalPlanHandler) handoverBuffer() *bytes.Buffer {
	b := h.buffer
	h.buffer = nil
	return b
}

var marshalPlanBufferPool = sync.Pool{New: func() any {
	return bytes.NewBuffer(make([]byte, 0, 8192))
}}

// get buffer from marshalPlanBufferPool
func getMarshalPlanBufferPool() *bytes.Buffer {
	return marshalPlanBufferPool.Get().(*bytes.Buffer)
}

func releaseMarshalPlanBufferPool(b *bytes.Buffer) {
	marshalPlanBufferPool.Put(b)
}

// allocBufferIfNeeded should call just right before needed.
// It will reuse buffer from pool if possible.
func (h *marshalPlanHandler) allocBufferIfNeeded() {
	if h.buffer == nil {
		h.buffer = getMarshalPlanBufferPool()
	}
}

func (h *marshalPlanHandler) Marshal(ctx context.Context) (jsonBytes []byte) {
	var err error
	h.allocBufferIfNeeded()
	h.buffer.Reset()
	if h.marshalPlan != nil {
		var jsonBytesLen = 0
		// XXX, `buffer` can be used repeatedly as a global variable in the future
		// Provide a relatively balanced initial capacity [8192] for byte slice to prevent multiple memory requests
		encoder := json.NewEncoder(h.buffer)
		encoder.SetEscapeHTML(false)
		err = encoder.Encode(h.marshalPlan)
		if err != nil {
			moError := moerr.NewInternalError(ctx, "serialize plan to json error: %s", err.Error())
			h.buffer.Reset()
			jsonBytes = buildErrorJsonPlan(h.buffer, h.uuid, moError.ErrorCode(), moError.Error())
		} else {
			jsonBytesLen = h.buffer.Len()
		}
		// BG: bytes.Buffer maintain buf []byte.
		// if buf[off:] not enough but len(buf) is enough place, then it will reset off = 0.
		// So, in here, we need call Next(...) after all data has been written
		if jsonBytesLen > 0 {
			jsonBytes = h.buffer.Next(jsonBytesLen)
		}
	} else if h.query != nil {
		jsonBytes = buildErrorJsonPlan(h.buffer, h.uuid, moerr.ErrWarn, "sql query ignore execution plan")
	} else {
		jsonBytes = buildErrorJsonPlan(h.buffer, h.uuid, moerr.ErrWarn, "sql query no record execution plan")
	}
	return
}

func (h *marshalPlanHandler) Stats(ctx context.Context) (statsByte statistic.StatsArray, stats motrace.Statistic) {
	if h.query != nil {
		options := &explain.MarshalPlanOptions
		statsByte.Reset()
		for _, node := range h.query.Nodes {
			// part 1: for statistic.StatsArray
			s := explain.GetStatistic4Trace(ctx, node, options)
			statsByte.Add(&s)
			// part 2: for motrace.Statistic
			if node.NodeType == plan.Node_TABLE_SCAN || node.NodeType == plan.Node_EXTERNAL_SCAN {
				rows, bytes := explain.GetInputRowsAndInputSize(ctx, node, options)
				stats.RowsRead += rows
				stats.BytesScan += bytes
			}
		}

		statsInfo := statistic.StatsInfoFromContext(ctx)
		if statsInfo != nil {
			val := int64(statsByte.GetTimeConsumed()) +
				int64(statsInfo.ParseDuration+
					statsInfo.CompileDuration+
					statsInfo.PlanDuration) - (statsInfo.IOAccessTimeConsumption + statsInfo.LockTimeConsumption)
			if val < 0 {
				logutil.Warnf(" negative cpu (%s) + statsInfo(%d + %d + %d - %d - %d) = %d",
					uuid.UUID(h.stmt.StatementID).String(),
					statsInfo.ParseDuration,
					statsInfo.CompileDuration,
					statsInfo.PlanDuration,
					statsInfo.IOAccessTimeConsumption,
					statsInfo.LockTimeConsumption,
					val)
				v2.GetTraceNegativeCUCounter("cpu").Inc()
			} else {
				statsByte.WithTimeConsumed(float64(val))
			}
		}
	} else {
		statsByte = statistic.DefaultStatsArray
	}
	return
}
