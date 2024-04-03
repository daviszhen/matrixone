// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"errors"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"time"
)

func (mce *MysqlCmdExecutor) executeStmtInBackWithTxn(requestCtx context.Context,
	backCtx *backExecCtx,
	stmt tree.Statement,
	proc *process.Process,
	cw ComputationWrapper,
	i int,
	cws []ComputationWrapper,
	pu *config.ParameterUnit,
	tenant string,
	userName string,
	sql string,
	execCtx *ExecCtx,
) (err error) {
	// defer transaction state management.
	defer func() {
		err = finishTxnFuncInBack(requestCtx, backCtx, err, execCtx)
	}()

	// statement management
	_, txnOp, err := backCtx.GetTxnHandler().GetTxnOperator()
	if err != nil {
		return err
	}

	//non derived statement
	if txnOp != nil && !backCtx.IsDerivedStmt() {
		//startStatement has been called
		ok, _ := backCtx.GetTxnHandler().calledStartStmt()
		if !ok {
			txnOp.GetWorkspace().StartStatement()
			backCtx.GetTxnHandler().enableStartStmt(txnOp.Txn().ID)
		}
	}

	// defer Start/End Statement management, called after finishTxnFunc()
	defer func() {
		// move finishTxnFunc() out to another defer so that if finishTxnFunc
		// paniced, the following is still called.
		var err3 error
		_, txnOp, err3 = backCtx.GetTxnHandler().GetTxnOperator()
		if err3 != nil {
			//logError(ses, ses.GetDebugString(), err3.Error())
			return
		}
		//non derived statement
		if txnOp != nil && !backCtx.IsDerivedStmt() {
			//startStatement has been called
			ok, id := backCtx.GetTxnHandler().calledStartStmt()
			if ok && bytes.Equal(txnOp.Txn().ID, id) {
				txnOp.GetWorkspace().EndStatement()
			}
		}
		backCtx.GetTxnHandler().disableStartStmt()
	}()
	return mce.executeStmtInBack(requestCtx, backCtx, proc, cw, i, cws, pu, tenant, userName, sql, execCtx)
}

func (mce *MysqlCmdExecutor) executeStmtInBack(requestCtx context.Context,
	backCtx *backExecCtx,
	proc *process.Process,
	cw ComputationWrapper,
	i int,
	cws []ComputationWrapper,
	pu *config.ParameterUnit,
	tenant string,
	userName string,
	sql string,
	execCtx *ExecCtx,
) (err error) {
	var cmpBegin time.Time
	var ret interface{}
	var runner ComputationRunner
	var selfHandle bool

	// XXX XXX
	// I hope I can break the following code into several functions, but I can't.
	// After separating the functions, the system cannot boot, due to mo_account
	// not exists.  No clue why, the closure/capture must do some magic.

	//check transaction states
	switch execCtx.stmt.(type) {
	case *tree.BeginTransaction:
		err = backCtx.TxnBegin()
		if err != nil {
			return
		}
	case *tree.CommitTransaction:
		err = backCtx.TxnCommit()
		if err != nil {
			return
		}
	case *tree.RollbackTransaction:
		err = backCtx.TxnRollback()
		if err != nil {
			return
		}
	}

	selfHandle = false
	switch st := execCtx.stmt.(type) {
	case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
		selfHandle = true
	case *tree.SetRole:
		selfHandle = true
		//switch role
		err = mce.handleSwitchRole(requestCtx, st)
		if err != nil {
			return
		}
	case *tree.Use:
		selfHandle = true
		//use database
		err = mce.handleChangeDB(requestCtx, st.Name.Compare())
		if err != nil {
			return
		}
	case *tree.MoDump:
		selfHandle = true
		//dump
		err = mce.handleDump(requestCtx, st)
		if err != nil {
			return
		}
	case *tree.CreateDatabase:
		err = inputNameIsInvalid(proc.Ctx, string(st.Name))
		if err != nil {
			return
		}
		if st.SubscriptionOption != nil && backCtx.tenant != nil && !backCtx.tenant.IsAdminRole() {
			err = moerr.NewInternalError(proc.Ctx, "only admin can create subscription")
			return
		}
		st.Sql = sql
	case *tree.DropDatabase:
		err = inputNameIsInvalid(proc.Ctx, string(st.Name))
		if err != nil {
			return
		}
		// if the droped database is the same as the one in use, database must be reseted to empty.
		if string(st.Name) == backCtx.GetDatabaseName() {
			backCtx.SetDatabaseName("")
		}
	case *tree.PrepareStmt,
		*tree.PrepareString,
		*tree.AnalyzeStmt,
		*tree.ExplainAnalyze,
		*tree.AlterDataBaseConfig, *tree.ShowTableStatus:
		selfHandle = true
		return moerr.NewInternalError(requestCtx, "does not support in background exec")
	case *tree.CreateConnector:
		selfHandle = true
		err = mce.handleCreateConnector(requestCtx, st)
		if err != nil {
			return
		}
	case *tree.PauseDaemonTask:
		selfHandle = true
		err = mce.handlePauseDaemonTask(requestCtx, st)
		if err != nil {
			return
		}
	case *tree.CancelDaemonTask:
		selfHandle = true
		err = mce.handleCancelDaemonTask(requestCtx, st)
		if err != nil {
			return
		}
	case *tree.ResumeDaemonTask:
		selfHandle = true
		err = mce.handleResumeDaemonTask(requestCtx, st)
		if err != nil {
			return
		}
	case *tree.DropConnector:
		selfHandle = true
		err = mce.handleDropConnector(requestCtx, st)
		if err != nil {
			return
		}
	case *tree.ShowConnectors:
		selfHandle = true
		if err = mce.handleShowConnectors(requestCtx, i, len(cws)); err != nil {
			return
		}
	case *tree.Deallocate:
		selfHandle = true
		err = mce.handleDeallocate(requestCtx, st)
		if err != nil {
			return
		}
	case *tree.Reset:
		selfHandle = true
		err = mce.handleReset(requestCtx, st)
		if err != nil {
			return
		}
	case *tree.SetVar:
		selfHandle = true
		err = mce.handleSetVar(requestCtx, st, sql)
		if err != nil {
			return
		}
	case *tree.ShowVariables:
		selfHandle = true
		err = mce.handleShowVariables(st, proc, i, len(cws))
		if err != nil {
			return
		}
	case *tree.ShowErrors, *tree.ShowWarnings:
		selfHandle = true
		err = mce.handleShowErrors(i, len(cws))
		if err != nil {
			return
		}
	case *tree.ExplainStmt:
		selfHandle = true
		if err = mce.handleExplainStmt(requestCtx, st); err != nil {
			return
		}
	case *InternalCmdFieldList:
		selfHandle = true
		if err = mce.handleCmdFieldList(requestCtx, st); err != nil {
			return
		}
	case *tree.CreatePublication:
		selfHandle = true
		if err = mce.handleCreatePublication(requestCtx, st); err != nil {
			return
		}
	case *tree.AlterPublication:
		selfHandle = true
		if err = mce.handleAlterPublication(requestCtx, st); err != nil {
			return
		}
	case *tree.DropPublication:
		selfHandle = true
		if err = mce.handleDropPublication(requestCtx, st); err != nil {
			return
		}
	case *tree.ShowSubscriptions:
		selfHandle = true
		if err = mce.handleShowSubscriptions(requestCtx, st, i, len(cws)); err != nil {
			return
		}
	case *tree.CreateStage:
		selfHandle = true
		if err = mce.handleCreateStage(requestCtx, st); err != nil {
			return
		}
	case *tree.DropStage:
		selfHandle = true
		if err = mce.handleDropStage(requestCtx, st); err != nil {
			return
		}
	case *tree.AlterStage:
		selfHandle = true
		if err = mce.handleAlterStage(requestCtx, st); err != nil {
			return
		}
	case *tree.CreateAccount:
		selfHandle = true
		if err = mce.handleCreateAccount(requestCtx, st); err != nil {
			return
		}
	case *tree.DropAccount:
		selfHandle = true
		if err = mce.handleDropAccount(requestCtx, st); err != nil {
			return
		}
	case *tree.AlterAccount:
		selfHandle = true
		if err = mce.handleAlterAccount(requestCtx, st); err != nil {
			return
		}
	case *tree.CreateUser:
		selfHandle = true
		if err = mce.handleCreateUser(requestCtx, st); err != nil {
			return
		}
	case *tree.DropUser:
		selfHandle = true
		if err = mce.handleDropUser(requestCtx, st); err != nil {
			return
		}
	case *tree.AlterUser: //TODO
		selfHandle = true
		if err = mce.handleAlterUser(requestCtx, st); err != nil {
			return
		}
	case *tree.CreateRole:
		selfHandle = true
		if err = mce.handleCreateRole(requestCtx, st); err != nil {
			return
		}
	case *tree.DropRole:
		selfHandle = true
		if err = mce.handleDropRole(requestCtx, st); err != nil {
			return
		}
	case *tree.CreateFunction:
		selfHandle = true
		if err = st.Valid(); err != nil {
			return err
		}
		if err = mce.handleCreateFunction(requestCtx, st); err != nil {
			return
		}
	case *tree.DropFunction:
		selfHandle = true
		if err = mce.handleDropFunction(requestCtx, st, proc); err != nil {
			return
		}
	case *tree.CreateProcedure:
		selfHandle = true
		if err = mce.handleCreateProcedure(requestCtx, st); err != nil {
			return
		}
	case *tree.DropProcedure:
		selfHandle = true
		if err = mce.handleDropProcedure(requestCtx, st); err != nil {
			return
		}
	case *tree.CallStmt:
		selfHandle = true
		if err = mce.handleCallProcedure(requestCtx, st, proc, i, len(cws)); err != nil {
			return
		}
	case *tree.Grant:
		selfHandle = true
		switch st.Typ {
		case tree.GrantTypeRole:
			if err = mce.handleGrantRole(requestCtx, &st.GrantRole); err != nil {
				return
			}
		case tree.GrantTypePrivilege:
			if err = mce.handleGrantPrivilege(requestCtx, &st.GrantPrivilege); err != nil {
				return
			}
		}
	case *tree.Revoke:
		selfHandle = true
		switch st.Typ {
		case tree.RevokeTypeRole:
			if err = mce.handleRevokeRole(requestCtx, &st.RevokeRole); err != nil {
				return
			}
		case tree.RevokeTypePrivilege:
			if err = mce.handleRevokePrivilege(requestCtx, &st.RevokePrivilege); err != nil {
				return
			}
		}
	case *tree.Kill:
		selfHandle = true
		if err = mce.handleKill(requestCtx, st); err != nil {
			return
		}
	case *tree.ShowAccounts:
		selfHandle = true
		if err = mce.handleShowAccounts(requestCtx, st, i, len(cws)); err != nil {
			return
		}
	case *tree.ShowCollation:
		selfHandle = true
		if err = mce.handleShowCollation(st, proc, i, len(cws)); err != nil {
			return
		}
	case *tree.Load:
		return moerr.NewInternalError(requestCtx, "does not support Loacd in background exec")
	case *tree.ShowBackendServers:
		selfHandle = true
		if err = mce.handleShowBackendServers(requestCtx, i, len(cws)); err != nil {
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
			st.Username = userName
		}
		if len(st.Hostname) == 0 || st.Hostname == "%" {
			st.Hostname = rootHost
		}
	case *tree.BackupStart:
		selfHandle = true
		if err = mce.handleStartBackup(requestCtx, st); err != nil {
			return
		}
	case *tree.EmptyStmt:
		selfHandle = true
		if err = mce.handleEmptyStmt(requestCtx, st); err != nil {
			return
		}
	}

	if selfHandle {
		return
	}

	cmpBegin = time.Now()

	if ret, err = cw.Compile(requestCtx, backCtx, backCtx.outputCallback); err != nil {
		return
	}
	execCtx.stmt = cw.GetAst()
	// reset some special stmt for execute statement
	switch st := execCtx.stmt.(type) {
	case *tree.SetVar:
		err = mce.handleSetVar(requestCtx, st, sql)
		if err != nil {
			return
		} else {
			return
		}
	case *tree.ShowVariables:
		err = mce.handleShowVariables(st, proc, i, len(cws))
		if err != nil {
			return
		} else {
			return
		}
	case *tree.ShowErrors, *tree.ShowWarnings:
		err = mce.handleShowErrors(i, len(cws))
		if err != nil {
			return
		} else {
			return
		}
	}

	runner = ret.(ComputationRunner)

	// only log if build time is longer than 1s
	if time.Since(cmpBegin) > time.Second {
		//logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Build : %s", time.Since(cmpBegin).String()))
	}

	var columns []interface{}
	var mrs *MysqlResultSet
	mrs = backCtx.GetMysqlResultSet()
	// cw.Compile might rewrite sql, here we fetch the latest version
	switch execCtx.stmt.(type) {
	//produce result set
	case *tree.Select:
		//no select into in the background exec
		columns, err = cw.GetColumns()
		if err != nil {
			//logError(backCtx, backCtx.GetDebugString(),
			//	"Failed to get columns from computation handler",
			//	zap.Error(err))
			return
		}
		for _, c := range columns {
			mysqlc := c.(Column)
			mrs.AddColumn(mysqlc)
		}
		if c, ok := cw.(*TxnComputationWrapper); ok {
			backCtx.rs = &plan.ResultColDef{ResultCols: plan2.GetResultColumnsFromPlan(c.plan)}
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
			//logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Run : %s", time.Since(runBegin).String()))
		}

	case *tree.ShowCreateTable, *tree.ShowCreateDatabase, *tree.ShowTables, *tree.ShowSequences, *tree.ShowDatabases, *tree.ShowColumns,
		*tree.ShowProcessList, *tree.ShowStatus, *tree.ShowTableStatus, *tree.ShowGrants, *tree.ShowRolesStmt,
		*tree.ShowIndex, *tree.ShowCreateView, *tree.ShowTarget, *tree.ShowCollation, *tree.ValuesStatement,
		*tree.ExplainFor, *tree.ExplainStmt, *tree.ShowTableNumber, *tree.ShowColumnNumber, *tree.ShowTableValues, *tree.ShowLocks, *tree.ShowNodeList, *tree.ShowFunctionOrProcedureStatus,
		*tree.ShowPublications, *tree.ShowCreatePublications, *tree.ShowStages:
		columns, err = cw.GetColumns()
		if err != nil {
			//logError(ses, ses.GetDebugString(),
			//	"Failed to get columns from computation handler",
			//	zap.Error(err))
			return
		}
		for _, c := range columns {
			mysqlc := c.(Column)
			mrs.AddColumn(mysqlc)
		}
		if c, ok := cw.(*TxnComputationWrapper); ok {
			backCtx.rs = &plan.ResultColDef{ResultCols: plan2.GetResultColumnsFromPlan(c.plan)}
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
			//logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Run : %s", time.Since(runBegin).String()))
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
		runBegin := time.Now()

		if _, err = runner.Run(0); err != nil {
			return
		}

		// only log if run time is longer than 1s
		if time.Since(runBegin) > time.Second {
			//logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Run : %s", time.Since(runBegin).String()))
		}
	}
	return
}

type backExecCtx struct {
	requestCtx           context.Context
	connectCtx           context.Context
	pu                   *config.ParameterUnit
	pool                 *mpool.MPool
	txnClient            TxnClient
	autoIncrCacheManager *defines.AutoIncrCacheManager
	proto                MysqlProtocol
	buf                  *buffer.Buffer
	stmtProfile          process.StmtProfile
	tenant               *TenantInfo
	txnHandler           *TxnHandler
	txnCompileCtx        *TxnCompilerContext
	mrs                  *MysqlResultSet
	//it gets the result set from the pipeline and send it to the client
	outputCallback func(interface{}, *batch.Batch) error

	//all the result set of executing the sql in background task
	allResultSet []*MysqlResultSet
	rs           *plan.ResultColDef

	// result batches of executing the sql in background task
	// set by func batchFetcher
	resultBatches []*batch.Batch
	serverStatus  uint16
	derivedStmt   bool
	optionBits    uint32
	shareTxn      bool
	gSysVars      *GlobalSystemVariables
	// when starting a transaction in session, the snapshot ts of the transaction
	// is to get a TN push to CN to get the maximum commitTS. but there is a problem,
	// when the last transaction ends and the next one starts, it is possible that the
	// log of the last transaction has not been pushed to CN, we need to wait until at
	// least the commit of the last transaction log of the previous transaction arrives.
	lastCommitTS timestamp.Timestamp
	upstream     *Session
	sql          string
	accountId    uint32
	label        map[string]string
	timeZone     *time.Location
}

func (backCtx *backExecCtx) cleanCache() {
}

func (backCtx *backExecCtx) GetUpstream() TempInter {
	return backCtx.upstream
}

func (backCtx *backExecCtx) EnableInitTempEngine() {

}

func (backCtx *backExecCtx) SetTempEngine(ctx context.Context, te engine.Engine) error {
	return nil
}

func (backCtx *backExecCtx) SetTempTableStorage(getClock clock.Clock) (*metadata.TNService, error) {
	return nil, nil
}

func (backCtx *backExecCtx) getCNLabels() map[string]string {
	return backCtx.label
}

func (backCtx *backExecCtx) SetData(i [][]interface{}) {

}

func (backCtx *backExecCtx) GetIsInternal() bool {
	return false
}

func (backCtx *backExecCtx) SetPlan(plan *plan.Plan) {
}

func (backCtx *backExecCtx) SetAccountId(u uint32) {
	backCtx.accountId = u
}

func (backCtx *backExecCtx) GetRawBatchBackgroundExec(ctx context.Context) BackgroundExec {
	//TODO implement me
	panic("implement me")
}

func (backCtx *backExecCtx) SetRequestContext(ctx context.Context) {
	backCtx.requestCtx = ctx
}

func (backCtx *backExecCtx) GetConnectionID() uint32 {
	return 0
}

func (backCtx *backExecCtx) SetMysqlResultSet(mrs *MysqlResultSet) {
	backCtx.mrs = mrs
}

func (backCtx *backExecCtx) getQueryId(internal bool) []string {
	return nil
}

func (backCtx *backExecCtx) CopySeqToProc(proc *process.Process) {

}

func (backCtx *backExecCtx) GetStmtProfile() *process.StmtProfile {
	return &backCtx.stmtProfile
}

func (backCtx *backExecCtx) GetBuffer() *buffer.Buffer {
	return backCtx.buf
}

func (backCtx *backExecCtx) GetSqlHelper() *SqlHelper {
	return nil
}

func (backCtx *backExecCtx) GetProc() *process.Process {
	return nil
}

func (backCtx *backExecCtx) GetLastInsertID() uint64 {
	return 0
}

func (backCtx *backExecCtx) GetAutoIncrCacheManager() *defines.AutoIncrCacheManager {
	return backCtx.autoIncrCacheManager
}

func (backCtx *backExecCtx) GetMemPool() *mpool.MPool {
	return backCtx.pool
}

func (backCtx *backExecCtx) SetSql(sql string) {
	backCtx.sql = sql
}

func (backCtx *backExecCtx) SetShowStmtType(statement ShowStatementType) {
}

func (backCtx *backExecCtx) RemovePrepareStmt(name string) {

}

func (backCtx *backExecCtx) CountPayload(i int) {

}

func (backCtx *backExecCtx) GetPrepareStmt(name string) (*PrepareStmt, error) {
	return nil, moerr.NewInternalError(backCtx.requestCtx, "do not support prepare in background exec")
}

func (backCtx *backExecCtx) IsBackgroundSession() bool {
	return true
}

func (backCtx *backExecCtx) GetTxnCompileCtx() *TxnCompilerContext {
	return backCtx.txnCompileCtx
}

func (backCtx *backExecCtx) GetCmd() CommandType {
	return COM_QUERY
}

func (backCtx *backExecCtx) GetServerStatus() uint16 {
	return backCtx.serverStatus
}

func (backCtx *backExecCtx) SetNewResponse(category int, affectedRows uint64, cmd int, d interface{}, cwIndex, cwsLen int) *Response {
	return nil
}

func (backCtx *backExecCtx) GetMysqlResultSet() *MysqlResultSet {
	return backCtx.mrs
}

func (backCtx *backExecCtx) GetTxnHandler() *TxnHandler {
	return backCtx.txnHandler
}

func (backCtx *backExecCtx) GetMysqlProtocol() MysqlProtocol {
	return backCtx.proto
}

func (backCtx *backExecCtx) TxnCreate() (context.Context, TxnOperator, error) {
	// SERVER_STATUS_IN_TRANS should be set to true regardless of whether autocommit is equal to 1.
	backCtx.SetServerStatus(SERVER_STATUS_IN_TRANS)

	if !backCtx.txnHandler.IsValidTxnOperator() {
		return backCtx.txnHandler.NewTxn()
	}
	txnCtx, txnOp, err := backCtx.txnHandler.GetTxnOperator()
	return txnCtx, txnOp, err
}

func (backCtx *backExecCtx) updateLastCommitTS(lastCommitTS timestamp.Timestamp) {
	if lastCommitTS.Greater(backCtx.lastCommitTS) {
		backCtx.lastCommitTS = lastCommitTS
	}
	if backCtx.upstream != nil {
		backCtx.upstream.updateLastCommitTS(lastCommitTS)
	}
}

func (backCtx *backExecCtx) GetSqlOfStmt() string {
	return ""
}

func (backCtx *backExecCtx) GetStmtId() uuid.UUID {
	return [16]byte{}
}

func (backCtx *backExecCtx) GetTxnId() uuid.UUID {
	return backCtx.stmtProfile.GetTxnId()
}

func (backCtx *backExecCtx) SetTxnId(id []byte) {
	backCtx.stmtProfile.SetTxnId(id)
}

// GetTenantName return tenant name according to GetTenantInfo and stmt.
//
// With stmt = nil, should be only called in TxnHandler.NewTxn, TxnHandler.CommitTxn, TxnHandler.RollbackTxn
func (backCtx *backExecCtx) GetTenantNameWithStmt(stmt tree.Statement) string {
	tenant := sysAccountName
	if backCtx.GetTenantInfo() != nil && (stmt == nil || !IsPrepareStatement(stmt)) {
		tenant = backCtx.GetTenantInfo().GetTenant()
	}
	return tenant
}

func (backCtx *backExecCtx) GetTenantName() string {
	return backCtx.GetTenantNameWithStmt(nil)
}

func (backCtx *backExecCtx) getLastCommitTS() timestamp.Timestamp {
	minTS := backCtx.lastCommitTS
	if backCtx.upstream != nil {
		v := backCtx.upstream.getLastCommitTS()
		if v.Greater(minTS) {
			minTS = v
		}
	}
	return minTS
}

func (backCtx *backExecCtx) GetFromRealUser() bool {
	return false
}

func (backCtx *backExecCtx) GetDebugString() string {
	return ""
}

func (backCtx *backExecCtx) GetTempTableStorage() *memorystorage.Storage {
	return nil
}

func (backCtx *backExecCtx) IfInitedTempEngine() bool {
	return false
}

func (backCtx *backExecCtx) GetParameterUnit() *config.ParameterUnit {
	return backCtx.pu
}

func (backCtx *backExecCtx) GetConnectContext() context.Context {
	return backCtx.connectCtx
}

func (backCtx *backExecCtx) GetUserDefinedVar(name string) (SystemVariableType, *UserDefinedVar, error) {
	return nil, nil, moerr.NewInternalError(backCtx.requestCtx, "do not support user defined var in background exec")
}

func (backCtx *backExecCtx) GetSessionVar(name string) (interface{}, error) {
	return nil, nil
}

func (backCtx *backExecCtx) getGlobalSystemVariableValue(name string) (interface{}, error) {
	return nil, moerr.NewInternalError(backCtx.requestCtx, "do not support system variable in background exec")
}

func (backCtx *backExecCtx) GetBackgroundExec(ctx context.Context) BackgroundExec {
	return NewBackgroundExec(
		ctx,
		backCtx,
		backCtx.GetMemPool(),
		backCtx.GetParameterUnit())
}

func (backCtx *backExecCtx) GetStorage() engine.Engine {
	return backCtx.pu.StorageEngine
}

func (backCtx *backExecCtx) GetTenantInfo() *TenantInfo {
	return backCtx.tenant
}

func (backCtx *backExecCtx) GetAccountId() uint32 {
	return backCtx.accountId
}

func (backCtx *backExecCtx) GetSql() string {
	return backCtx.sql
}

func (backCtx *backExecCtx) GetUserName() string {
	return backCtx.proto.GetUserName()
}

func (backCtx *backExecCtx) GetStatsCache() *plan2.StatsCache {
	return nil
}

func (backCtx *backExecCtx) GetRequestContext() context.Context {
	return backCtx.requestCtx
}

func (backCtx *backExecCtx) GetTimeZone() *time.Location {
	return backCtx.timeZone
}

func (backCtx *backExecCtx) clear() {
	backCtx.requestCtx = nil
	backCtx.connectCtx = nil
	backCtx.pu = nil
	backCtx.pool = nil
	backCtx.txnClient = nil
	backCtx.autoIncrCacheManager = nil
	backCtx.proto = nil
	if backCtx.buf != nil {
		backCtx.buf.Free()
		backCtx.buf = nil
	}
	backCtx.tenant = nil
	backCtx.txnHandler = nil
	backCtx.txnCompileCtx = nil
	backCtx.mrs = nil
	backCtx.allResultSet = nil
	backCtx.resultBatches = nil
	backCtx.gSysVars = nil
}

func (backCtx *backExecCtx) InActiveTransaction() bool {
	return backCtx.ServerStatusIsSet(SERVER_STATUS_IN_TRANS)
}

func (backCtx *backExecCtx) TxnRollbackSingleStatement(stmt tree.Statement, inputErr error) error {
	var err error
	var rollbackWholeTxn bool
	if inputErr != nil {
		rollbackWholeTxn = isErrorRollbackWholeTxn(inputErr)
	}
	/*
			Rollback Rules:
			1, if it is in single-statement mode (Case2):
				it rollbacks.
			2, if it is in multi-statement mode (Case1,Case3,Case4):
		        the transaction need to be rollback at the end of the statement.
				(every error will abort the transaction.)
	*/
	if !backCtx.InMultiStmtTransactionMode() ||
		backCtx.InActiveTransaction() && NeedToBeCommittedInActiveTransaction(stmt) ||
		rollbackWholeTxn {
		//Case1.1: autocommit && not_begin
		//Case1.2: (not_autocommit || begin) && activeTxn && needToBeCommitted
		//Case1.3: the error that should rollback the whole txn
		err = backCtx.rollbackWholeTxn()
	} else {
		//Case2: not ( autocommit && !begin ) && not ( activeTxn && needToBeCommitted )
		//<==>  ( not_autocommit || begin ) && not ( activeTxn && needToBeCommitted )
		//just rollback statement
		var err3 error
		txnCtx, txnOp, err3 := backCtx.txnHandler.GetTxnOperator()
		if err3 != nil {
			return err3
		}

		//non derived statement
		if txnOp != nil && !backCtx.IsDerivedStmt() {
			//incrStatement has been called
			ok, id := backCtx.txnHandler.calledIncrStmt()
			if ok && bytes.Equal(txnOp.Txn().ID, id) {
				err = txnOp.GetWorkspace().RollbackLastStatement(txnCtx)
				backCtx.txnHandler.disableIncrStmt()
				if err != nil {
					err4 := backCtx.rollbackWholeTxn()
					return errors.Join(err, err4)
				}
			}
		}
	}
	return err
}

func (backCtx *backExecCtx) TxnCommitSingleStatement(stmt tree.Statement) error {
	var err error
	/*
		Commit Rules:
		1, if it is in single-statement mode:
			it commits.
		2, if it is in multi-statement mode:
			if the statement is the one can be executed in the active transaction,
				the transaction need to be committed at the end of the statement.
	*/
	if !backCtx.InMultiStmtTransactionMode() ||
		backCtx.InActiveTransaction() && NeedToBeCommittedInActiveTransaction(stmt) {
		err = backCtx.txnHandler.CommitTxn()
		backCtx.ClearServerStatus(SERVER_STATUS_IN_TRANS)
		backCtx.ClearOptionBits(OPTION_BEGIN)
	}
	return err
}

func (backCtx *backExecCtx) IsDerivedStmt() bool {
	return backCtx.derivedStmt
}

func (backCtx *backExecCtx) TxnBegin() error {
	var err error
	if backCtx.InMultiStmtTransactionMode() {
		backCtx.ClearServerStatus(SERVER_STATUS_IN_TRANS)
		err = backCtx.txnHandler.CommitTxn()
	}
	backCtx.ClearOptionBits(OPTION_BEGIN)
	if err != nil {
		/*
			fix issue 6024.
			When we get a w-w conflict during commit the txn,
			we convert the error into a readable error.
		*/
		if moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
			return moerr.NewInternalError(backCtx.requestCtx, writeWriteConflictsErrorInfo())
		}
		return err
	}
	backCtx.SetOptionBits(OPTION_BEGIN)
	backCtx.SetServerStatus(SERVER_STATUS_IN_TRANS)
	_, _, err = backCtx.txnHandler.NewTxn()
	return err
}

func (backCtx *backExecCtx) TxnCommit() error {
	var err error
	backCtx.ClearServerStatus(SERVER_STATUS_IN_TRANS | SERVER_STATUS_IN_TRANS_READONLY)
	err = backCtx.txnHandler.CommitTxn()
	backCtx.ClearServerStatus(SERVER_STATUS_IN_TRANS)
	backCtx.ClearOptionBits(OPTION_BEGIN)
	return err
}

func (backCtx *backExecCtx) TxnRollback() error {
	var err error
	backCtx.ClearServerStatus(SERVER_STATUS_IN_TRANS | SERVER_STATUS_IN_TRANS_READONLY)
	err = backCtx.txnHandler.RollbackTxn()
	backCtx.ClearOptionBits(OPTION_BEGIN)
	return err
}

func (backCtx *backExecCtx) GetDatabaseName() string {
	return backCtx.proto.GetDatabaseName()
}

func (backCtx *backExecCtx) SetDatabaseName(s string) {
	backCtx.proto.SetDatabaseName(s)
	backCtx.GetTxnCompileCtx().SetDatabase(s)
}

func (backCtx *backExecCtx) ServerStatusIsSet(bit uint16) bool {
	return backCtx.serverStatus&bit != 0
}

func (backCtx *backExecCtx) InMultiStmtTransactionMode() bool {
	return backCtx.OptionBitsIsSet(OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)
}

func (backCtx *backExecCtx) rollbackWholeTxn() error {
	err := backCtx.txnHandler.RollbackTxn()
	backCtx.ClearServerStatus(SERVER_STATUS_IN_TRANS)
	backCtx.ClearOptionBits(OPTION_BEGIN)
	return err
}

func (backCtx *backExecCtx) ClearServerStatus(bit uint16) {
	backCtx.serverStatus &= ^bit
}

func (backCtx *backExecCtx) ClearOptionBits(bit uint32) {
	backCtx.optionBits &= ^bit
}

func (backCtx *backExecCtx) SetOptionBits(bit uint32) {
	backCtx.optionBits |= bit
}

func (backCtx *backExecCtx) SetServerStatus(bit uint16) {
	backCtx.serverStatus |= bit
}

func (backCtx *backExecCtx) OptionBitsIsSet(bit uint32) bool {
	return backCtx.optionBits&bit != 0
}

func (backCtx *backExecCtx) isShareTxn() bool {
	return backCtx.shareTxn
}

func (backCtx *backExecCtx) GetGlobalVar(name string) (interface{}, error) {
	if def, val, ok := backCtx.gSysVars.GetGlobalSysVar(name); ok {
		if def.GetScope() == ScopeSession {
			//empty
			return nil, moerr.NewInternalError(backCtx.requestCtx, errorSystemVariableSessionEmpty())
		}
		return val, nil
	}
	return nil, moerr.NewInternalError(backCtx.requestCtx, errorSystemVariableDoesNotExist())
}

func (backCtx *backExecCtx) SetMysqlResultSetOfBackgroundTask(mrs *MysqlResultSet) {
	if len(backCtx.allResultSet) == 0 {
		backCtx.allResultSet = append(backCtx.allResultSet, mrs)
	}
}

func (backCtx *backExecCtx) SaveResultSet() {
	if len(backCtx.allResultSet) == 0 && backCtx.mrs != nil {
		backCtx.allResultSet = []*MysqlResultSet{backCtx.mrs}
	}
}

func (backCtx *backExecCtx) AppendResultBatch(bat *batch.Batch) error {
	copied, err := bat.Dup(backCtx.pool)
	if err != nil {
		return err
	}
	backCtx.resultBatches = append(backCtx.resultBatches, copied)
	return nil
}

func (backCtx *backExecCtx) ReplaceDerivedStmt(b bool) bool {
	prev := backCtx.derivedStmt
	backCtx.derivedStmt = b
	return prev
}

// execute query
func (mce *MysqlCmdExecutor) doComQueryInBack(requestCtx context.Context,
	backCtx *backExecCtx,
	input *UserInput) (retErr error) {
	backCtx.SetSql(input.getSql())
	//the ses.GetUserName returns the user_name with the account_name.
	//here,we only need the user_name.
	pu := backCtx.pu
	userNameOnly := rootName
	proc := process.New(
		requestCtx,
		backCtx.pool,
		backCtx.txnClient,
		nil,
		pu.FileService,
		pu.LockService,
		pu.QueryService,
		pu.HAKeeperClient,
		pu.UdfService,
		backCtx.autoIncrCacheManager)
	//proc.CopyVectorPool(ses.proc)
	//proc.CopyValueScanBatch(ses.proc)
	proc.Id = mce.getNextProcessId()
	proc.Lim.Size = pu.SV.ProcessLimitationSize
	proc.Lim.BatchRows = pu.SV.ProcessLimitationBatchRows
	proc.Lim.MaxMsgSize = pu.SV.MaxMessageSize
	proc.Lim.PartitionRows = pu.SV.ProcessLimitationPartitionRows
	proc.SessionInfo = process.SessionInfo{
		User:          backCtx.proto.GetUserName(),
		Host:          pu.SV.Host,
		Database:      backCtx.proto.GetDatabaseName(),
		Version:       makeServerVersion(pu, serverVersion.Load().(string)),
		TimeZone:      backCtx.GetTimeZone(),
		StorageEngine: pu.StorageEngine,
		Buf:           backCtx.buf,
	}
	proc.SetStmtProfile(&backCtx.stmtProfile)
	proc.SetResolveVariableFunc(backCtx.txnCompileCtx.ResolveVariable)
	//!!!does not init sequence in the background exec
	if backCtx.tenant != nil {
		proc.SessionInfo.Account = backCtx.tenant.GetTenant()
		proc.SessionInfo.AccountId = backCtx.tenant.GetTenantID()
		proc.SessionInfo.Role = backCtx.tenant.GetDefaultRole()
		proc.SessionInfo.RoleId = backCtx.tenant.GetDefaultRoleID()
		proc.SessionInfo.UserId = backCtx.tenant.GetUserID()

		if len(backCtx.tenant.GetVersion()) != 0 {
			proc.SessionInfo.Version = backCtx.tenant.GetVersion()
		}
		userNameOnly = backCtx.tenant.GetUser()
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
	backCtx.txnCompileCtx.SetProcess(proc)

	cws, err := GetComputationWrapperInBack(backCtx.proto.GetDatabaseName(),
		input,
		backCtx.proto.GetUserName(),
		pu.StorageEngine,
		proc, backCtx)

	if err != nil {
		retErr = err
		if _, ok := err.(*moerr.Error); !ok {
			retErr = moerr.NewParseError(requestCtx, err.Error())
		}
		return retErr
	}

	defer func() {
		backCtx.mrs = nil
	}()

	sqlRecord := parsers.HandleSqlForRecord(input.getSql())

	for i, cw := range cws {
		backCtx.mrs = &MysqlResultSet{}
		stmt := cw.GetAst()
		tenant := backCtx.GetTenantNameWithStmt(stmt)

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
		if backCtx.InActiveTransaction() {
			err = mce.canExecuteStatementInUncommittedTransaction(requestCtx, stmt)
			if err != nil {
				return err
			}
		}

		execCtx := &ExecCtx{
			stmt:       stmt,
			isLastStmt: i >= len(cws)-1,
			tenant:     tenant,
			userName:   userNameOnly,
			sqlOfStmt:  sqlRecord[i],
			cw:         cw,
			i:          i,
			cws:        cws,
			proc:       proc,
		}
		err = mce.executeStmtInBackWithTxn(requestCtx, backCtx, stmt, proc, cw, i, cws, pu, tenant, userNameOnly, sqlRecord[i], execCtx)
		if err != nil {
			return err
		}
	} // end of for

	return nil
}
