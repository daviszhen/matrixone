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
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type backExec struct {
	backCtx *backExecCtx
}

func (back *backExec) Close() {
	back.Clear()
}

func (back *backExec) Exec(ctx context.Context, sql string) error {
	if ctx == nil {
		ctx = back.backCtx.GetRequestContext()
	} else {
		back.backCtx.SetRequestContext(ctx)
	}
	_, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	// For determine this is a background sql.
	ctx = context.WithValue(ctx, defines.BgKey{}, true)
	back.backCtx.requestCtx = ctx
	//logutil.Debugf("-->bh:%s", sql)
	v, err := back.backCtx.GetGlobalVar("lower_case_table_names")
	if err != nil {
		return err
	}
	statements, err := mysql.Parse(ctx, sql, v.(int64))
	if err != nil {
		return err
	}
	if len(statements) > 1 {
		return moerr.NewInternalError(ctx, "Exec() can run one statement at one time. but get '%d' statements now, sql = %s", len(statements), sql)
	}
	//share txn can not run transaction statement
	if back.backCtx.isShareTxn() {
		for _, stmt := range statements {
			switch stmt.(type) {
			case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
				return moerr.NewInternalError(ctx, "Exec() can not run transaction statement in share transaction, sql = %s", sql)
			}
		}
	}
	return doComQueryInBack(ctx, back.backCtx, &UserInput{sql: sql})
}

func (back *backExec) ExecStmt(ctx context.Context, statement tree.Statement) error {
	return nil
}

func (back *backExec) GetExecResultSet() []interface{} {
	mrs := back.backCtx.allResultSet
	ret := make([]interface{}, len(mrs))
	for i, mr := range mrs {
		ret[i] = mr
	}
	return ret
}

func (back *backExec) ClearExecResultSet() {
	back.backCtx.allResultSet = nil
}

func (back *backExec) GetExecResultBatches() []*batch.Batch {
	return back.backCtx.resultBatches
}

func (back *backExec) ClearExecResultBatches() {
	back.backCtx.resultBatches = nil
}

func (back *backExec) Clear() {
	back.backCtx.clear()
}

// execute query
func doComQueryInBack(requestCtx context.Context,
	backCtx *backExecCtx,
	input *UserInput) (retErr error) {
	backCtx.SetSql(input.getSql())
	//the ses.GetUserName returns the user_name with the account_name.
	//here,we only need the user_name.
	userNameOnly := rootName
	proc := process.New(
		requestCtx,
		backCtx.pool,
		gPu.TxnClient,
		nil,
		gPu.FileService,
		gPu.LockService,
		gPu.QueryService,
		gPu.HAKeeperClient,
		gPu.UdfService,
		gAicm)
	//proc.CopyVectorPool(ses.proc)
	//proc.CopyValueScanBatch(ses.proc)
	proc.Id = backCtx.getNextProcessId()
	proc.Lim.Size = gPu.SV.ProcessLimitationSize
	proc.Lim.BatchRows = gPu.SV.ProcessLimitationBatchRows
	proc.Lim.MaxMsgSize = gPu.SV.MaxMessageSize
	proc.Lim.PartitionRows = gPu.SV.ProcessLimitationPartitionRows
	proc.SessionInfo = process.SessionInfo{
		User:          backCtx.proto.GetUserName(),
		Host:          gPu.SV.Host,
		Database:      backCtx.proto.GetDatabaseName(),
		Version:       makeServerVersion(gPu, serverVersion.Load().(string)),
		TimeZone:      backCtx.GetTimeZone(),
		StorageEngine: gPu.StorageEngine,
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
		gPu.StorageEngine,
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
			err = canExecuteStatementInUncommittedTransaction(requestCtx, backCtx, stmt)
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
			proc:       proc,
		}
		err = executeStmtInBackWithTxn(requestCtx, backCtx, execCtx)
		if err != nil {
			return err
		}
	} // end of for

	return nil
}

func executeStmtInBackWithTxn(requestCtx context.Context,
	backCtx *backExecCtx,
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
	return executeStmtInBack(requestCtx, backCtx, execCtx)
}

func executeStmtInBack(requestCtx context.Context,
	backCtx *backExecCtx,
	execCtx *ExecCtx,
) (err error) {
	var cmpBegin time.Time
	var ret interface{}

	switch execCtx.stmt.HandleType() {
	case tree.InFrontend:
		return handleInFrontendInBack(requestCtx, backCtx, execCtx)
	case tree.InBackend:
	case tree.Unknown:
		return moerr.NewInternalError(requestCtx, "backExec needs set handle type for %s", execCtx.sqlOfStmt)
	}

	switch st := execCtx.stmt.(type) {
	case *tree.CreateDatabase:
		err = inputNameIsInvalid(execCtx.proc.Ctx, string(st.Name))
		if err != nil {
			return
		}
		if st.SubscriptionOption != nil && backCtx.tenant != nil && !backCtx.tenant.IsAdminRole() {
			err = moerr.NewInternalError(execCtx.proc.Ctx, "only admin can create subscription")
			return
		}
		st.Sql = execCtx.sqlOfStmt
	case *tree.DropDatabase:
		err = inputNameIsInvalid(execCtx.proc.Ctx, string(st.Name))
		if err != nil {
			return
		}
		// if the droped database is the same as the one in use, database must be reseted to empty.
		if string(st.Name) == backCtx.GetDatabaseName() {
			backCtx.SetDatabaseName("")
		}
	}

	cmpBegin = time.Now()

	if ret, err = execCtx.cw.Compile(requestCtx, backCtx, backCtx.outputCallback); err != nil {
		return
	}

	// cw.Compile may rewrite the stmt in the EXECUTE statement, we fetch the latest version
	//need to check again.
	execCtx.stmt = execCtx.cw.GetAst()
	switch execCtx.stmt.HandleType() {
	case tree.InFrontend:
		return handleInFrontendInBack(requestCtx, backCtx, execCtx)
	case tree.InBackend:
	case tree.Unknown:
		return moerr.NewInternalError(requestCtx, "backExec needs set handle type for %s", execCtx.sqlOfStmt)
	}

	execCtx.runner = ret.(ComputationRunner)

	// only log if build time is longer than 1s
	if time.Since(cmpBegin) > time.Second {
		//logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Build : %s", time.Since(cmpBegin).String()))
	}

	resultType := execCtx.stmt.ResultType()
	switch resultType {
	case tree.ResultRow:
		err = executeResultRowStmtInBack(requestCtx, backCtx, execCtx)
		if err != nil {
			return err
		}
	case tree.Status:
		err = executeStatusStmtInBack(requestCtx, backCtx, execCtx)
		if err != nil {
			return err
		}
	case tree.NoResp:
	case tree.RespItself:
	case tree.Undefined:
		isExecute := false
		switch execCtx.stmt.(type) {
		case *tree.Execute:
			isExecute = true
		}
		if !isExecute {
			return moerr.NewInternalError(requestCtx, "need set result type for %s", execCtx.sqlOfStmt)
		}
	}

	return
}

var GetComputationWrapperInBack = func(db string, input *UserInput, user string, eng engine.Engine, proc *process.Process, ses TempInter) ([]ComputationWrapper, error) {
	var cw []ComputationWrapper = nil

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

var NewBackgroundExec = func(
	reqCtx context.Context,
	upstream TempInter,
	mp *mpool.MPool) BackgroundExec {
	txnHandler := InitTxnHandler(gPu.StorageEngine, nil, nil)
	backCtx := &backExecCtx{
		requestCtx:     reqCtx,
		connectCtx:     upstream.GetConnectContext(),
		pool:           mp,
		proto:          &FakeProtocol{},
		buf:            buffer.New(),
		stmtProfile:    process.StmtProfile{},
		tenant:         nil,
		txnHandler:     txnHandler,
		txnCompileCtx:  InitTxnCompilerContext(txnHandler, ""),
		mrs:            nil,
		outputCallback: fakeDataSetFetcher2,
		allResultSet:   nil,
		resultBatches:  nil,
		serverStatus:   0,
		derivedStmt:    false,
		optionBits:     0,
		shareTxn:       false,
		gSysVars:       GSysVariables,
		label:          make(map[string]string),
		timeZone:       time.Local,
	}
	backCtx.GetTxnCompileCtx().SetSession(backCtx)
	backCtx.GetTxnHandler().SetSession(backCtx)
	//var accountId uint32
	//var err error
	//accountId, err = defines.GetAccountId(reqCtx)
	//if err != nil {
	//	panic(err)
	//}
	//backCtx.tenant = &TenantInfo{
	//	TenantID:      accountId,
	//	UserID:        defines.GetUserId(reqCtx),
	//	DefaultRoleID: defines.GetRoleId(reqCtx),
	//}

	bh := &backExec{
		backCtx: backCtx,
	}

	return bh
}

// executeSQLInBackgroundSession executes the sql in an independent session and transaction.
// It sends nothing to the client.
func executeSQLInBackgroundSession(reqCtx context.Context, upstream *Session, mp *mpool.MPool, sql string) ([]ExecResult, error) {
	bh := NewBackgroundExec(reqCtx, upstream, mp)
	defer bh.Close()
	logutil.Debugf("background exec sql:%v", sql)
	err := bh.Exec(reqCtx, sql)
	logutil.Debugf("background exec sql done")
	if err != nil {
		return nil, err
	}
	return getResultSet(reqCtx, bh)
}

// executeStmtInSameSession executes the statement in the same session.
// To be clear, only for the select statement derived from the set_var statement
// in an independent transaction
func executeStmtInSameSession(ctx context.Context, ses *Session, stmt tree.Statement) error {
	switch stmt.(type) {
	case *tree.Select, *tree.ParenSelect:
	default:
		return moerr.NewInternalError(ctx, "executeStmtInSameSession can not run non select statement in the same session")
	}

	prevDB := ses.GetDatabaseName()
	prevOptionBits := ses.GetOptionBits()
	prevServerStatus := ses.GetServerStatus()
	//autocommit = on
	ses.setAutocommitOn()
	//1. replace output callback by batchFetcher.
	// the result batch will be saved in the session.
	// you can get the result batch by calling GetResultBatches()
	ses.SetOutputCallback(batchFetcher)
	//2. replace protocol by FakeProtocol.
	// Any response yielded during running query will be dropped by the FakeProtocol.
	// The client will not receive any response from the FakeProtocol.
	prevProto := ses.ReplaceProtocol(&FakeProtocol{})
	// inherit database
	ses.SetDatabaseName(prevDB)
	proc := ses.GetTxnCompileCtx().GetProcess()
	//restore normal protocol and output callback
	defer func() {
		//@todo we need to improve: make one session, one proc, one txnOperator
		p := ses.GetTxnCompileCtx().GetProcess()
		p.FreeVectors()
		ses.GetTxnCompileCtx().SetProcess(proc)
		ses.SetOptionBits(prevOptionBits)
		ses.SetServerStatus(prevServerStatus)
		ses.SetOutputCallback(getDataFromPipeline)
		ses.ReplaceProtocol(prevProto)
	}()
	logDebug(ses, ses.GetDebugString(), "query trace(ExecStmtInSameSession)",
		logutil.ConnectionIdField(ses.GetConnectionID()))
	//3. execute the statement
	return doComQuery(ctx, ses, &UserInput{stmt: stmt})
}

// fakeDataSetFetcher2 gets the result set from the pipeline and save it in the session.
// It will not send the result to the client.
func fakeDataSetFetcher2(handle interface{}, dataSet *batch.Batch) error {
	if handle == nil || dataSet == nil {
		return nil
	}

	back := handle.(*backExecCtx)
	oq := newFakeOutputQueue(back.mrs)
	err := fillResultSet(oq, dataSet, back)
	if err != nil {
		return err
	}
	back.SetMysqlResultSetOfBackgroundTask(back.mrs)
	return nil
}

func fillResultSet(oq outputPool, dataSet *batch.Batch, ses TempInter) error {
	n := dataSet.RowCount()
	for j := 0; j < n; j++ { //row index
		//needCopyBytes = true. we need to copy the bytes from the batch.Batch
		//to avoid the data being changed after the batch.Batch returned to the
		//pipeline.
		_, err := extractRowFromEveryVector(ses, dataSet, j, oq, true)
		if err != nil {
			return err
		}
	}
	return oq.flush()
}

// batchFetcher2 gets the result batches from the pipeline and save the origin batches in the session.
// It will not send the result to the client.
func batchFetcher2(handle interface{}, dataSet *batch.Batch) error {
	if handle == nil {
		return nil
	}
	back := handle.(*backExecCtx)
	back.SaveResultSet()
	if dataSet == nil {
		return nil
	}
	return back.AppendResultBatch(dataSet)
}

// batchFetcher gets the result batches from the pipeline and save the origin batches in the session.
// It will not send the result to the client.
func batchFetcher(handle interface{}, dataSet *batch.Batch) error {
	if handle == nil {
		return nil
	}
	ses := handle.(*Session)
	ses.SaveResultSet()
	if dataSet == nil {
		return nil
	}
	return ses.AppendResultBatch(dataSet)
}

// getResultSet extracts the result set
func getResultSet(ctx context.Context, bh BackgroundExec) ([]ExecResult, error) {
	results := bh.GetExecResultSet()
	rsset := make([]ExecResult, len(results))
	for i, value := range results {
		if er, ok := value.(ExecResult); ok {
			rsset[i] = er
		} else {
			return nil, moerr.NewInternalError(ctx, "it is not the type of result set")
		}
	}
	return rsset, nil
}

type backExecCtx struct {
	requestCtx    context.Context
	connectCtx    context.Context
	pool          *mpool.MPool
	proto         MysqlProtocol
	buf           *buffer.Buffer
	stmtProfile   process.StmtProfile
	tenant        *TenantInfo
	txnHandler    *TxnHandler
	txnCompileCtx *TxnCompilerContext
	mrs           *MysqlResultSet
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

	sqlCount uint64
}

func (backCtx *backExecCtx) getNextProcessId() string {
	/*
		temporary method:
		routineId + sqlCount
	*/
	routineId := backCtx.GetMysqlProtocol().ConnectionID()
	return fmt.Sprintf("%d%d", routineId, backCtx.GetSqlCount())
}

func (backCtx *backExecCtx) GetSqlCount() uint64 {
	return backCtx.sqlCount
}

func (backCtx *backExecCtx) addSqlCount(a uint64) {
	backCtx.sqlCount += a
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

func (backCtx *backExecCtx) SetNewResponse(category int, affectedRows uint64, cmd int, d interface{}, isLastStmt bool) *Response {
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
		backCtx.GetMemPool())
}

func (backCtx *backExecCtx) GetStorage() engine.Engine {
	return gPu.StorageEngine
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
	backCtx.pool = nil
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

type SqlHelper struct {
	ses *Session
}

func (sh *SqlHelper) GetCompilerContext() any {
	return sh.ses.txnCompileCtx
}

// Made for sequence func. nextval, setval.
func (sh *SqlHelper) ExecSql(sql string) (ret []interface{}, err error) {
	var erArray []ExecResult

	ctx := sh.ses.GetRequestContext()
	/*
		if we run the transaction statement (BEGIN, ect) here , it creates an independent transaction.
		if we do not run the transaction statement (BEGIN, ect) here, it runs the sql in the share transaction
		and committed outside this function.
		!!!NOTE: wen can not execute the transaction statement(BEGIN,COMMIT,ROLLBACK,START TRANSACTION ect) here.
	*/
	bh := sh.ses.GetShareTxnBackgroundExec(ctx, false)
	defer bh.Close()

	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	if len(erArray) == 0 {
		return nil, nil
	}

	return erArray[0].(*MysqlResultSet).Data[0], nil
}
