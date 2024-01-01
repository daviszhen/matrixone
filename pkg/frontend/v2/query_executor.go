// Copyright 2023 Matrix Origin
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

package v2

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/memoryengine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/mohae/deepcopy"
)

func (ge *GeneralExecutor) Open(ctx context.Context, opts ...QueryExecutorOpt) error {
	for _, opt := range opts {
		opt(&ge.opts)
	}
	return nil
}
func (ge *GeneralExecutor) Exec(requestCtx context.Context, input *UserInput) (retErr error) {
	// set the batch buf for stream scan
	var inMemStreamScan []*kafka.Message

	if batchValue, ok := requestCtx.Value(defines.SourceScanResKey{}).([]*kafka.Message); ok {
		inMemStreamScan = batchValue
	}

	beginInstant := time.Now()
	requestCtx = appendStatementAt(requestCtx, beginInstant)

	ses := ge.opts.conn.ses
	input.genSqlSourceType(ses)
	ses.SetShowStmtType(NotShowStatement)
	ses.SetSql(input.getSql())
	//the ses.GetUserName returns the user_name with the account_name.
	//here,we only need the user_name.
	userNameOnly := rootName

	proc := process.New(
		requestCtx,
		ses.GetMemPool(),
		feTxnClient,
		nil,
		fePu.FileService,
		fePu.LockService,
		fePu.QueryService,
		fePu.HAKeeperClient,
		fePu.UdfService,
		feAicm)
	proc.CopyVectorPool(ses.proc)
	proc.CopyValueScanBatch(ses.proc)
	proc.Id = ge.opts.conn.getNextProcessId()
	proc.Lim.Size = fePu.SV.ProcessLimitationSize
	proc.Lim.BatchRows = fePu.SV.ProcessLimitationBatchRows
	proc.Lim.MaxMsgSize = fePu.SV.MaxMessageSize
	proc.Lim.PartitionRows = fePu.SV.ProcessLimitationPartitionRows
	proc.SessionInfo = process.SessionInfo{
		User:                 ses.GetUserName(),
		Host:                 fePu.SV.Host,
		ConnectionID:         uint64(ge.opts.conn.connId),
		Database:             ses.GetDatabaseName(),
		Version:              makeServerVersion(fePu, serverVersion.Load().(string)),
		TimeZone:             ses.GetTimeZone(),
		StorageEngine:        fePu.StorageEngine,
		LastInsertID:         ses.GetLastInsertID(),
		SqlHelper:            ses.GetSqlHelper(),
		Buf:                  ses.GetBuffer(),
		SourceInMemScanBatch: inMemStreamScan,
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
		proc.SessionInfo.Account = sysAccountName
		proc.SessionInfo.AccountId = sysAccountID
		proc.SessionInfo.RoleId = moAdminRoleID
		proc.SessionInfo.UserId = rootID
	}
	var span trace.Span
	requestCtx, span = trace.Start(requestCtx, "MysqlCmdExecutor.doComQuery",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End()

	proc.SessionInfo.User = userNameOnly
	proc.SessionInfo.QueryId = ses.getQueryId(input.isInternal())
	ses.txnCompileCtx.SetProcess(ses.proc)
	ses.proc.SessionInfo = proc.SessionInfo

	statsInfo := statistic.StatsInfo{ParseStartTime: beginInstant}
	requestCtx = statistic.ContextWithStatsInfo(requestCtx, &statsInfo)

	//1. parse sql

	//2. check privilege on the statement level

	//3. check if the statement is in an active transaction

	//4. execute the statement

	stmts, err := parseSql(requestCtx, ses.GetDatabaseName(), input, ses.GetUserName(), ses)

	ParseDuration := time.Since(beginInstant)

	if err != nil {
		statsInfo.ParseDuration = ParseDuration
		requestCtx = RecordParseErrorStatement(requestCtx, ses, proc, beginInstant, parsers.HandleSqlForRecord(input.getSql()), input.getSqlSourceTypes(), err)
		retErr = err
		if _, ok := err.(*moerr.Error); !ok {
			retErr = moerr.NewParseError(requestCtx, err.Error())
		}
		logStatementStringStatus(requestCtx, ses, input.getSql(), fail, retErr)
		return retErr
	}

	return ge.executeStmts(requestCtx, ses, userNameOnly, input, proc, stmts, beginInstant, ParseDuration, &statsInfo)
}

func (ge *GeneralExecutor) executeStmts(requestCtx context.Context,
	ses *Session,
	userNameOnly string,
	input *UserInput,
	proc *process.Process,
	stmts []*Stmt,
	beginInstant time.Time,
	ParseDuration time.Duration,
	statsInfo *statistic.StatsInfo) (err error) {
	canCache := true

	singleStatement := len(stmts) == 1
	sqlRecord := parsers.HandleSqlForRecord(input.getSql())

	for i, stmt := range stmts {
		if stmt.stmt.GetQueryType() == tree.QueryTypeDDL || stmt.stmt.GetQueryType() == tree.QueryTypeDCL ||
			stmt.stmt.GetQueryType() == tree.QueryTypeOth ||
			stmt.stmt.GetQueryType() == tree.QueryTypeTCL {
			if _, ok := stmt.stmt.(*tree.SetVar); !ok {
				ses.cleanCache()
			}
			canCache = false
		}

		ses.sentRows.Store(int64(0))
		ses.writeCsvBytes.Store(int64(0))
		// proto.ResetStatistics() // move from getDataFromPipeline, for record column fields' data
		sqlType := input.getSqlSourceType(i)
		requestCtx = RecordStatement(requestCtx, ses, proc, stmt, beginInstant, sqlRecord[i], sqlType, singleStatement)

		statsInfo.Reset()
		//average parse duration
		statsInfo.ParseDuration = time.Duration(ParseDuration.Nanoseconds() / int64(len(stmts)))

		err = ge.executeSingleStmt(requestCtx, ses, userNameOnly, input, proc, stmts, i, sqlRecord[i], beginInstant, ParseDuration, statsInfo)
		if err != nil {
			return err
		}
	} // end of for

	if canCache && !ses.isCached(input.getSql()) {
		plans := make([]*plan.Plan, len(stmts))
		asts := make([]tree.Statement, len(stmts))
		for i, stmt := range stmts {
			if checkNodeCanCache(stmt.plan) {
				plans[i] = stmt.plan
				asts[i] = stmt.stmt
			} else {
				return nil
			}
		}
		ses.cachePlan(input.getSql(), asts, plans)
	}
	return
}

func (ge *GeneralExecutor) executeSingleStmt(requestCtx context.Context,
	ses *Session,
	userNameOnly string,
	input *UserInput,
	proc *process.Process,
	stmts []*Stmt,
	i int,
	sqlOfStmt string,
	beginInstant time.Time,
	ParseDuration time.Duration,
	statsInfo *statistic.StatsInfo) (err error) {
	stmt := stmts[i]
	tenant := ses.GetTenantNameWithStmt(stmt.stmt)

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
			stmt.stmt.Format(formatCtx)
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

	//======= statement level privilege check =======
	//skip PREPARE statement here
	if ses.GetTenantInfo() != nil && !IsPrepareStatement(stmt.stmt) {
		err = authenticateUserCanExecuteStatement(requestCtx, ses, stmt.stmt)
		if err != nil {
			logStatementStatus(requestCtx, ses, stmt.stmt, fail, err)
			return err
		}
	}

	//======= setup statement executor =======
	exec, err := prepareExecutor(requestCtx, ses, stmt.stmt)
	if err != nil {
		return err
	}
	eo := &ExecutorOptions{
		ses:        ses,
		proc:       proc,
		stmt:       stmt,
		endPoint:   ge.opts.endPoint,
		isLastStmt: i >= len(stmts)-1,
	}
	err = exec.Open(requestCtx, eo)
	if err != nil {
		return err
	}
	defer func() {
		_ = exec.Close(requestCtx)
	}()

	//======= pre check & prepare transaction =======
	label := exec.Label()
	if ses.txn.InActiveTransaction() {
		err = canExecuteStatementInUncommittedTransaction(requestCtx, label, stmt.stmt)
		if err != nil {
			logStatementStatus(requestCtx, ses, stmt.stmt, fail, err)
			return err
		}
	}

	if label&CommitTxnBeforeExec != 0 {
		//do not txn
		err = ses.txn.CommitTxn()
		if err != nil {
			return err
		}
		//no txn anymore
		if ses.txn.InActiveTransaction() {
			panic("txn need to be committed")
		}
	} else {
		//need txn
		if !ses.txn.InActiveTransaction() {
			_, _, err = ses.txn.NewTxn()
			if err != nil {
				return err
			}
		}
	}

	if label&SkipStmt == 0 {
		err = ses.txn.startStmt()
		if err != nil {
			return err
		}
	}

	//cleanup transaction
	defer func() {
		err = cleanupTxn(requestCtx, ses, tenant, stmt.stmt, label, err)
	}()

	// update UnixTime for new query, which is used for now() / CURRENT_TIMESTAMP
	proc.UnixTime = time.Now().UnixNano()
	if ses.proc != nil {
		ses.proc.UnixTime = proc.UnixTime
	}

	//======= execute statement =======
	err = ge.runExec(requestCtx, exec, eo)
	if err != nil {
		return err
	}

	//======= post check & cleanup transaction =======
	if label&TxnExistsAferExc != 0 {
		if !ses.txn.IsValidTxnOperator() {
			return moerr.NewInternalError(requestCtx, "need active transaction")
		}
	} else if label&TxnDisappearsAferExec != 0 {
		if ses.txn.IsValidTxnOperator() {
			return moerr.NewInternalError(requestCtx, "do not need active transaction")
		}
	}

	return err
}

// cleanup the transaction
func cleanupTxn(requestCtx context.Context,
	ses *Session, tenant string,
	stmt tree.Statement,
	label Label,
	execErr error) (err error) {
	// First recover all panics.   If paniced, we will abort.
	if r := recover(); r != nil {
		err = moerr.ConvertPanicError(requestCtx, r)
	}

	if execErr == nil {
		if label&SkipStmt == 0 {
			ses.txn.endStmt()
		}
		execErr = commitTxn(requestCtx, ses, tenant, stmt, label)
		if execErr == nil {
			return nil
		}
		// if commitTxnFunc failed, we will rollback the transaction.
	}

	if label&SkipStmt == 0 {
		_ = ses.txn.rollbackStmt() //?
	}
	return rollbackTxn(requestCtx, ses, tenant, stmt, execErr)
}

// execution succeeds during the transaction. commit the transaction
func commitTxn(requestCtx context.Context, ses *Session, tenant string, stmt tree.Statement, label Label) (retErr error) {
	// Call a defer function -- if it is paniced, we
	// want to catch it and convert it to an error.
	defer func() {
		if r := recover(); r != nil {
			retErr = moerr.ConvertPanicError(requestCtx, r)
		}
	}()

	//load data handle txn failure internally
	incStatementCounter(tenant, stmt)
	cond := func() bool {
		return !ses.txn.optionBitsIsSetUnsafe(OPTION_BEGIN) || label&CommitTxnAfterExec != 0
	}

	retErr = ses.txn.CommitTxnCond(cond)
	if retErr != nil {
		logStatementStatus(requestCtx, ses, stmt, fail, retErr)
	}
	return
}

// get errors during the transaction. rollback the transaction
func rollbackTxn(requestCtx context.Context,
	ses *Session,
	tenant string,
	stmt tree.Statement,
	execErr error) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = moerr.ConvertPanicError(requestCtx, r)
		}
	}()
	incStatementCounter(tenant, stmt)
	incStatementErrorsCounter(tenant, stmt)

	logError(ses, ses.GetDebugString(), execErr.Error())
	retErr = ses.txn.RollbackTxn()
	if retErr != nil {
		logStatementStatus(requestCtx, ses, stmt, fail, retErr)
		return retErr
	}
	logStatementStatus(requestCtx, ses, stmt, fail, execErr)
	return execErr
}

func (ge *GeneralExecutor) runExec(requestCtx context.Context, exec Executor, eo *ExecutorOptions) (err error) {
	err = exec.Next(requestCtx, eo)
	if err != nil {
		return err
	}
	return err
}

// for most sql
func runStmt(requestCtx context.Context, opts *ExecutorOptions) error {
	//====== compile stmt ======
	cmp, err := compileStmt(requestCtx, opts.ses, opts.proc, opts.stmt, getDataFromPipeline2)
	if err != nil {
		return err
	}
	cols, err := getColumns(opts.ses, opts.stmt)
	if err != nil {
		return err
	}

	//====== init or update format writer ======
	colDef := make([]*MysqlColumn, 0)
	for _, col := range cols {
		mysqlc := col.(*MysqlColumn)
		colDef = append(colDef, mysqlc)
	}
	if opts.ses.formatWriter == nil {
		opts.ses.formatWriter = &MysqlFormatWriter{
			ChunksWriter: &ChunksWriter{
				ses:      opts.ses,
				endPoint: opts.endPoint,
			},
		}
		opts.ses.formatWriter.Open(requestCtx)
	}
	opts.ses.formatWriter.colDef = colDef
	opts.ses.formatWriter.row = make([]any, len(colDef))
	//====== send the column info to the client ======
	//send column count
	colCountPacket := LengthEncodedNumber{}
	defer colCountPacket.Close(requestCtx)
	colCountPacket.Open(requestCtx, WithNumber(uint64(len(cols))))
	err = opts.endPoint.SendPacket(requestCtx, &colCountPacket, true)
	if err != nil {
		return err
	}

	//send column defs
	for _, col := range cols {
		mysqlc := col.(*MysqlColumn)
		// fmt.Fprintf(os.Stderr, "==> %v %v\n", mysqlc.table, mysqlc.name)
		cd := ColumnDefinition{}
		defer cd.Close(requestCtx)
		err = cd.Open(requestCtx,
			WithCapability(opts.ses.conn.capability),
			WithColumn(mysqlc),
			WithCmd(int(COM_QUERY))) // not CMD_FILED_LIST
		if err != nil {
			return err
		}
		err = opts.endPoint.SendPacket(requestCtx, &cd, true)
		if err != nil {
			return err
		}
	}

	//send EOFIf
	eofif := EOFPacketIf{}
	defer eofif.Close(requestCtx)
	err = eofif.Open(requestCtx, WithCapability(opts.ses.conn.capability))
	if err != nil {
		return err
	}
	err = opts.endPoint.SendPacket(requestCtx, &eofif, true)
	if err != nil {
		return err
	}

	//run the computations
	runner := cmp.(ComputationRunner)
	runResult, err := runner.Run(0)
	opts.stmt.runResult = runResult
	opts.stmt.compile = nil
	if err != nil {
		return err
	}
	//====== repsonse client ======
	eofok := EOFOrOkPacket{}
	defer eofok.Close(requestCtx)
	err = eofok.Open(requestCtx,
		WithCapability(opts.ses.conn.capability),
		WithStatus(adjustServerStatus(opts.ses.txn.GetServerStatus(), opts.isLastStmt)),
	)
	if err != nil {
		return err
	}
	err = opts.endPoint.SendPacket(requestCtx, &eofok, true)
	if err != nil {
		return err
	}
	return err
}

func getColumns(ses *Session, stmt *Stmt) ([]interface{}, error) {
	var err error
	cols := plan2.GetResultColumnsFromPlan(stmt.plan)
	switch stmt.stmt.(type) {
	case *tree.ShowColumns:
		if len(cols) == 7 {
			cols = []*plan2.ColDef{
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Field"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Type"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Null"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Key"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Default"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Extra"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Comment"},
			}
		} else {
			cols = []*plan2.ColDef{
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Field"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Type"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Collation"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Null"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Key"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Default"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Extra"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Privileges"},
				{Typ: &plan2.Type{Id: int32(types.T_char)}, Name: "Comment"},
			}
		}
	}
	columns := make([]interface{}, len(cols))
	for i, col := range cols {
		c := new(MysqlColumn)
		c.SetName(col.Name)
		c.SetOrgName(col.Name)
		c.SetTable(col.Typ.Table)
		c.SetOrgTable(col.Typ.Table)
		c.SetAutoIncr(col.Typ.AutoIncr)
		c.SetSchema(ses.GetTxnCompileCtx().DefaultDatabase())
		err = convertEngineTypeToMysqlType(ses.GetRequestContext(), types.T(col.Typ.Id), c)
		if err != nil {
			return nil, err
		}
		setColFlag(c)
		setColLength(c, col.Typ.Width)
		setCharacter(c)

		// For binary/varbinary with mysql_type_varchar.Change the charset.
		if types.T(col.Typ.Id) == types.T_binary || types.T(col.Typ.Id) == types.T_varbinary {
			c.SetCharset(0x3f)
		}

		c.SetDecimal(col.Typ.Scale)
		convertMysqlTextTypeToBlobType(c)
		columns[i] = c
	}
	return columns, err
}

func compileStmt(requestCtx context.Context, ses *Session, proc *process.Process, stmt *Stmt, fill func(interface{}, *batch.Batch) error) (interface{}, error) {
	var span trace.Span
	requestCtx, span = trace.Start(requestCtx, "TxnComputationWrapper.Compile",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End(trace.WithStatementExtra(ses.GetTxnId(), ses.GetStmtId(), ses.GetSqlOfStmt()))

	stats := statistic.StatsInfoFromContext(requestCtx)
	stats.CompileStart()
	defer stats.CompileEnd()

	var err error
	defer RecordStatementTxnID(requestCtx, ses)
	if ses.IfInitedTempEngine() {
		requestCtx = context.WithValue(requestCtx, defines.TemporaryTN{}, ses.GetTempTableStorage())
		proc.Ctx = context.WithValue(proc.Ctx, defines.TemporaryTN{}, ses.GetTempTableStorage())
		ses.GetTxnHandler().AttachTempStorageToTxnCtx()
	}

	txnHandler := ses.GetTxnHandler()
	var txnCtx context.Context
	txnCtx, proc.TxnOperator = txnHandler.GetTxnOperator()
	txnCtx = fileservice.EnsureStatementProfiler(txnCtx, requestCtx)
	txnCtx = statistic.EnsureStatsInfoCanBeFound(txnCtx, requestCtx)

	// Increase the statement ID and update snapshot TS before build plan, because the
	// snapshot TS is used when build plan.
	// NB: In internal executor, we should also do the same action, which is increasing
	// statement ID and updating snapshot TS.
	// See `func (exec *txnExecutor) Exec(sql string)` for details.
	// txnOp := proc.TxnOperator
	// ses.SetTxnId(txnOp.Txn().ID)
	// if txnOp != nil && !ses.IsDerivedStmt() {
	// 	ok, _ := ses.GetTxnHandler().calledStartStmt()
	// 	if !ok {
	// 		txnOp.GetWorkspace().StartStatement()
	// 		ses.GetTxnHandler().enableStartStmt(txnOp.Txn().ID)
	// 	}

	// 	err = txnOp.GetWorkspace().IncrStatementID(requestCtx, false)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }

	cacheHit := stmt.plan != nil
	if !cacheHit {
		stmt.plan, err = buildPlan(requestCtx, ses, ses.GetTxnCompileCtx(), stmt.stmt)
	} else if ses != nil && ses.GetTenantInfo() != nil {
		ses.accountId = defines.GetAccountId(requestCtx)
		err = authenticateCanExecuteStatementAndPlan(requestCtx, ses, stmt.stmt, stmt.plan)
	}
	if err != nil {
		return nil, err
	}
	ses.p = stmt.plan
	if ids := isResultQuery(stmt.plan); ids != nil {
		if err = checkPrivilege(ids, requestCtx, ses); err != nil {
			return nil, err
		}
	}
	if _, ok := stmt.stmt.(*tree.Execute); ok {
		executePlan := stmt.plan.GetDcl().GetExecute()
		stmtName := executePlan.GetName()
		prepareStmt, err := ses.GetPrepareStmt(stmtName)
		if err != nil {
			return nil, err
		}
		preparePlan := prepareStmt.PreparePlan.GetDcl().GetPrepare()

		// TODO check if schema change, obj.Obj is zero all the time in 0.6
		for _, obj := range preparePlan.GetSchemas() {
			newObj, newTableDef := ses.txnCompileCtx.Resolve(obj.SchemaName, obj.ObjName)
			if newObj == nil {
				return nil, moerr.NewInternalError(requestCtx, "table '%s' in prepare statement '%s' does not exist anymore", obj.ObjName, stmtName)
			}
			if newObj.Obj != obj.Obj || newTableDef.Version != uint32(obj.Server) {
				return nil, moerr.NewInternalError(requestCtx, "table '%s' has been changed, please reset prepare statement '%s'", obj.ObjName, stmtName)
			}
		}

		// The default count is 1. Setting it to 2 ensures that memory will not be reclaimed.
		//  Convenient to reuse memory next time
		if prepareStmt.InsertBat != nil {
			prepareStmt.InsertBat.SetCnt(1000) //we will make sure :  when retry in lock error, we will not clean up this batch
			proc.SetPrepareBatch(prepareStmt.InsertBat)
			proc.SetPrepareExprList(prepareStmt.exprList)
		}
		numParams := len(preparePlan.ParamTypes)
		if prepareStmt.params != nil && prepareStmt.params.Length() > 0 { //use binary protocol
			if prepareStmt.params.Length() != numParams {
				return nil, moerr.NewInvalidInput(requestCtx, "Incorrect arguments to EXECUTE")
			}
			proc.SetPrepareParams(prepareStmt.params)
		} else if len(executePlan.Args) > 0 {
			if len(executePlan.Args) != numParams {
				return nil, moerr.NewInvalidInput(requestCtx, "Incorrect arguments to EXECUTE")
			}
			params := proc.GetVector(types.T_text.ToType())
			for _, arg := range executePlan.Args {
				exprImpl := arg.Expr.(*plan.Expr_V)
				param, err := proc.GetResolveVariableFunc()(exprImpl.V.Name, exprImpl.V.System, exprImpl.V.Global)
				if err != nil {
					return nil, err
				}
				if param == nil {
					return nil, moerr.NewInvalidInput(requestCtx, "Incorrect arguments to EXECUTE")
				}
				err = util.AppendAnyToStringVector(proc, param, params)
				if err != nil {
					return nil, err
				}
			}
			proc.SetPrepareParams(params)
		} else {
			if numParams > 0 {
				return nil, moerr.NewInvalidInput(requestCtx, "Incorrect arguments to EXECUTE")
			}
		}

		stmt.plan = preparePlan.Plan

		// reset plan & stmt
		stmt.stmt = prepareStmt.PrepareStmt
		// reset some special stmt for execute statement
		switch stmt.stmt.(type) {
		case *tree.ShowTableStatus:
			ses.showStmtType = ShowTableStatus
			ses.SetData(nil)
		case *tree.SetVar, *tree.ShowVariables, *tree.ShowErrors, *tree.ShowWarnings:
			return nil, nil
		}

		//check privilege
		/* prepare not need check privilege
		   err = authenticateUserCanExecutePrepareOrExecute(requestCtx, ses, prepareStmt.PrepareStmt, newPlan)
		   if err != nil {
		   	return nil, err
		   }
		*/
	}

	addr := ""
	if len(fePu.ClusterNodes) > 0 {
		addr = fePu.ClusterNodes[0].Addr
	}
	proc.Ctx = txnCtx
	proc.FileService = fePu.FileService

	var tenant string
	tInfo := ses.GetTenantInfo()
	if tInfo != nil {
		tenant = tInfo.GetTenant()
	}
	stmt.compile = compile.NewCompile(
		addr,
		ses.GetDatabaseName(),
		ses.GetSql(),
		tenant,
		ses.GetUserName(),
		txnCtx,
		ses.GetStorage(),
		proc,
		stmt.stmt,
		ses.isInternal,
		deepcopy.Copy(ses.getCNLabels()).(map[string]string),
		getStatementStartAt(requestCtx),
	)
	defer func() {
		if err != nil {
			stmt.compile.Release()
		}
	}()
	stmt.compile.SetBuildPlanFunc(func() (*plan2.Plan, error) {
		plan, err := buildPlan(requestCtx, ses, ses.GetTxnCompileCtx(), stmt.stmt)
		if err != nil {
			return nil, err
		}
		if plan.IsPrepare {
			_, _, err = plan2.ResetPreparePlan(ses.GetTxnCompileCtx(), plan)
		}
		return plan, err
	})

	if _, ok := stmt.stmt.(*tree.ExplainAnalyze); ok {
		fill = func(obj interface{}, bat *batch.Batch) error { return nil }
	}
	err = stmt.compile.Compile(txnCtx, stmt.plan, ses, fill)
	if err != nil {
		return nil, err
	}
	// check if it is necessary to initialize the temporary engine
	if stmt.compile.NeedInitTempEngine(ses.IfInitedTempEngine()) {
		// 0. init memory-non-dist storage
		var tnStore *metadata.TNService
		t := runtime.ProcessLevelRuntime()
		clock := t.Clock()
		tnStore, err = ses.SetTempTableStorage(clock)
		if err != nil {
			return nil, err
		}

		// temporary storage is passed through Ctx
		requestCtx = context.WithValue(requestCtx, defines.TemporaryTN{}, ses.GetTempTableStorage())

		// 1. init memory-non-dist engine
		tempEngine := memoryengine.New(
			requestCtx,
			memoryengine.NewDefaultShardPolicy(
				mpool.MustNewZeroNoFixed(),
			),
			memoryengine.RandomIDGenerator,
			clusterservice.NewMOCluster(
				nil,
				0,
				clusterservice.WithDisableRefresh(),
				clusterservice.WithServices(nil, []metadata.TNService{
					*tnStore,
				})),
		)

		// 2. bind the temporary engine to the session and txnHandler
		_ = ses.SetTempEngine(requestCtx, tempEngine)
		stmt.compile.SetTempEngine(requestCtx, tempEngine)
		txnHandler.SetTempEngine(tempEngine)
		ses.GetTxnHandler().AttachTempStorageToTxnCtx()

		_, txnOp := ses.txn.GetTxnOperator()
		// 3. init temp-db to store temporary relations
		err = tempEngine.Create(requestCtx, defines.TEMPORARY_DBNAME, txnOp)
		if err != nil {
			return nil, err
		}

		ses.EnableInitTempEngine()
	}
	return stmt.compile, err
}

func (ge *GeneralExecutor) Close(ctx context.Context) error {
	return nil
}

func NewQuery(ast tree.Statement) *Stmt {
	uuid, _ := uuid.NewUUID()
	return &Stmt{
		stmt: ast,
		uuid: uuid,
	}
}

var parseSql = func(ctx context.Context, db string, input *UserInput, user string, ses *Session) ([]*Stmt, error) {
	queries := make([]*Stmt, 0)
	if cached := ses.getCachedPlan(input.getSql()); cached != nil {
		modify := false
		for i, stmt := range cached.stmts {
			query := NewQuery(stmt)
			query.plan = cached.plans[i]
			if checkColModify(query.plan, ses) {
				modify = true
				break
			}
			queries = append(queries, query)
		}
		if modify {
			queries = queries[:0]
		} else {
			return queries, nil
		}
	}

	var stmts []tree.Statement = nil
	var cmdFieldStmt *InternalCmdFieldList
	var err error
	// if the input is an option ast, we should use it directly
	if input.getStmt() != nil {
		stmts = append(stmts, input.getStmt())
	} else if isCmdFieldListSql(input.getSql()) {
		cmdFieldStmt, err = parseCmdFieldList(ctx, input.getSql())
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
		stmts, err = parsers.Parse(ctx, dialect.MYSQL, input.getSql(), v.(int64))
		if err != nil {
			return nil, err
		}
	}

	for _, stmt := range stmts {
		queries = append(queries, NewQuery(stmt))
	}
	return queries, nil
}

// canExecuteStatementInUncommittedTxn checks the user can execute the statement in an uncommitted transaction
func canExecuteStatementInUncommittedTransaction(requestCtx context.Context, label Label, stmt tree.Statement) error {
	if label&CanExecInUncommittedTxnTxn == 0 {
		//is ddl statement
		if IsCreateDropDatabase(stmt) {
			return moerr.NewInternalError(requestCtx, createDropDatabaseErrorInfo())
		} else if IsDDL(stmt) {
			return moerr.NewInternalError(requestCtx, onlyCreateStatementErrorInfo())
		} else if IsAdministrativeStatement(stmt) {
			return moerr.NewInternalError(requestCtx, administrativeCommandIsUnsupportedInTxnErrorInfo())
		} else if IsParameterModificationStatement(stmt) {
			return moerr.NewInternalError(requestCtx, parameterModificationInTxnErrorInfo())
		} else {
			return moerr.NewInternalError(requestCtx, unclassifiedStatementInUncommittedTxnErrorInfo())
		}
	}
	return nil
}

func prepareExecutor(reqCtx context.Context, ses *Session, stmt tree.Statement) (Executor, error) {
	var err error
	var ret Executor
	switch st := stmt.(type) {
	case *tree.Select:
		ret = &SelectExecutor{
			sel: st,
		}
		// case *tree.ValuesStatement:
		// 	ret = &ValuesStmtExecutor{

		// 		sel: st,
		// 	}
		// case *tree.ShowCreateTable:
		// 	ret = &ShowCreateTableExecutor{

		// 		sct: st,
		// 	}
		// case *tree.ShowCreateDatabase:
		// 	ret = &ShowCreateDatabaseExecutor{

		// 		scd: st,
		// 	}
		// case *tree.ShowTables:
		// 	ret = &ShowTablesExecutor{

		// 		st: st,
		// 	}
		// case *tree.ShowSequences:
		// 	ret = &ShowSequencesExecutor{

		// 		ss: st,
		// 	}
		// case *tree.ShowDatabases:
		// 	ret = &ShowDatabasesExecutor{

		// 		sd: st,
		// 	}
		// case *tree.ShowColumns:
		// 	ret = &ShowColumnsExecutor{

		// 		sc: st,
		// 	}
		// case *tree.ShowProcessList:
		// 	ret = &ShowProcessListExecutor{

		// 		spl: st,
		// 	}
		// case *tree.ShowStatus:
		// 	ret = &ShowStatusExecutor{

		// 		ss: st,
		// 	}
		// case *tree.ShowTableStatus:
		// 	ret = &ShowTableStatusExecutor{

		// 		sts: st,
		// 	}
		// case *tree.ShowGrants:
		// 	ret = &ShowGrantsExecutor{

		// 		sg: st,
		// 	}
		// case *tree.ShowIndex:
		// 	ret = &ShowIndexExecutor{

		// 		si: st,
		// 	}
		// case *tree.ShowCreateView:
		// 	ret = &ShowCreateViewExecutor{

		// 		scv: st,
		// 	}
		// case *tree.ShowTarget:
		// 	ret = &ShowTargetExecutor{

		// 		st: st,
		// 	}
		// case *tree.ExplainFor:
		// 	ret = &ExplainForExecutor{

		// 		ef: st,
		// 	}
		// case *tree.ExplainStmt:

		// 	ret = &ExplainStmtExecutor{

		// 		es: st,
		// 	}
		// case *tree.ShowVariables:

		// 	ret = &ShowVariablesExecutor{

		// 		sv: st,
		// 	}
		// case *tree.ShowErrors:

		// 	ret = &ShowErrorsExecutor{

		// 		se: st,
		// 	}
		// case *tree.ShowWarnings:

		// 	ret = &ShowWarningsExecutor{

		// 		sw: st,
		// 	}
		// case *tree.AnalyzeStmt:

		// 	ret = &AnalyzeStmtExecutor{

		// 		as: st,
		// 	}
		// case *tree.ExplainAnalyze:
		// 	ret = &ExplainAnalyzeExecutor{

		// 		ea: st,
		// 	}
		// case *InternalCmdFieldList:

		// 	ret = &InternalCmdFieldListExecutor{

		// 		icfl: st,
		// 	}
		// //PART 2: the statement with the status only
	case *tree.BeginTransaction:

		ret = &BeginTxnExecutor{

			bt: st,
		}
	case *tree.CommitTransaction:

		ret = &CommitTxnExecutor{

			ct: st,
		}
	case *tree.RollbackTransaction:

		ret = &RollbackTxnExecutor{

			rt: st,
		}
	// case *tree.SetRole:

	// 	ret = &SetRoleExecutor{

	// 		sr: st,
	// 	}
	case *tree.Use:

		ret = &UseExecutor{

			u: st,
		}
	// case *tree.MoDump:
	// 	//TODO:
	// 	err = moerr.NewInternalError(proc.Ctx, "needs to add modump")
	// case *tree.DropDatabase:
	// 	ret = &DropDatabaseExecutor{

	// 		dd: st,
	// 	}
	// case *tree.PrepareStmt:

	// 	ret = &PrepareStmtExecutor{

	// 		ps: st,
	// 	}
	// case *tree.PrepareString:

	// 	ret = &PrepareStringExecutor{

	// 		ps: st,
	// 	}
	// case *tree.Deallocate:

	// 	ret = &DeallocateExecutor{

	// 		d: st,
	// 	}
	// case *tree.SetVar:

	// 	ret = &SetVarExecutor{

	// 		sv: st,
	// 	}
	// case *tree.Delete:
	// 	ret = &DeleteExecutor{

	// 		d: st,
	// 	}
	// case *tree.Update:
	// 	ret = &UpdateExecutor{

	// 		u: st,
	// 	}
	// case *tree.CreatePublication:

	// 	ret = &CreatePublicationExecutor{

	// 		cp: st,
	// 	}
	// case *tree.AlterPublication:

	// 	ret = &AlterPublicationExecutor{

	// 		ap: st,
	// 	}
	// case *tree.DropPublication:

	// 	ret = &DropPublicationExecutor{

	// 		dp: st,
	// 	}
	// case *tree.CreateAccount:

	// 	ret = &CreateAccountExecutor{

	// 		ca: st,
	// 	}
	// case *tree.DropAccount:

	// 	ret = &DropAccountExecutor{

	// 		da: st,
	// 	}
	// case *tree.AlterAccount:
	// 	ret = &AlterAccountExecutor{

	// 		aa: st,
	// 	}
	// case *tree.CreateUser:

	// 	ret = &CreateUserExecutor{

	// 		cu: st,
	// 	}
	// case *tree.DropUser:

	// 	ret = &DropUserExecutor{

	// 		du: st,
	// 	}
	// case *tree.AlterUser:
	// 	ret = &AlterUserExecutor{

	// 		au: st,
	// 	}
	// case *tree.CreateRole:

	// 	ret = &CreateRoleExecutor{

	// 		cr: st,
	// 	}
	// case *tree.DropRole:

	// 	ret = &DropRoleExecutor{

	// 		dr: st,
	// 	}
	// case *tree.Grant:

	// 	ret = &GrantExecutor{

	// 		g: st,
	// 	}
	// case *tree.Revoke:

	// 	ret = &RevokeExecutor{

	// 		r: st,
	// 	}
	// case *tree.CreateTable:
	// 	ret = &CreateTableExecutor{

	// 		ct: st,
	// 	}
	// case *tree.DropTable:
	// 	ret = &DropTableExecutor{

	// 		dt: st,
	// 	}
	// case *tree.CreateDatabase:
	// 	ret = &CreateDatabaseExecutor{

	// 		cd: st,
	// 	}
	// case *tree.CreateIndex:
	// 	ret = &CreateIndexExecutor{

	// 		ci: st,
	// 	}
	// case *tree.DropIndex:
	// 	ret = &DropIndexExecutor{

	// 		di: st,
	// 	}
	// case *tree.CreateSequence:
	// 	ret = &CreateSequenceExecutor{

	// 		cs: st,
	// 	}
	// case *tree.DropSequence:
	// 	ret = &DropSequenceExecutor{

	// 		ds: st,
	// 	}
	// case *tree.AlterSequence:
	// 	ret = &AlterSequenceExecutor{

	// 		cs: st,
	// 	}
	// case *tree.CreateView:
	// 	ret = &CreateViewExecutor{

	// 		cv: st,
	// 	}
	// case *tree.AlterView:
	// 	ret = &AlterViewExecutor{

	// 		av: st,
	// 	}
	// case *tree.AlterTable:
	// 	ret = &AlterTableExecutor{

	// 		at: st,
	// 	}
	// case *tree.DropView:
	// 	ret = &DropViewExecutor{

	// 		dv: st,
	// 	}
	// case *tree.Insert:
	// 	ret = &InsertExecutor{

	// 		i: st,
	// 	}
	// case *tree.Replace:
	// 	ret = &ReplaceExecutor{

	// 		r: st,
	// 	}
	// case *tree.Load:
	// 	ret = &LoadExecutor{

	// 		l: st,
	// 	}
	// case *tree.SetDefaultRole:
	// 	ret = &SetDefaultRoleExecutor{

	// 		sdr: st,
	// 	}
	// case *tree.SetPassword:
	// 	ret = &SetPasswordExecutor{

	// 		sp: st,
	// 	}
	// case *tree.TruncateTable:
	// 	ret = &TruncateTableExecutor{

	// 		tt: st,
	// 	}
	// //PART 3: hybrid
	// case *tree.Execute:
	// 	ret = &ExecuteExecutor{
	// 		e: st,
	// 	}
	default:
		panic(fmt.Sprintf("no such statement %s", stmt.String()))
		return nil, moerr.NewInternalError(reqCtx, "no such statement %s", stmt.String())
	}
	return ret, err
}

func (be *BackgroundExecutor) Open(ctx context.Context, opts ...QueryExecutorOpt) error {
	for _, opt := range opts {
		opt(&be.opts)
	}
	return nil
}
func (be *BackgroundExecutor) Exec(ctx context.Context, input *UserInput) error {
	return nil
}
func (be *BackgroundExecutor) Close(ctx context.Context) error {
	return nil
}
