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
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	"io"
	gotrace "runtime/trace"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend/constant"
	"go.uber.org/zap"

	"github.com/fagongzi/goetty/v2"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/route"
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

func parameterModificationInTxnErrorInfo() string {
	return "Uncommitted transaction exists. Please commit or rollback first."
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

func getPrepareStmtName(stmtID uint32) string {
	return fmt.Sprintf("%s_%d", prefixPrepareStmtName, stmtID)
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

type MysqlCmdExecutor struct {
	CmdExecutorImpl

	//for cmd 0x4
	TableInfoCache

	//the count of sql has been processed
	sqlCount uint64

	ses TempInter

	routineMgr *RoutineManager

	cancelRequestFunc context.CancelFunc

	doQueryFunc doComQueryFunc

	mu sync.Mutex
}

func NewMysqlCmdExecutor() *MysqlCmdExecutor {
	return &MysqlCmdExecutor{}
}

func (mce *MysqlCmdExecutor) CancelRequest() {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	if mce.cancelRequestFunc != nil {
		mce.cancelRequestFunc()
	}
}

func (mce *MysqlCmdExecutor) ChooseDoQueryFunc(choice bool) {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	mce.doQueryFunc = mce.doComQuery
}

func (mce *MysqlCmdExecutor) GetDoQueryFunc() doComQueryFunc {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	if mce.doQueryFunc == nil {
		mce.doQueryFunc = mce.doComQuery
	}
	return mce.doQueryFunc
}

func (mce *MysqlCmdExecutor) SetSession(ses TempInter) {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	mce.ses = ses
}

func (mce *MysqlCmdExecutor) GetSession() TempInter {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	return mce.ses
}

// get new process id
func (mce *MysqlCmdExecutor) getNextProcessId() string {
	/*
		temporary method:
		routineId + sqlCount
	*/
	routineId := mce.GetSession().GetMysqlProtocol().ConnectionID()
	return fmt.Sprintf("%d%d", routineId, mce.GetSqlCount())
}

func (mce *MysqlCmdExecutor) GetSqlCount() uint64 {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	return mce.sqlCount
}

func (mce *MysqlCmdExecutor) addSqlCount(a uint64) {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	mce.sqlCount += a
}

func (mce *MysqlCmdExecutor) SetRoutineManager(mgr *RoutineManager) {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	mce.routineMgr = mgr
}

func (mce *MysqlCmdExecutor) GetRoutineManager() *RoutineManager {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	return mce.routineMgr
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
			text = SubStringFromBegin(bb.String(), int(ses.GetParameterUnit().SV.LengthOfQueryPrinted))
		} else {
			text = SubStringFromBegin(envStmt, int(ses.GetParameterUnit().SV.LengthOfQueryPrinted))
		}
	} else {
		stmID, _ = uuid.NewV7()
		text = SubStringFromBegin(envStmt, int(ses.GetParameterUnit().SV.LengthOfQueryPrinted))
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

func handleShowTableStatus(ses *Session, stmt *tree.ShowTableStatus, proc *process.Process) error {
	var db engine.Database
	var err error

	ctx := ses.requestCtx
	if db, err = ses.GetParameterUnit().StorageEngine.Database(ctx, stmt.DbName, proc.TxnOperator); err != nil {
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
	if dbMeta, err = ses.GetParameterUnit().StorageEngine.Database(txnCtx, db, txn); err != nil {
		//echo client. no such database
		return moerr.NewBadDB(ctx, db)
	}
	if dbMeta.IsSubscription(ctx) {
		_, err = checkSubscriptionValid(ctx, ses.(*Session), dbMeta.GetCreateSql(ctx))
		if err != nil {
			return err
		}
	}
	oldDB := ses.GetDatabaseName()
	ses.SetDatabaseName(db)

	logDebugf(ses.GetDebugString(), "User %s change database from [%s] to [%s]", ses.GetUserName(), oldDB, ses.GetDatabaseName())

	return nil
}

func (mce *MysqlCmdExecutor) handleChangeDB(requestCtx context.Context, db string) error {
	return doUse(requestCtx, mce.GetSession(), db)
}

func (mce *MysqlCmdExecutor) handleDump(requestCtx context.Context, dump *tree.MoDump) error {
	return doDumpQueryResult(requestCtx, mce.GetSession().(*Session), dump.ExportParams)
}

/*
handle "SELECT @@xxx.yyyy"
*/
func (mce *MysqlCmdExecutor) handleSelectVariables(ve *tree.VarExpr, cwIndex, cwsLen int) error {
	var err error = nil
	ses := mce.GetSession()
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
	resp := mce.ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, cwIndex, cwsLen)

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
	//if mce.tableInfos == nil || mce.db != dbName {
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
	//	if mce.tableInfos == nil {
	//		mce.tableInfos = make(map[string][]ColumnInfo)
	//	}
	//	mce.tableInfos[tableName] = attrs
	//}
	//
	//cols, ok := mce.tableInfos[tableName]
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
func (mce *MysqlCmdExecutor) handleCmdFieldList(requestCtx context.Context, icfl *InternalCmdFieldList) error {
	var err error
	ses := mce.GetSession()
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

func doSetVar(ctx context.Context, mce *MysqlCmdExecutor, ses *Session, sv *tree.SetVar, sql string) error {
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

		value, err = getExprValue(assign.Value, mce, ses)
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
func (mce *MysqlCmdExecutor) handleSetVar(ctx context.Context, sv *tree.SetVar, sql string) error {
	ses := mce.GetSession()
	err := doSetVar(ctx, mce, ses.(*Session), sv, sql)
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

func (mce *MysqlCmdExecutor) handleShowErrors(cwIndex, cwsLen int) error {
	var err error
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()
	err = doShowErrors(ses.(*Session))
	if err != nil {
		return err
	}

	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := mce.ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, cwIndex, cwsLen)

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
func (mce *MysqlCmdExecutor) handleShowVariables(sv *tree.ShowVariables, proc *process.Process, cwIndex, cwsLen int) error {
	ses := mce.GetSession()
	proto := ses.GetMysqlProtocol()
	err := doShowVariables(ses.(*Session), proc, sv)
	if err != nil {
		return err
	}
	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := mce.ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, cwIndex, cwsLen)

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

func (mce *MysqlCmdExecutor) handleAnalyzeStmt(requestCtx context.Context, ses *Session, stmt *tree.AnalyzeStmt) error {
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
	return mce.GetDoQueryFunc()(requestCtx, &UserInput{sql: sql})
}

// Note: for pass the compile quickly. We will remove the comments in the future.
func (mce *MysqlCmdExecutor) handleExplainStmt(requestCtx context.Context, stmt *tree.ExplainStmt) error {
	es, err := getExplainOption(requestCtx, stmt.Options)
	if err != nil {
		return err
	}

	ses := mce.GetSession()

	//get query optimizer and execute Optimize
	plan, err := buildPlan(requestCtx, ses.(*Session), ses.GetTxnCompileCtx(), stmt.Statement)
	if err != nil {
		return err
	}
	if plan.GetQuery() == nil {
		return moerr.NewNotSupported(requestCtx, "the sql query plan does not support explain.")
	}
	// generator query explain
	explainQuery := explain.NewExplainQueryImpl(plan.GetQuery())

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
func (mce *MysqlCmdExecutor) handlePrepareStmt(ctx context.Context, st *tree.PrepareStmt, sql string) (*PrepareStmt, error) {
	return doPrepareStmt(ctx, mce.GetSession().(*Session), st, sql)
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
func (mce *MysqlCmdExecutor) handlePrepareString(ctx context.Context, st *tree.PrepareString) (*PrepareStmt, error) {
	return doPrepareString(ctx, mce.GetSession().(*Session), st)
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
func (mce *MysqlCmdExecutor) handleDeallocate(ctx context.Context, st *tree.Deallocate) error {
	return doDeallocate(ctx, mce.GetSession().(*Session), st)
}

// handleReset
func (mce *MysqlCmdExecutor) handleReset(ctx context.Context, st *tree.Reset) error {
	return doReset(ctx, mce.GetSession().(*Session), st)
}

func (mce *MysqlCmdExecutor) handleCreatePublication(ctx context.Context, cp *tree.CreatePublication) error {
	return doCreatePublication(ctx, mce.GetSession().(*Session), cp)
}

func (mce *MysqlCmdExecutor) handleAlterPublication(ctx context.Context, ap *tree.AlterPublication) error {
	return doAlterPublication(ctx, mce.GetSession().(*Session), ap)
}

func (mce *MysqlCmdExecutor) handleDropPublication(ctx context.Context, dp *tree.DropPublication) error {
	return doDropPublication(ctx, mce.GetSession().(*Session), dp)
}

func (mce *MysqlCmdExecutor) handleCreateStage(ctx context.Context, cs *tree.CreateStage) error {
	return doCreateStage(ctx, mce.GetSession().(*Session), cs)
}

func (mce *MysqlCmdExecutor) handleAlterStage(ctx context.Context, as *tree.AlterStage) error {
	return doAlterStage(ctx, mce.GetSession().(*Session), as)
}

func (mce *MysqlCmdExecutor) handleDropStage(ctx context.Context, ds *tree.DropStage) error {
	return doDropStage(ctx, mce.GetSession().(*Session), ds)
}

// handleCreateAccount creates a new user-level tenant in the context of the tenant SYS
// which has been initialized.
func (mce *MysqlCmdExecutor) handleCreateAccount(ctx context.Context, ca *tree.CreateAccount) error {
	//step1 : create new account.
	return InitGeneralTenant(ctx, mce.GetSession().(*Session), ca)
}

// handleDropAccount drops a new user-level tenant
func (mce *MysqlCmdExecutor) handleDropAccount(ctx context.Context, da *tree.DropAccount) error {
	return doDropAccount(ctx, mce.GetSession().(*Session), da)
}

// handleDropAccount drops a new user-level tenant
func (mce *MysqlCmdExecutor) handleAlterAccount(ctx context.Context, aa *tree.AlterAccount) error {
	return doAlterAccount(ctx, mce.GetSession().(*Session), aa)
}

// handleAlterDatabaseConfig alter a database's mysql_compatibility_mode
func (mce *MysqlCmdExecutor) handleAlterDataBaseConfig(ctx context.Context, ses *Session, ad *tree.AlterDataBaseConfig) error {
	return doAlterDatabaseConfig(ctx, mce.GetSession().(*Session), ad)
}

// handleAlterAccountConfig alter a account's mysql_compatibility_mode
func (mce *MysqlCmdExecutor) handleAlterAccountConfig(ctx context.Context, ses *Session, st *tree.AlterDataBaseConfig) error {
	return doAlterAccountConfig(ctx, mce.GetSession().(*Session), st)
}

// handleCreateUser creates the user for the tenant
func (mce *MysqlCmdExecutor) handleCreateUser(ctx context.Context, cu *tree.CreateUser) error {
	ses := mce.GetSession().(*Session)
	tenant := ses.GetTenantInfo()

	//step1 : create the user
	return InitUser(ctx, ses, tenant, cu)
}

// handleDropUser drops the user for the tenant
func (mce *MysqlCmdExecutor) handleDropUser(ctx context.Context, du *tree.DropUser) error {
	return doDropUser(ctx, mce.GetSession().(*Session), du)
}

func (mce *MysqlCmdExecutor) handleAlterUser(ctx context.Context, au *tree.AlterUser) error {
	return doAlterUser(ctx, mce.GetSession().(*Session), au)
}

// handleCreateRole creates the new role
func (mce *MysqlCmdExecutor) handleCreateRole(ctx context.Context, cr *tree.CreateRole) error {
	ses := mce.GetSession().(*Session)
	tenant := ses.GetTenantInfo()

	//step1 : create the role
	return InitRole(ctx, ses, tenant, cr)
}

// handleDropRole drops the role
func (mce *MysqlCmdExecutor) handleDropRole(ctx context.Context, dr *tree.DropRole) error {
	return doDropRole(ctx, mce.GetSession().(*Session), dr)
}

func (mce *MysqlCmdExecutor) handleCreateFunction(ctx context.Context, cf *tree.CreateFunction) error {
	ses := mce.GetSession().(*Session)
	tenant := ses.GetTenantInfo()

	return mce.InitFunction(ctx, ses, tenant, cf)
}

func (mce *MysqlCmdExecutor) handleDropFunction(ctx context.Context, df *tree.DropFunction, proc *process.Process) error {
	return doDropFunction(ctx, mce.GetSession().(*Session), df, func(path string) error {
		return proc.FileService.Delete(ctx, path)
	})
}

func (mce *MysqlCmdExecutor) handleCreateProcedure(ctx context.Context, cp *tree.CreateProcedure) error {
	ses := mce.GetSession().(*Session)
	tenant := ses.GetTenantInfo()

	return InitProcedure(ctx, ses, tenant, cp)
}

func (mce *MysqlCmdExecutor) handleDropProcedure(ctx context.Context, dp *tree.DropProcedure) error {
	return doDropProcedure(ctx, mce.GetSession().(*Session), dp)
}

func (mce *MysqlCmdExecutor) handleCallProcedure(ctx context.Context, call *tree.CallStmt, proc *process.Process, cwIndex, cwsLen int) error {
	ses := mce.GetSession().(*Session)
	proto := ses.GetMysqlProtocol()
	results, err := doInterpretCall(ctx, mce.GetSession().(*Session), call)
	if err != nil {
		return err
	}

	resp := NewGeneralOkResponse(COM_QUERY, mce.ses.GetServerStatus())

	if len(results) == 0 {
		if err := proto.SendResponse(ses.requestCtx, resp); err != nil {
			return moerr.NewInternalError(ses.requestCtx, "routine send response failed. error:%v ", err)
		}
	} else {
		for i, result := range results {
			mer := NewMysqlExecutionResult(0, 0, 0, 0, result.(*MysqlResultSet))
			resp = mce.ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, i, len(results))
			if err := proto.SendResponse(ses.requestCtx, resp); err != nil {
				return moerr.NewInternalError(ses.requestCtx, "routine send response failed. error:%v ", err)
			}
		}
	}
	return nil
}

// handleGrantRole grants the role
func (mce *MysqlCmdExecutor) handleGrantRole(ctx context.Context, gr *tree.GrantRole) error {
	return doGrantRole(ctx, mce.GetSession().(*Session), gr)
}

// handleRevokeRole revokes the role
func (mce *MysqlCmdExecutor) handleRevokeRole(ctx context.Context, rr *tree.RevokeRole) error {
	return doRevokeRole(ctx, mce.GetSession().(*Session), rr)
}

// handleGrantRole grants the privilege to the role
func (mce *MysqlCmdExecutor) handleGrantPrivilege(ctx context.Context, gp *tree.GrantPrivilege) error {
	return doGrantPrivilege(ctx, mce.GetSession().(*Session), gp)
}

// handleRevokePrivilege revokes the privilege from the user or role
func (mce *MysqlCmdExecutor) handleRevokePrivilege(ctx context.Context, rp *tree.RevokePrivilege) error {
	return doRevokePrivilege(ctx, mce.GetSession().(*Session), rp)
}

// handleSwitchRole switches the role to another role
func (mce *MysqlCmdExecutor) handleSwitchRole(ctx context.Context, sr *tree.SetRole) error {
	return doSwitchRole(ctx, mce.GetSession().(*Session), sr)
}

func doKill(ctx context.Context, rm *RoutineManager, ses *Session, k *tree.Kill) error {
	var err error
	//true: kill a connection
	//false: kill a query in a connection
	idThatKill := uint64(ses.GetConnectionID())
	if !k.Option.Exist || k.Option.Typ == tree.KillTypeConnection {
		err = rm.kill(ctx, true, idThatKill, k.ConnectionId, "")
	} else {
		err = rm.kill(ctx, false, idThatKill, k.ConnectionId, k.StmtOption.StatementId)
	}
	return err
}

// handleKill kill a connection or query
func (mce *MysqlCmdExecutor) handleKill(ctx context.Context, k *tree.Kill) error {
	var err error
	ses := mce.GetSession().(*Session)
	proto := ses.GetMysqlProtocol()
	err = doKill(ctx, mce.GetRoutineManager(), ses, k)
	if err != nil {
		return err
	}
	resp := NewGeneralOkResponse(COM_QUERY, mce.ses.GetServerStatus())
	if err = proto.SendResponse(ctx, resp); err != nil {
		return moerr.NewInternalError(ctx, "routine send response failed. error:%v ", err)
	}
	return err
}

// handleShowAccounts lists the info of accounts
func (mce *MysqlCmdExecutor) handleShowAccounts(ctx context.Context, sa *tree.ShowAccounts, cwIndex, cwsLen int) error {
	var err error
	ses := mce.GetSession().(*Session)
	proto := ses.GetMysqlProtocol()
	err = doShowAccounts(ctx, ses, sa)
	if err != nil {
		return err
	}
	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := mce.ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, cwIndex, cwsLen)

	if err = proto.SendResponse(ctx, resp); err != nil {
		return moerr.NewInternalError(ctx, "routine send response failed. error:%v ", err)
	}
	return err
}

// handleShowCollation lists the info of collation
func (mce *MysqlCmdExecutor) handleShowCollation(sc *tree.ShowCollation, proc *process.Process, cwIndex, cwsLen int) error {
	var err error
	ses := mce.GetSession().(*Session)
	proto := ses.GetMysqlProtocol()
	err = doShowCollation(ses, proc, sc)
	if err != nil {
		return err
	}
	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := mce.ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, cwIndex, cwsLen)

	if err := proto.SendResponse(ses.requestCtx, resp); err != nil {
		return moerr.NewInternalError(ses.requestCtx, "routine send response failed. error:%v ", err)
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

func (mce *MysqlCmdExecutor) handleShowSubscriptions(ctx context.Context, ss *tree.ShowSubscriptions, cwIndex, cwsLen int) error {
	var err error
	ses := mce.GetSession().(*Session)
	proto := ses.GetMysqlProtocol()
	err = doShowSubscriptions(ctx, ses, ss)
	if err != nil {
		return err
	}
	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := mce.ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, cwIndex, cwsLen)

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

func (mce *MysqlCmdExecutor) handleShowBackendServers(ctx context.Context, cwIndex, cwsLen int) error {
	var err error
	ses := mce.GetSession().(*Session)
	proto := ses.GetMysqlProtocol()
	if err := doShowBackendServers(ses); err != nil {
		return err
	}

	mer := NewMysqlExecutionResult(0, 0, 0, 0, ses.GetMysqlResultSet())
	resp := mce.ses.SetNewResponse(ResultResponse, 0, int(COM_QUERY), mer, cwIndex, cwsLen)
	if err := proto.SendResponse(ses.requestCtx, resp); err != nil {
		return moerr.NewInternalError(ses.requestCtx, "routine send response failed, error: %v ", err)
	}
	return err
}

func (mce *MysqlCmdExecutor) handleEmptyStmt(ctx context.Context, stmt *tree.EmptyStmt) error {
	var err error
	ses := mce.GetSession().(*Session)
	proto := ses.GetMysqlProtocol()

	resp := NewGeneralOkResponse(COM_QUERY, mce.ses.GetServerStatus())
	if err = proto.SendResponse(ctx, resp); err != nil {
		return moerr.NewInternalError(ctx, "routine send response failed. error:%v ", err)
	}
	return err
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

// authenticateUserCanExecuteStatement checks the user can execute the statement
func authenticateUserCanExecuteStatement(requestCtx context.Context, ses *Session, stmt tree.Statement) error {
	requestCtx, span := trace.Debug(requestCtx, "authenticateUserCanExecuteStatement")
	defer span.End()
	if ses.pu.SV.SkipCheckPrivilege {
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
	if ses.pu.SV.SkipCheckPrivilege {
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
	if ses.pu.SV.SkipCheckPrivilege {
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
func (mce *MysqlCmdExecutor) canExecuteStatementInUncommittedTransaction(requestCtx context.Context, stmt tree.Statement) error {
	can, err := statementCanBeExecutedInUncommittedTransaction(mce.GetSession(), stmt)
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

func (mce *MysqlCmdExecutor) processLoadLocal(ctx context.Context, param *tree.ExternParam, writer *io.PipeWriter) (err error) {
	ses := mce.GetSession()
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

func (mce *MysqlCmdExecutor) executeStmt(requestCtx context.Context,
	ses *Session,
	stmt tree.Statement,
	proc *process.Process,
	cw ComputationWrapper,
	i int,
	cws []ComputationWrapper,
	proto MysqlProtocol,
	pu *config.ParameterUnit,
	tenant string,
	userName string,
	sql string,
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
			stmt.Format(formatCtx)
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
	switch stmt.(type) {
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

	var runResult *util2.RunResult
	var prepareStmt *PrepareStmt

	var cmpBegin time.Time
	var ret interface{}
	var runner ComputationRunner
	var selfHandle bool
	var columns []interface{}
	var mrs *MysqlResultSet
	var loadLocalErrGroup *errgroup.Group
	var loadLocalWriter *io.PipeWriter

	//response the client
	respClientFunc := func() error {
		var rspLen uint64
		if runResult != nil {
			rspLen = runResult.AffectRows
		}

		switch stmt.(type) {
		case *tree.Select:
			if len(proc.SessionInfo.SeqAddValues) != 0 {
				ses.AddSeqValues(proc)
			}
			ses.SetSeqLastValue(proc)
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
			resp := mce.setResponse(i, len(cws), rspLen)
			if _, ok := stmt.(*tree.Insert); ok {
				resp.lastInsertId = proc.GetLastInsertID()
				if proc.GetLastInsertID() != 0 {
					ses.SetLastInsertID(proc.GetLastInsertID())
				}
			}
			if len(proc.SessionInfo.SeqDeleteKeys) != 0 {
				ses.DeleteSeqValues(proc)
			}

			if st, ok := cw.GetAst().(*tree.CreateTable); ok {
				_ = doGrantPrivilegeImplicitly(requestCtx, ses, st)
			}

			if st, ok := cw.GetAst().(*tree.DropTable); ok {
				_ = doRevokePrivilegeImplicitly(requestCtx, ses, st)
			}

			if st, ok := cw.GetAst().(*tree.CreateDatabase); ok {
				_ = insertRecordToMoMysqlCompatibilityMode(requestCtx, ses, stmt)
				_ = doGrantPrivilegeImplicitly(requestCtx, ses, st)
			}

			if st, ok := cw.GetAst().(*tree.DropDatabase); ok {
				_ = deleteRecordToMoMysqlCompatbilityMode(requestCtx, ses, stmt)
				_ = doRevokePrivilegeImplicitly(requestCtx, ses, st)
			}

			if err2 := mce.GetSession().GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
				err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
				logStatementStatus(requestCtx, ses, stmt, fail, err)
				return err
			}

		case *tree.PrepareStmt, *tree.PrepareString:
			if ses.GetCmd() == COM_STMT_PREPARE {
				if err2 := mce.GetSession().GetMysqlProtocol().SendPrepareResponse(requestCtx, prepareStmt); err2 != nil {
					err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
					logStatementStatus(requestCtx, ses, stmt, fail, err)
					return err
				}
			} else {
				resp := mce.setResponse(i, len(cws), rspLen)
				if err2 := mce.GetSession().GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
					err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
					logStatementStatus(requestCtx, ses, stmt, fail, err)
					return err
				}
			}

		case *tree.SetVar, *tree.SetTransaction, *tree.BackupStart, *tree.CreateConnector, *tree.DropConnector,
			*tree.PauseDaemonTask, *tree.ResumeDaemonTask, *tree.CancelDaemonTask:
			resp := mce.setResponse(i, len(cws), rspLen)
			if err2 := proto.SendResponse(requestCtx, resp); err2 != nil {
				return moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
			}
		case *tree.Deallocate:
			//we will not send response in COM_STMT_CLOSE command
			if ses.GetCmd() != COM_STMT_CLOSE {
				resp := mce.setResponse(i, len(cws), rspLen)
				if err2 := mce.GetSession().GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
					err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
					logStatementStatus(requestCtx, ses, stmt, fail, err)
					return err
				}
			}

		case *tree.Reset:
			resp := mce.setResponse(i, len(cws), rspLen)
			if err2 := mce.GetSession().GetMysqlProtocol().SendResponse(requestCtx, resp); err2 != nil {
				err = moerr.NewInternalError(requestCtx, "routine send response failed. error:%v ", err2)
				logStatementStatus(requestCtx, ses, stmt, fail, err)
				return err
			}
		}
		if ses.GetQueryInExecute() {
			logStatementStatus(requestCtx, ses, stmt, success, nil)
		} else {
			logStatementStatus(requestCtx, ses, stmt, fail, moerr.NewInternalError(requestCtx, "query is killed"))
		}
		return err
	}

	//get errors during the transaction. rollback the transaction
	rollbackTxnFunc := func() error {
		incStatementCounter(tenant, stmt)
		incStatementErrorsCounter(tenant, stmt)
		/*
			Cases    | set Autocommit = 1/0 | BEGIN statement |
			---------------------------------------------------
			Case1      1                       Yes
			Case2      1                       No
			Case3      0                       Yes
			Case4      0                       No
			---------------------------------------------------
			update error message in Case1,Case3,Case4.
		*/
		if ses.InMultiStmtTransactionMode() && ses.InActiveTransaction() {
			ses.cleanCache()
		}
		logError(ses, ses.GetDebugString(), err.Error())
		txnErr := ses.TxnRollbackSingleStatement(stmt, err)
		if txnErr != nil {
			logStatementStatus(requestCtx, ses, stmt, fail, txnErr)
			return txnErr
		}
		logStatementStatus(requestCtx, ses, stmt, fail, err)
		return err
	}

	//execution succeeds during the transaction. commit the transaction
	commitTxnFunc := func() (retErr error) {
		// Call a defer function -- if TxnCommitSingleStatement paniced, we
		// want to catch it and convert it to an error.
		defer func() {
			if r := recover(); r != nil {
				retErr = moerr.ConvertPanicError(requestCtx, r)
			}
		}()

		//load data handle txn failure internally
		incStatementCounter(tenant, stmt)
		retErr = ses.TxnCommitSingleStatement(stmt)
		if retErr != nil {
			logStatementStatus(requestCtx, ses, stmt, fail, retErr)
		}
		return
	}

	//finish the transaction
	finishTxnFunc := func() error {
		// First recover all panics.   If paniced, we will abort.
		if r := recover(); r != nil {
			err = moerr.ConvertPanicError(requestCtx, r)
		}

		if err == nil {
			err = commitTxnFunc()
			if err == nil {
				err = respClientFunc()
				return err
			}
			// if commitTxnFunc failed, we will rollback the transaction.
		}

		err = rollbackTxnFunc()
		return err
	}

	// defer transaction state management.
	defer func() {
		err = finishTxnFunc()
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

	// XXX XXX
	// I hope I can break the following code into several functions, but I can't.
	// After separating the functions, the system cannot boot, due to mo_account
	// not exists.  No clue why, the closure/capture must do some magic.

	//check transaction states
	switch stmt.(type) {
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

	switch st := stmt.(type) {
	case *tree.Select:
		if st.Ep != nil {
			if ses.pu.SV.DisableSelectInto {
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

	switch st := stmt.(type) {
	case *tree.BeginTransaction, *tree.CommitTransaction, *tree.RollbackTransaction:
		selfHandle = true
	case *tree.SetRole:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		//switch role
		err = mce.handleSwitchRole(requestCtx, st)
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
		err = mce.handleChangeDB(requestCtx, st.Name.Compare())
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
		err = mce.handleDump(requestCtx, st)
		if err != nil {
			return
		}
	case *tree.CreateDatabase:
		err = inputNameIsInvalid(proc.Ctx, string(st.Name))
		if err != nil {
			return
		}
		if st.SubscriptionOption != nil && ses.GetTenantInfo() != nil && !ses.GetTenantInfo().IsAdminRole() {
			err = moerr.NewInternalError(proc.Ctx, "only admin can create subscription")
			return
		}
		st.Sql = sql
	case *tree.DropDatabase:
		err = inputNameIsInvalid(proc.Ctx, string(st.Name))
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
		prepareStmt, err = mce.handlePrepareStmt(requestCtx, st, sql)
		if err != nil {
			return
		}
		err = authenticateUserCanExecutePrepareOrExecute(requestCtx, ses, prepareStmt.PrepareStmt, prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
		if err != nil {
			mce.GetSession().RemovePrepareStmt(prepareStmt.Name)
			return
		}
	case *tree.PrepareString:
		selfHandle = true
		prepareStmt, err = mce.handlePrepareString(requestCtx, st)
		if err != nil {
			return
		}
		err = authenticateUserCanExecutePrepareOrExecute(requestCtx, ses, prepareStmt.PrepareStmt, prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
		if err != nil {
			mce.GetSession().RemovePrepareStmt(prepareStmt.Name)
			return
		}
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
	case *tree.AnalyzeStmt:
		selfHandle = true
		if err = mce.handleAnalyzeStmt(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.ExplainStmt:
		selfHandle = true
		if err = mce.handleExplainStmt(requestCtx, st); err != nil {
			return
		}
	case *tree.ExplainAnalyze:
		ses.SetData(nil)
	case *tree.ShowTableStatus:
		ses.SetShowStmtType(ShowTableStatus)
		ses.SetData(nil)
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
		ses.InvalidatePrivilegeCache()
		if err = mce.handleCreateAccount(requestCtx, st); err != nil {
			return
		}
	case *tree.DropAccount:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = mce.handleDropAccount(requestCtx, st); err != nil {
			return
		}
	case *tree.AlterAccount:
		ses.InvalidatePrivilegeCache()
		selfHandle = true
		if err = mce.handleAlterAccount(requestCtx, st); err != nil {
			return
		}
	case *tree.AlterDataBaseConfig:
		ses.InvalidatePrivilegeCache()
		selfHandle = true
		if st.IsAccountLevel {
			if err = mce.handleAlterAccountConfig(requestCtx, ses, st); err != nil {
				return
			}
		} else {
			if err = mce.handleAlterDataBaseConfig(requestCtx, ses, st); err != nil {
				return
			}
		}
	case *tree.CreateUser:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = mce.handleCreateUser(requestCtx, st); err != nil {
			return
		}
	case *tree.DropUser:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = mce.handleDropUser(requestCtx, st); err != nil {
			return
		}
	case *tree.AlterUser: //TODO
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = mce.handleAlterUser(requestCtx, st); err != nil {
			return
		}
	case *tree.CreateRole:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
		if err = mce.handleCreateRole(requestCtx, st); err != nil {
			return
		}
	case *tree.DropRole:
		selfHandle = true
		ses.InvalidatePrivilegeCache()
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
		ses.InvalidatePrivilegeCache()
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
		ses.InvalidatePrivilegeCache()
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
		ses.InvalidatePrivilegeCache()
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
		if st.Local {
			proc.LoadLocalReader, loadLocalWriter = io.Pipe()
		}
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

	if ret, err = cw.Compile(requestCtx, ses, ses.GetOutputCallback()); err != nil {
		return
	}
	stmt = cw.GetAst()
	// reset some special stmt for execute statement
	switch st := stmt.(type) {
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
		logInfo(ses, ses.GetDebugString(), fmt.Sprintf("time of Exec.Build : %s", time.Since(cmpBegin).String()))
	}

	mrs = ses.GetMysqlResultSet()
	ep := ses.GetExportConfig()
	// cw.Compile might rewrite sql, here we fetch the latest version
	switch statement := stmt.(type) {
	//produce result set
	case *tree.Select:
		if ep.needExportToFile() {

			columns, err = cw.GetColumns()
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
			ep.DefaultBufSize = pu.SV.ExportDataDefaultFlushSize
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
			if cwft, ok := cw.(*TxnComputationWrapper); ok {
				_ = cwft.RecordExecPlan(requestCtx)
			}

		} else {
			columns, err = cw.GetColumns()
			if err != nil {
				logError(ses, ses.GetDebugString(),
					"Failed to get columns from computation handler",
					zap.Error(err))
				return
			}
			if c, ok := cw.(*TxnComputationWrapper); ok {
				ses.rs = &plan.ResultColDef{ResultCols: plan2.GetResultColumnsFromPlan(c.plan)}
			}
			/*
				Step 1 : send column count and column definition.
			*/
			//send column count
			colCnt := uint64(len(columns))
			err = proto.SendColumnCountPacket(colCnt)
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
				err = proto.SendColumnDefinitionPacket(requestCtx, mysqlc, int(cmd))
				if err != nil {
					return
				}
			}

			/*
				mysql COM_QUERY response: End after the column has been sent.
				send EOF packet
			*/
			err = proto.SendEOFPacketIf(0, ses.GetServerStatus())
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
			err = proto.sendEOFOrOkPacket(0, ses.GetServerStatus())
			if err != nil {
				return
			}

			/*
				Step 4: Serialize the execution plan by json
			*/
			if cwft, ok := cw.(*TxnComputationWrapper); ok {
				_ = cwft.RecordExecPlan(requestCtx)
			}
		}

	case *tree.ShowCreateTable, *tree.ShowCreateDatabase, *tree.ShowTables, *tree.ShowSequences, *tree.ShowDatabases, *tree.ShowColumns,
		*tree.ShowProcessList, *tree.ShowStatus, *tree.ShowTableStatus, *tree.ShowGrants, *tree.ShowRolesStmt,
		*tree.ShowIndex, *tree.ShowCreateView, *tree.ShowTarget, *tree.ShowCollation, *tree.ValuesStatement,
		*tree.ExplainFor, *tree.ExplainStmt, *tree.ShowTableNumber, *tree.ShowColumnNumber, *tree.ShowTableValues, *tree.ShowLocks, *tree.ShowNodeList, *tree.ShowFunctionOrProcedureStatus,
		*tree.ShowPublications, *tree.ShowCreatePublications, *tree.ShowStages:
		columns, err = cw.GetColumns()
		if err != nil {
			logError(ses, ses.GetDebugString(),
				"Failed to get columns from computation handler",
				zap.Error(err))
			return
		}
		if c, ok := cw.(*TxnComputationWrapper); ok {
			ses.rs = &plan.ResultColDef{ResultCols: plan2.GetResultColumnsFromPlan(c.plan)}
		}
		/*
			Step 1 : send column count and column definition.
		*/
		//send column count
		colCnt := uint64(len(columns))
		err = proto.SendColumnCountPacket(colCnt)
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
			err = proto.SendColumnDefinitionPacket(requestCtx, mysqlc, int(cmd))
			if err != nil {
				return
			}
		}

		/*
			mysql COM_QUERY response: End after the column has been sent.
			send EOF packet
		*/
		err = proto.SendEOFPacketIf(0, ses.GetServerStatus())
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
			if err = handleShowTableStatus(ses, statement.(*tree.ShowTableStatus), proc); err != nil {
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
		err = proto.sendEOFOrOkPacket(0, ses.GetServerStatus())
		if err != nil {
			return
		}

		/*
			Step 4: Serialize the execution plan by json
		*/
		if cwft, ok := cw.(*TxnComputationWrapper); ok {
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
		switch cw.GetAst().(type) {
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

		if st, ok := cw.GetAst().(*tree.Load); ok {
			if st.Local {
				loadLocalErrGroup = new(errgroup.Group)
				loadLocalErrGroup.Go(func() error {
					return mce.processLoadLocal(proc.Ctx, st.Param, loadLocalWriter)
				})
			}
		}

		if runResult, err = runner.Run(0); err != nil {
			if loadLocalErrGroup != nil { // release resources
				err2 := proc.LoadLocalReader.Close()
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
		if cwft, ok := cw.(*TxnComputationWrapper); ok {
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
		err = proto.SendColumnCountPacket(colCnt)
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
			err = proto.SendColumnDefinitionPacket(requestCtx, mysqlc, int(cmd))
			if err != nil {
				return
			}
		}
		/*
			mysql COM_QUERY response: End after the column has been sent.
			send EOF packet
		*/
		err = proto.SendEOFPacketIf(0, ses.GetServerStatus())
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

		if cwft, ok := cw.(*TxnComputationWrapper); ok {
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
			err = proto.sendEOFOrOkPacket(0, ses.GetServerStatus())
			if err != nil {
				return
			}
		}
	}
	return
}

// execute query
func (mce *MysqlCmdExecutor) doComQuery(requestCtx context.Context, input *UserInput) (retErr error) {
	beginInstant := time.Now()
	requestCtx = appendStatementAt(requestCtx, beginInstant)

	ses := mce.GetSession().(*Session)
	input.genSqlSourceType(ses)
	ses.SetShowStmtType(NotShowStatement)
	proto := ses.GetMysqlProtocol()
	ses.SetSql(input.getSql())

	if judgeIsClientBIQuery(input) {
		dialectEquivalentRewrite(input)
	}

	pu := ses.GetParameterUnit()
	//the ses.GetUserName returns the user_name with the account_name.
	//here,we only need the user_name.
	userNameOnly := rootName
	proc := process.New(
		requestCtx,
		ses.GetMemPool(),
		ses.GetTxnHandler().GetTxnClient(),
		nil,
		pu.FileService,
		pu.LockService,
		pu.QueryService,
		pu.HAKeeperClient,
		pu.UdfService,
		ses.GetAutoIncrCacheManager())
	proc.CopyVectorPool(ses.proc)
	proc.CopyValueScanBatch(ses.proc)
	proc.Id = mce.getNextProcessId()
	proc.Lim.Size = pu.SV.ProcessLimitationSize
	proc.Lim.BatchRows = pu.SV.ProcessLimitationBatchRows
	proc.Lim.MaxMsgSize = pu.SV.MaxMessageSize
	proc.Lim.PartitionRows = pu.SV.ProcessLimitationPartitionRows
	proc.SessionInfo = process.SessionInfo{
		User:          ses.GetUserName(),
		Host:          pu.SV.Host,
		ConnectionID:  uint64(proto.ConnectionID()),
		Database:      ses.GetDatabaseName(),
		Version:       makeServerVersion(pu, serverVersion.Load().(string)),
		TimeZone:      ses.GetTimeZone(),
		StorageEngine: pu.StorageEngine,
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
		pu.StorageEngine,
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
			err = mce.canExecuteStatementInUncommittedTransaction(requestCtx, stmt)
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

		err = mce.executeStmt(requestCtx, ses, stmt, proc, cw, i, cws, proto, pu, tenant, userNameOnly, sqlRecord[i])
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

func (mce *MysqlCmdExecutor) executeStmtInBack(requestCtx context.Context,
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
) (err error) {
	var cmpBegin time.Time
	var ret interface{}
	var runner ComputationRunner
	var selfHandle bool
	//get errors during the transaction. rollback the transaction
	rollbackTxnFunc := func() error {
		incStatementCounter(tenant, stmt)
		incStatementErrorsCounter(tenant, stmt)
		/*
			Cases    | set Autocommit = 1/0 | BEGIN statement |
			---------------------------------------------------
			Case1      1                       Yes
			Case2      1                       No
			Case3      0                       Yes
			Case4      0                       No
			---------------------------------------------------
			update error message in Case1,Case3,Case4.
		*/
		//logError(ses, ses.GetDebugString(), err.Error())
		txnErr := backCtx.TxnRollbackSingleStatement(stmt, err)
		if txnErr != nil {
			return txnErr
		}
		return err
	}

	//execution succeeds during the transaction. commit the transaction
	commitTxnFunc := func() (retErr error) {
		// Call a defer function -- if TxnCommitSingleStatement paniced, we
		// want to catch it and convert it to an error.
		defer func() {
			if r := recover(); r != nil {
				retErr = moerr.ConvertPanicError(requestCtx, r)
			}
		}()

		//load data handle txn failure internally
		incStatementCounter(tenant, stmt)
		retErr = backCtx.TxnCommitSingleStatement(stmt)
		return
	}

	//finish the transaction
	finishTxnFunc := func() error {
		// First recover all panics.   If paniced, we will abort.
		if r := recover(); r != nil {
			err = moerr.ConvertPanicError(requestCtx, r)
		}

		if err == nil {
			err = commitTxnFunc()
			if err == nil {
				return err
			}
			// if commitTxnFunc failed, we will rollback the transaction.
		}

		err = rollbackTxnFunc()
		return err
	}

	// defer transaction state management.
	defer func() {
		err = finishTxnFunc()
	}()

	// statement management
	_, txnOp, err := backCtx.txnHandler.GetTxnOperator()
	if err != nil {
		return err
	}

	//non derived statement
	if txnOp != nil && !backCtx.IsDerivedStmt() {
		//startStatement has been called
		ok, _ := backCtx.txnHandler.calledStartStmt()
		if !ok {
			txnOp.GetWorkspace().StartStatement()
			backCtx.txnHandler.enableStartStmt(txnOp.Txn().ID)
		}
	}

	// defer Start/End Statement management, called after finishTxnFunc()
	defer func() {
		// move finishTxnFunc() out to another defer so that if finishTxnFunc
		// paniced, the following is still called.
		var err3 error
		_, txnOp, err3 = backCtx.txnHandler.GetTxnOperator()
		if err3 != nil {
			//logError(ses, ses.GetDebugString(), err3.Error())
			return
		}
		//non derived statement
		if txnOp != nil && !backCtx.IsDerivedStmt() {
			//startStatement has been called
			ok, id := backCtx.txnHandler.calledStartStmt()
			if ok && bytes.Equal(txnOp.Txn().ID, id) {
				txnOp.GetWorkspace().EndStatement()
			}
		}
		backCtx.txnHandler.disableStartStmt()
	}()

	// XXX XXX
	// I hope I can break the following code into several functions, but I can't.
	// After separating the functions, the system cannot boot, due to mo_account
	// not exists.  No clue why, the closure/capture must do some magic.

	//check transaction states
	switch stmt.(type) {
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
	switch st := stmt.(type) {
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
	stmt = cw.GetAst()
	// reset some special stmt for execute statement
	switch st := stmt.(type) {
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
	switch stmt.(type) {
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

		err = mce.executeStmtInBack(requestCtx, backCtx, stmt, proc, cw, i, cws, pu, tenant, userNameOnly, sqlRecord[i])
		if err != nil {
			return err
		}
	} // end of for

	return nil
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

func (mce *MysqlCmdExecutor) setResponse(cwIndex, cwsLen int, rspLen uint64) *Response {
	return mce.ses.SetNewResponse(OkResponse, rspLen, int(COM_QUERY), "", cwIndex, cwsLen)
}

// ExecRequest the server execute the commands from the client following the mysql's routine
func (mce *MysqlCmdExecutor) ExecRequest(requestCtx context.Context, ses *Session, req *Request) (resp *Response, err error) {
	//defer func() {
	//	if e := recover(); e != nil {
	//		moe, ok := e.(*moerr.Error)
	//		if !ok {
	//			err = moerr.ConvertPanicError(requestCtx, e)
	//			resp = NewGeneralErrorResponse(COM_QUERY, mce.ses.GetServerStatus(), err)
	//		} else {
	//			resp = NewGeneralErrorResponse(COM_QUERY, mce.ses.GetServerStatus(), moe)
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
	doComQuery := mce.GetDoQueryFunc()
	switch req.GetCmd() {
	case COM_QUIT:
		return resp, moerr.GetMysqlClientQuit()
	case COM_QUERY:
		var query = string(req.GetData().([]byte))
		mce.addSqlCount(1)
		logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(SubStringFromBegin(query, int(ses.GetParameterUnit().SV.LengthOfQueryPrinted))))
		err = doComQuery(requestCtx, &UserInput{sql: query})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_QUERY, mce.ses.GetServerStatus(), err)
		}
		return resp, nil
	case COM_INIT_DB:
		var dbname = string(req.GetData().([]byte))
		mce.addSqlCount(1)
		query := "use `" + dbname + "`"
		err = doComQuery(requestCtx, &UserInput{sql: query})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_INIT_DB, mce.ses.GetServerStatus(), err)
		}

		return resp, nil
	case COM_FIELD_LIST:
		var payload = string(req.GetData().([]byte))
		mce.addSqlCount(1)
		query := makeCmdFieldListSql(payload)
		err = doComQuery(requestCtx, &UserInput{sql: query})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_FIELD_LIST, mce.ses.GetServerStatus(), err)
		}

		return resp, nil
	case COM_PING:
		resp = NewGeneralOkResponse(COM_PING, mce.ses.GetServerStatus())

		return resp, nil

	case COM_STMT_PREPARE:
		ses.SetCmd(COM_STMT_PREPARE)
		sql = string(req.GetData().([]byte))
		mce.addSqlCount(1)

		// rewrite to "Prepare stmt_name from 'xxx'"
		newLastStmtID := ses.GenNewStmtId()
		newStmtName := getPrepareStmtName(newLastStmtID)
		sql = fmt.Sprintf("prepare %s from %s", newStmtName, sql)
		logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))

		err = doComQuery(requestCtx, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_PREPARE, mce.ses.GetServerStatus(), err)
		}
		return resp, nil

	case COM_STMT_EXECUTE:
		ses.SetCmd(COM_STMT_EXECUTE)
		data := req.GetData().([]byte)
		var prepareStmt *PrepareStmt
		sql, prepareStmt, err = mce.parseStmtExecute(requestCtx, data)
		if err != nil {
			return NewGeneralErrorResponse(COM_STMT_EXECUTE, mce.ses.GetServerStatus(), err), nil
		}
		err = doComQuery(requestCtx, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_EXECUTE, mce.ses.GetServerStatus(), err)
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
		err = mce.parseStmtSendLongData(requestCtx, data)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_SEND_LONG_DATA, mce.ses.GetServerStatus(), err)
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

		err = doComQuery(requestCtx, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_CLOSE, mce.ses.GetServerStatus(), err)
		}
		return resp, nil

	case COM_STMT_RESET:
		data := req.GetData().([]byte)

		//Payload of COM_STMT_RESET
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		stmtName := getPrepareStmtName(stmtID)
		sql = fmt.Sprintf("reset prepare %s", stmtName)
		logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))
		err = doComQuery(requestCtx, &UserInput{sql: sql})
		if err != nil {
			resp = NewGeneralErrorResponse(COM_STMT_RESET, mce.ses.GetServerStatus(), err)
		}
		return resp, nil

	case COM_SET_OPTION:
		data := req.GetData().([]byte)
		err := mce.handleSetOption(requestCtx, data)
		if err != nil {
			resp = NewGeneralErrorResponse(COM_SET_OPTION, mce.ses.GetServerStatus(), err)
		}
		return NewGeneralOkResponse(COM_SET_OPTION, mce.ses.GetServerStatus()), nil

	default:
		resp = NewGeneralErrorResponse(req.GetCmd(), mce.ses.GetServerStatus(), moerr.NewInternalError(requestCtx, "unsupported command. 0x%x", req.GetCmd()))
	}
	return resp, nil
}

func (mce *MysqlCmdExecutor) parseStmtExecute(requestCtx context.Context, data []byte) (string, *PrepareStmt, error) {
	// see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
	pos := 0
	if len(data) < 4 {
		return "", nil, moerr.NewInvalidInput(requestCtx, "sql command contains malformed packet")
	}
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmtName := fmt.Sprintf("%s_%d", prefixPrepareStmtName, stmtID)
	ses := mce.GetSession()
	preStmt, err := ses.GetPrepareStmt(stmtName)
	if err != nil {
		return "", nil, err
	}

	sql := fmt.Sprintf("execute %s", stmtName)
	logDebug(ses.(*Session), ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))
	err = ses.GetMysqlProtocol().ParseExecuteData(requestCtx, ses.GetTxnCompileCtx().GetProcess(), preStmt, data, pos)
	if err != nil {
		return "", nil, err
	}
	return sql, preStmt, nil
}

func (mce *MysqlCmdExecutor) parseStmtSendLongData(requestCtx context.Context, data []byte) error {
	// see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_send_long_data.html
	pos := 0
	if len(data) < 4 {
		return moerr.NewInvalidInput(requestCtx, "sql command contains malformed packet")
	}
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmtName := fmt.Sprintf("%s_%d", prefixPrepareStmtName, stmtID)
	ses := mce.GetSession()
	preStmt, err := ses.GetPrepareStmt(stmtName)
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("send long data for stmt %s", stmtName)
	logDebug(ses.(*Session), ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))

	err = ses.GetMysqlProtocol().ParseSendLongData(requestCtx, ses.GetTxnCompileCtx().GetProcess(), preStmt, data, pos)
	if err != nil {
		return err
	}
	return nil
}

func (mce *MysqlCmdExecutor) SetCancelFunc(cancelFunc context.CancelFunc) {
	mce.mu.Lock()
	defer mce.mu.Unlock()
	mce.cancelRequestFunc = cancelFunc
}

func (mce *MysqlCmdExecutor) Close() {}

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

func (mce *MysqlCmdExecutor) handleSetOption(ctx context.Context, data []byte) (err error) {
	if len(data) < 2 {
		return moerr.NewInternalError(ctx, "invalid cmd_set_option data length")
	}
	cap := mce.GetSession().GetMysqlProtocol().GetCapability()
	switch binary.LittleEndian.Uint16(data[:2]) {
	case 0:
		// MO do not support CLIENT_MULTI_STATEMENTS in prepare, so do nothing here(Like MySQL)
		// cap |= CLIENT_MULTI_STATEMENTS
		// mce.GetSession().GetMysqlProtocol().SetCapability(cap)

	case 1:
		cap &^= CLIENT_MULTI_STATEMENTS
		mce.GetSession().GetMysqlProtocol().SetCapability(cap)

	default:
		return moerr.NewInternalError(ctx, "invalid cmd_set_option data")
	}

	return nil
}
