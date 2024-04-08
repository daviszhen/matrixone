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
	"context"
	"io"
	"time"

	"github.com/fagongzi/goetty/v2/buf"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/clock"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	"github.com/matrixorigin/matrixone/pkg/util"
	util2 "github.com/matrixorigin/matrixone/pkg/util"
)

const (
	DefaultRpcBufferSize = 1 << 10
)

type (
	TxnOperator = client.TxnOperator
	TxnClient   = client.TxnClient
	TxnOption   = client.TxnOption
)

type ComputationRunner interface {
	Run(ts uint64) (*util.RunResult, error)
}

// ComputationWrapper is the wrapper of the computation
type ComputationWrapper interface {
	ComputationRunner
	GetAst() tree.Statement

	GetProcess() *process.Process

	GetColumns() ([]interface{}, error)

	Compile(requestCtx context.Context, u interface{}, fill func(interface{}, *batch.Batch) error) (interface{}, error)

	GetUUID() []byte

	RecordExecPlan(ctx context.Context) error

	GetLoadTag() bool

	GetServerStatus() uint16
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

type ColumnInfo interface {
	GetName() string

	GetType() types.T
}

var _ ColumnInfo = &engineColumnInfo{}

type TableInfo interface {
	GetColumns()
}

type engineColumnInfo struct {
	name string
	typ  types.Type
}

func (ec *engineColumnInfo) GetName() string {
	return ec.name
}

func (ec *engineColumnInfo) GetType() types.T {
	return ec.typ.Oid
}

type PrepareStmt struct {
	Name           string
	Sql            string
	PreparePlan    *plan.Plan
	PrepareStmt    tree.Statement
	ParamTypes     []byte
	IsInsertValues bool
	InsertBat      *batch.Batch
	proc           *process.Process

	exprList [][]colexec.ExpressionExecutor

	params              *vector.Vector
	getFromSendLongData map[int]struct{}
}

/*
Disguise the COMMAND CMD_FIELD_LIST as sql query.
*/
const (
	cmdFieldListSql    = "__++__internal_cmd_field_list"
	cmdFieldListSqlLen = len(cmdFieldListSql)
	cloudUserTag       = "cloud_user"
	cloudNoUserTag     = "cloud_nonuser"
	saveResultTag      = "save_result"
)

var _ tree.Statement = &InternalCmdFieldList{}

// InternalCmdFieldList the CMD_FIELD_LIST statement
type InternalCmdFieldList struct {
	tableName string
}

func (icfl *InternalCmdFieldList) String() string {
	return makeCmdFieldListSql(icfl.tableName)
}

func (icfl *InternalCmdFieldList) Format(ctx *tree.FmtCtx) {
	ctx.WriteString(makeCmdFieldListSql(icfl.tableName))
}

func (icfl *InternalCmdFieldList) ResultType() tree.ResultType {
	return tree.Status
}

func (si *InternalCmdFieldList) HandleType() tree.HandleType {
	return tree.InFrontend
}

func (icfl *InternalCmdFieldList) GetStatementType() string { return "InternalCmd" }
func (icfl *InternalCmdFieldList) GetQueryType() string     { return tree.QueryTypeDQL }

// ExecResult is the result interface of the execution
type ExecResult interface {
	GetRowCount() uint64

	GetString(ctx context.Context, rindex, cindex uint64) (string, error)

	GetUint64(ctx context.Context, rindex, cindex uint64) (uint64, error)

	GetInt64(ctx context.Context, rindex, cindex uint64) (int64, error)
}

func execResultArrayHasData(arr []ExecResult) bool {
	return len(arr) != 0 && arr[0].GetRowCount() != 0
}

// BackgroundExec executes the sql in background session without network output.
type BackgroundExec interface {
	Close()
	Exec(context.Context, string) error
	ExecStmt(context.Context, tree.Statement) error
	GetExecResultSet() []interface{}
	ClearExecResultSet()

	GetExecResultBatches() []*batch.Batch
	ClearExecResultBatches()
	Clear()
}

var _ BackgroundExec = &backExec{}

type unknownStatementType struct {
	tree.StatementType
}

func (unknownStatementType) GetStatementType() string { return "Unknown" }
func (unknownStatementType) GetQueryType() string     { return tree.QueryTypeOth }

func getStatementType(stmt tree.Statement) tree.StatementType {
	switch stmt.(type) {
	case tree.StatementType:
		return stmt
	default:
		return unknownStatementType{}
	}
}

// TableInfoCache tableInfos of a database
type TableInfoCache struct {
	db         string
	tableInfos map[string][]ColumnInfo
}

// outputPool outputs the data
type outputPool interface {
	resetLineStr()

	reset()

	getEmptyRow() ([]interface{}, error)

	flush() error
}

func (prepareStmt *PrepareStmt) Close() {
	if prepareStmt.params != nil {
		prepareStmt.params.Free(prepareStmt.proc.Mp())
	}
	if prepareStmt.InsertBat != nil {
		prepareStmt.InsertBat.SetCnt(1)
		prepareStmt.InsertBat.Clean(prepareStmt.proc.Mp())
		prepareStmt.InsertBat = nil
	}
	if prepareStmt.exprList != nil {
		for _, exprs := range prepareStmt.exprList {
			for _, expr := range exprs {
				expr.Free()
			}
		}
	}
}

var _ buf.Allocator = &SessionAllocator{}

type SessionAllocator struct {
	mp *mpool.MPool
}

func NewSessionAllocator(pu *config.ParameterUnit) *SessionAllocator {
	pool, err := mpool.NewMPool("frontend-goetty-pool-cn-level", pu.SV.GuestMmuLimitation, mpool.NoFixed)
	if err != nil {
		panic(err)
	}
	ret := &SessionAllocator{mp: pool}
	return ret
}

func (s *SessionAllocator) Alloc(capacity int) []byte {
	alloc, err := s.mp.Alloc(capacity)
	if err != nil {
		panic(err)
	}
	return alloc
}

func (s SessionAllocator) Free(bs []byte) {
	s.mp.Free(bs)
}

var _ FeSession = &Session{}
var _ FeSession = &backSession{}

type FeSession interface {
	GetRequestContext() context.Context
	GetTimeZone() *time.Location
	GetStatsCache() *plan2.StatsCache
	GetUserName() string
	GetSql() string
	GetAccountId() uint32
	GetTenantInfo() *TenantInfo
	GetStorage() engine.Engine
	GetBackgroundExec(ctx context.Context) BackgroundExec
	GetRawBatchBackgroundExec(ctx context.Context) BackgroundExec
	getGlobalSystemVariableValue(name string) (interface{}, error)
	GetSessionVar(name string) (interface{}, error)
	GetUserDefinedVar(name string) (SystemVariableType, *UserDefinedVar, error)
	GetConnectContext() context.Context
	IfInitedTempEngine() bool
	GetTempTableStorage() *memorystorage.Storage
	GetDebugString() string
	GetFromRealUser() bool
	getLastCommitTS() timestamp.Timestamp
	GetTenantName() string
	SetTxnId(i []byte)
	GetTxnId() uuid.UUID
	GetStmtId() uuid.UUID
	GetSqlOfStmt() string
	updateLastCommitTS(ts timestamp.Timestamp)
	GetMysqlProtocol() MysqlProtocol
	GetTxnHandler() *TxnHandler
	GetDatabaseName() string
	SetDatabaseName(db string)
	GetMysqlResultSet() *MysqlResultSet
	GetGlobalVar(name string) (interface{}, error)
	SetNewResponse(category int, affectedRows uint64, cmd int, d interface{}, isLastStmt bool) *Response
	GetTxnCompileCtx() *TxnCompilerContext
	GetCmd() CommandType
	IsBackgroundSession() bool
	GetPrepareStmt(name string) (*PrepareStmt, error)
	CountPayload(i int)
	RemovePrepareStmt(name string)
	SetShowStmtType(statement ShowStatementType)
	SetSql(sql string)
	GetMemPool() *mpool.MPool
	GetProc() *process.Process
	GetLastInsertID() uint64
	GetSqlHelper() *SqlHelper
	GetBuffer() *buffer.Buffer
	GetStmtProfile() *process.StmtProfile
	CopySeqToProc(proc *process.Process)
	getQueryId(internal bool) []string
	SetMysqlResultSet(mrs *MysqlResultSet)
	GetConnectionID() uint32
	SetRequestContext(ctx context.Context)
	IsDerivedStmt() bool
	SetAccountId(uint32)
	SetPlan(plan *plan.Plan)
	SetData([][]interface{})
	GetIsInternal() bool
	getCNLabels() map[string]string
	SetTempTableStorage(getClock clock.Clock) (*metadata.TNService, error)
	SetTempEngine(ctx context.Context, te engine.Engine) error
	EnableInitTempEngine()
	GetUpstream() FeSession
	cleanCache()
	getNextProcessId() string
	GetSqlCount() uint64
	addSqlCount(a uint64)
	GetStmtInfo() *motrace.StatementInfo
	GetTxnInfo() string
	GetUUID() []byte
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
