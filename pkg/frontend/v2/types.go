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
	"crypto/tls"
	"sync"
	"sync/atomic"

	"github.com/fagongzi/goetty/v2"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	util2 "github.com/matrixorigin/matrixone/pkg/util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// ======= Transaction =======

type TxnOptions struct {
	begin bool //txn created by the BEGIN statement
}
type TxnOpt func(*TxnOptions)

func WithBegin(begin bool) TxnOpt {
	return func(o *TxnOptions) {
		o.begin = begin
	}
}

type Txn struct {
	storage engine.Engine
	mu      struct {
		sync.Mutex
		//the server status
		serverStatus uint16

		//the option bits
		optionBits uint32

		//start a new statement
		inStmt bool

		txnCtx    context.Context
		txnCancel context.CancelFunc
		txnOp     TxnOperator
	}

	ses *Session
}

// ======= Query Executor =======

type Stmt struct {
	uuid      uuid.UUID
	stmt      tree.Statement
	plan      *plan.Plan
	compile   *compile.Compile
	runResult *util2.RunResult
}

type QueryExecutorOptions struct {
	conn     *Connection
	endPoint *PacketEndPoint

	bSes *BackgroundSession
}

func WithConnection(conn *Connection) QueryExecutorOpt {
	return func(o *QueryExecutorOptions) {
		o.conn = conn
	}
}

func WithEndPoint(endPoint *PacketEndPoint) QueryExecutorOpt {
	return func(o *QueryExecutorOptions) {
		o.endPoint = endPoint
	}
}

func WithBackgroundSession(ses *BackgroundSession) QueryExecutorOpt {
	return func(o *QueryExecutorOptions) {
		o.bSes = ses
	}
}

type QueryExecutorOpt func(*QueryExecutorOptions)

// QueryExecutor denotes the executing environment for the query.
// It can be an explicit executor, or an implicit executor.
type QueryExecutor interface {
	Open(context.Context, ...QueryExecutorOpt) error
	Exec(context.Context, *UserInput) error
	Close(context.Context) error
}

// GeneralExecutor executes the sql regularly.
type GeneralExecutor struct {
	opts QueryExecutorOptions
}

// BackgroundExecutor executes the sql using different structure.
// it is triggered in the mo with none client.
// it does not maintain some states also.
type BackgroundExecutor struct {
	*GeneralExecutor
	opts QueryExecutorOptions
}

// ======= Statement Executor =======
type ExecutorOptions struct {
	isLastStmt bool
	ses        *Session
	proc       *process.Process
	stmt       *Stmt
	endPoint   *PacketEndPoint
}
type ExecutorOpt func(*ExecutorOptions)

func WithSession(ses *Session) ExecutorOpt {
	return func(o *ExecutorOptions) {
		o.ses = ses
	}
}

func WithProcess(proc *process.Process) ExecutorOpt {
	return func(o *ExecutorOptions) {
		o.proc = proc
	}
}

func WithStmt(stmt *Stmt) ExecutorOpt {
	return func(o *ExecutorOptions) {
		o.stmt = stmt
	}
}

func WithEndPointToExec(endPoint *PacketEndPoint) ExecutorOpt {
	return func(o *ExecutorOptions) {
		o.endPoint = endPoint
	}
}

func WithIsLastStmt(isLastStmt bool) ExecutorOpt {
	return func(o *ExecutorOptions) {
		o.isLastStmt = isLastStmt
	}
}

type Label uint32

const (
	// can be executed in the uncommitted txn
	CanExecInUncommittedTxnTxn Label = 1 << 0

	/*
		before			after
		nil				nil
		not nil			not nil (same txn)
	*/
	KeepTxnUnchangedAfterExec = 1 << 1

	/*
		before			after
		not nil			not nil
	*/
	KeepTxnNotNilAfterExec = 1 << 2
	CommitTxnBeforeExec    = 1 << 3 //for begin
	TxnExistsAferExc       = 1 << 4 //for begin
	/*
		before			after
		nil				nil
		not nil			nil
	*/
	TxnDisappearsAferExec = 1 << 5 //for commit or rollback
	CommitTxnAfterExec    = 1 << 6 //some statement must commit
	SkipStmt              = 1 << 7 //skip start/end statement
)

type Executor interface {
	// Open prepares the transaction, PacketEndpoint, etc.
	Open(context.Context, *ExecutorOptions) error
	Label() Label
	Next(context.Context, *ExecutorOptions) error
	Close(context.Context) error
}

// ======= Specific Statement Executor =======

//type SelectExecutor struct{}

// ======= Connection Related =======

type Request struct {
	ctx       Ctx
	payload   *mysqlPayload
	userInput *UserInput
}

type Ctx struct {
	sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
}

func (ctx *Ctx) Ctx() (context.Context, context.CancelFunc) {
	ctx.Lock()
	defer ctx.Unlock()
	return ctx.ctx, ctx.cancel
}

func (ctx *Ctx) SetCtx(cctx context.Context, cancel context.CancelFunc) {
	ctx.Lock()
	defer ctx.Unlock()
	ctx.ctx, ctx.cancel = cctx, cancel
}

type Connection struct {
	//====== conn ======
	connId      uint32
	client      goetty.IOSession
	peerAddress string
	thisAddress string

	// the id of goroutine that runs this conn
	goroutineID uint64

	//====== mysql protocol payload & handshake ======
	//The sequence-id is incremented with each packet and may wrap around.
	//It starts at 0 and is reset to 0 when a new command begins in the Command Phase.
	sequenceId atomic.Uint32
	//joint capability shared by the server and the client
	capability uint32
	salt       []byte

	//max packet size of the client
	maxClientPacketSize uint32

	// Connection attributes are key-value pairs that application programs
	// can pass to the server at connect time.
	connectAttrs map[string]string

	//====== ======

	connCtx context.Context
	req     Request

	mu struct {
		sync.Mutex
		connCancel       context.CancelFunc
		inProcessRequest bool
	}

	printInfoOnce       bool
	restricted          atomic.Bool
	connectionBeCounted atomic.Bool
	closeOnce           sync.Once
	cancelled           atomic.Bool

	//====== business ======
	ses *Session
}

var _ goetty.IOSessionAware = &Connections{}

/*
Connections holds all client conns in the cn.

	the KILL statement use the connectionid to kill connection or query on it.
*/
type Connections struct {
	mu struct {
		sync.RWMutex
		connId2Conn map[uint32]*Connection
		conns       map[goetty.IOSession]*Connection
	}

	ctx       context.Context
	tlsConfig *tls.Config
}

type Sessions struct {
	mu struct {
		sync.RWMutex
		seses map[uuid.UUID]*Session
	}
}

// ======= Bussiness Related =======

type Account struct{}

type Accounts struct {
	accountMu struct {
		sync.RWMutex
		accountId2Conn map[int64]map[*Connection]uint64
		//
		accountId2Account   map[int64]*Account
		accountName2Account map[string]*Account
	}

	queueMu struct {
		sync.RWMutex
		killIdQueue map[int64]KillRecord
	}
}

// ======= WriteBuffer =======

type WriteBufferOptions struct {
	conn       goetty.IOSession
	sequenceId *atomic.Uint32
}
type WriteBufferOpt func(*WriteBufferOptions)

func WithConn(conn goetty.IOSession) WriteBufferOpt {
	return func(o *WriteBufferOptions) {
		o.conn = conn
	}
}

func WithSequenceId(id *atomic.Uint32) WriteBufferOpt {
	return func(o *WriteBufferOptions) {
		o.sequenceId = id
	}
}

type WriteBuffer interface {
	Open(context.Context, ...WriteBufferOpt) error
	Write(context.Context, []byte) error
	Flush(context.Context) error
	Close(context.Context) error
}

var _ WriteBuffer = &MysqlPayloadWriteBuffer{}

/*
ConnWriteBuffer holds the bytes that will be written into the connection.
*/
type ConnWriteBuffer struct {
}

/*
MysqlPayloadWriteBuffer holds the bytes that will be encoded and written into the mysql client.
the data will split into mysql payloads.
*/
type MysqlPayloadWriteBuffer struct {
	rowHandler
	conn       goetty.IOSession
	clientAddr string
	thisAddr   string
	//The sequence-id is incremented with each packet and may wrap around.
	//It starts at 0 and is reset to 0 when a new command begins in the Command Phase.
	sequenceId      *atomic.Uint32
	maxBytesToFlush int
}

// ======= packets =======
type MysqlWritePacketOptions struct {
	serverVersionPrefix string
	salt                []byte
	//the id of the connection
	connectionID uint32

	//joint capability shared by the server and the client
	capability uint32

	affectedRows, lastInsertId uint64
	statusFlags, warnings      uint16
	message                    string

	status uint16

	errorCode              uint16
	sqlState, errorMessage string

	number uint64

	column *MysqlColumn
	cmd    int

	colDef        []*MysqlColumn
	colData       []any
	lenEncBuffer  []byte
	strconvBuffer []byte
}

type MysqlWritePacketOpt func(*MysqlWritePacketOptions)

func WithVersionPrefix(prefix string) MysqlWritePacketOpt {
	return func(o *MysqlWritePacketOptions) {
		o.serverVersionPrefix = prefix
	}
}

func WithSalt(salt []byte) MysqlWritePacketOpt {
	return func(o *MysqlWritePacketOptions) {
		o.salt = salt
	}
}

func WithConnectionID(id uint32) MysqlWritePacketOpt {
	return func(o *MysqlWritePacketOptions) {
		o.connectionID = id
	}
}

func WithCapability(capability uint32) MysqlWritePacketOpt {
	return func(o *MysqlWritePacketOptions) {
		o.capability = capability
	}
}

func WithAffectedRows(rows uint64) MysqlWritePacketOpt {
	return func(o *MysqlWritePacketOptions) {
		o.affectedRows = rows
	}
}

func WithLastInsertId(id uint64) MysqlWritePacketOpt {
	return func(o *MysqlWritePacketOptions) {
		o.lastInsertId = id
	}
}

func WithStatus(flags uint16) MysqlWritePacketOpt {
	return func(o *MysqlWritePacketOptions) {
		o.statusFlags = flags
	}
}

func WithWarnings(warnings uint16) MysqlWritePacketOpt {
	return func(o *MysqlWritePacketOptions) {
		o.warnings = warnings
	}
}

func WithMessage(message string) MysqlWritePacketOpt {
	return func(o *MysqlWritePacketOptions) {
		o.message = message
	}
}

func WithErrorCode(code uint16) MysqlWritePacketOpt {
	return func(o *MysqlWritePacketOptions) {
		o.errorCode = code
	}
}

func WithSqlState(state string) MysqlWritePacketOpt {
	return func(o *MysqlWritePacketOptions) {
		o.sqlState = state
	}
}

func WithErrorMessage(message string) MysqlWritePacketOpt {
	return func(o *MysqlWritePacketOptions) {
		o.errorMessage = message
	}
}

func WithNumber(number uint64) MysqlWritePacketOpt {
	return func(o *MysqlWritePacketOptions) {
		o.number = number
	}
}

func WithColumn(column *MysqlColumn) MysqlWritePacketOpt {
	return func(o *MysqlWritePacketOptions) {
		o.column = column
	}
}

func WithCmd(cmd int) MysqlWritePacketOpt {
	return func(o *MysqlWritePacketOptions) {
		o.cmd = cmd
	}
}

/*
MysqlWritePacket denotes the mysql packets.
*/
type MysqlWritePacket interface {
	Open(context.Context, ...MysqlWritePacketOpt) error
	Write(context.Context, WriteBuffer) error
	Close(context.Context) error
}

var _ MysqlWritePacket = &Handshake{}
var _ MysqlWritePacket = &OKPacket{}
var _ MysqlWritePacket = &OKPacketWithEOF{}
var _ MysqlWritePacket = &EOFPacket{}
var _ MysqlWritePacket = &EOFPacketIf{}
var _ MysqlWritePacket = &EOFOrOkPacket{}
var _ MysqlWritePacket = &ERRPacket{}
var _ MysqlWritePacket = &LengthEncodedNumber{}
var _ MysqlWritePacket = &ColumnDefinition{}
var _ MysqlWritePacket = &ResultSetRowText{}
var _ MysqlWritePacket = &ResultSetRowBinary{}

type MysqlReadPacketOptions struct{}
type MysqlReadPacketOpt func(*MysqlReadPacketOptions)

/*
MysqlReadPacket denotes the mysql packets sent by the client.
*/
type MysqlReadPacket interface {
	Open(context.Context, ...MysqlReadPacketOpt) error
	Read(context.Context, []byte) error
	Close(context.Context) error
}

var _ MysqlReadPacket = &HandshakeResponse{}

// packets implement both

// packets implement MysqlReadPacket
type SSLRequest struct{}

type AuthSwitchPacket struct{}

// packets implement MysqlWritePacket
type OKPacket struct {
	capability uint32
	data       []byte
}

type OKPacketWithEOF struct {
	capability uint32
	data       []byte
}

type EOFPacket struct {
	capability uint32
	data       []byte
}

type EOFPacketIf struct {
	data []byte
}

type EOFOrOkPacket struct {
	data []byte
}

type ERRPacket struct {
	capability uint32
	data       []byte
}

type LengthEncodedNumber struct {
	data []byte
}

type Handshake struct {
	serverVersionPrefix string

	//random bytes
	salt []byte

	//the id of the connection
	connectionID uint32

	//joint capability shared by the server and the client
	capability uint32

	data []byte
}

type HandshakeResponse struct {
	// opaque authentication response data generated by Authentication Method
	// indicated by the plugin name field.
	authResponse []byte

	//joint capability shared by the server and the client
	capability uint32

	//collation id
	collationID int

	//collation name
	collationName string

	//character set
	charset string

	//max packet size of the client
	maxClientPacketSize uint32

	//the user of the client
	username string

	//the default database for the client
	database string

	// Connection attributes are key-value pairs that application programs
	// can pass to the server at connect time.
	connectAttrs map[string]string

	isAskForTlsHeader bool

	//TODO: change authe method later
	needChangAuthMethod bool
}

type AuthSwitchRequest struct{}

type AuthMoreData struct{}

type ResultSetRowText struct {
	colDef        []*MysqlColumn
	colData       []any
	lenEncBuffer  []byte
	strconvBuffer []byte
}

type ResultSetRowBinary struct {
	colDef           []*MysqlColumn
	colData          []any
	lenEncBuffer     []byte
	strconvBuffer    []byte
	binaryNullBuffer []byte
}

type ColumnDefinition struct {
	data []byte
}

/*
PacketEndPoint reads & writes the packets.
*/
type PacketEndPoint struct {
	option goetty.ReadOptions
	conn   goetty.IOSession
	out    WriteBuffer
	//The sequence-id is incremented with each packet and may wrap around.
	//It starts at 0 and is reset to 0 when a new command begins in the Command Phase.
	sequenceId *atomic.Uint32
}

// ======= reader & writer =======

// type Chunk interface{}
// type Chunks interface{}

// var _ Chunk = &vector.Vector{}
// var _ Chunks = &batch.Batch{}

type Chunk *vector.Vector
type Chunks *batch.Batch

type ReaderOptions struct{}
type ReaderOpt func(*ReaderOptions)

type Reader interface {
	Open(context.Context, ...ReaderOpt) error
	Reader(context.Context, Chunks) error
	Close(context.Context) error
}

type WriterOptions struct{}
type WriterOpt func(*WriterOptions)

/*
Writer writes the Chunks generated from the computation engine
to the destination.
*/
type Writer interface {
	Open(context.Context, ...WriterOpt) error
	Write(context.Context, Chunks) error
	WriteBytes(context.Context, []byte) error
	Flush(context.Context) error
	Close(context.Context) error
}

/*
ChunksWriter writes the Chunks into the destination.
The default implementation of the Writer.
*/
type ChunksWriter struct {
	ses      *Session
	endPoint *PacketEndPoint
	row      []any
	writeRow func(ctx context.Context, row []any) error
}

/*
S3Writer writes the Chunks into the S3.
*/
type S3Writer struct {
	*ChunksWriter
}

/*
MysqlFormatWriter convertes the Chunks to the mysql format
and writes them into the destination.
*/
type MysqlFormatWriter struct {
	*ChunksWriter
	textRow          *ResultSetRowText
	binRow           *ResultSetRowBinary
	isBin            bool
	colDef           []*MysqlColumn
	lenEncBuffer     []byte
	strconvBuffer    []byte
	binaryNullBuffer []byte
}

/*
CsvFormatWriter writes the Chunks into the csv file.
*/
type CsvFormatWriter struct {
	*ChunksWriter
}
