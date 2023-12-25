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

package v2

import (
	"context"

	"github.com/fagongzi/goetty/v2"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// ======= Transaction =======

type TxnOptions struct{}
type TxnOpt func(*TxnOptions)

type Txn struct {
	txnOperator TxnOperator
	txnCtx      context.Context
	txnCancel   context.CancelFunc

	//the server status
	serverStatus uint16

	//the option bits
	optionBits uint32
}

// ======= Query Executor =======

type QueryExecutorOptions struct{}
type QueryExecutorOpt func(*QueryExecutorOptions)

// QueryExecutor denotes the executing environment for the query.
// It can be an explicit executor, or an implicit executor.
type QueryExecutor interface {
	Open(context.Context, ...QueryExecutorOpt) error
	Exec(context.Context, string) error
	Close(context.Context) error
}

// MyqlExecutor executes the sql regularly.
type MyqlExecutor struct{}

// BackgroundExecutor executes the sql using different structure.
// it is triggered in the mo with none client.
// it does not maintain some states also.
type BackgroundExecutor struct{}

// ======= Statement Executor =======
type ExecutorOptions struct{}
type ExecutorOpt func(*ExecutorOptions)

type ExecutorTxnKind uint32

const (
	ExecDoesNotNeedTxn        ExecutorTxnKind = 1 << 0
	KeepTxnUnchangedAfterExec                 = 1 << 1
	KeepTxnNotNilAfterExec                    = 1 << 2
	CommitTxnBeforeExec                       = 1 << 3 //for begin
	TxnExistsAferExc                          = 1 << 4 //for begin
	TxnDisappearAferExec                      = 1 << 5 //for commit or rollback
)

type Executor interface {
	// Open prepares the transaction, PacketEndpoint, etc.
	Open(context.Context, ...ExecutorOpt) error
	Label() ExecutorTxnKind
	Next(context.Context) error
	Close(context.Context) error
}

// ======= Specific Statement Executor =======

//type SelectExecutor struct{}

// ======= Connection Related =======

type RequestCtx struct {
	reqCtx    context.Context
	reqCancel context.CancelFunc
}

type Connection struct {
	client      goetty.IOSession
	ses         *Session
	peerAddress string
	thisAddress string
	// the id of goroutine that executes the request
	goroutineID uint64
	connCtx     context.Context
	connCancel  context.CancelFunc
}

/*
Connections holds all client conns in the cn.

	the KILL statement use the connectionid to kill connection or query on it.
*/
type Connections struct {
	connId2Conn map[uint32]*Connection
	conns       map[goetty.IOSession]*Connection
}

type Sessions struct {
	ses map[uuid.UUID]*Session
}

// ======= Bussiness Related =======

type Account struct{}

type Accounts struct {
	accountId2Conn      map[int64]map[*Connection]uint64
	accountId2Account   map[int64]*Account
	accountName2Account map[string]*Account
	killIdQueue         map[int64]KillRecord
}

// ======= WriteBuffer =======

type WriteBufferOptions struct{}
type WriteBufferOpt func(*WriteBufferOptions)

type WriteBuffer interface {
	Open(context.Context, ...WriteBufferOpt) error
	Write(context.Context, []byte) error
	Close(context.Context) error
	Flush(context.Context) error
}

/*
BytesWriteBuffer holds bytes.
The default implementation of the WriteBuffer.
*/
type BytesWriteBuffer struct {
	buf []byte
}

/*
ConnWriteBuffer holds the bytes that will be written into the connection.
*/
type ConnWriteBuffer struct {
	*BytesWriteBuffer
	//TODO: conn
	clientAddr string
	thisAddr   string
}

/*
MysqlPayloadWriteBuffer holds the bytes that will be encoded and written into the mysql client.
the data will split into mysql payloads.
*/
type MysqlPayloadWriteBuffer struct {
	*BytesWriteBuffer

	out WriteBuffer
}

// ======= packets =======
type MysqlWritePacketOptions struct{}
type MysqlWritePacketOpt func(*MysqlWritePacketOptions)

/*
MysqlWritePacket denotes the mysql packets.
*/
type MysqlWritePacket interface {
	Open(context.Context, ...MysqlWritePacketOpt) error
	Write(context.Context, WriteBuffer) error
	Close(context.Context) error
}

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

// packets implement both

// packets implement MysqlReadPacket
type SSLRequest struct{}

type AuthSwitchPacket struct{}

// packets implement MysqlWritePacket
type OKPacket struct{}

type EOFPacket struct{}

type ERRPacket struct{}

type LengthEncodedNumber struct{}

type Handshake struct{}

type HandshakeResponse struct{}

type AuthSwitchRequest struct{}

type AuthMoreData struct{}

type ResultSetRowText struct{}

type ResultSetRowBinary struct{}

type ColumnDefinition struct{}

/*
PacketEndPoint reads & writes the packets.
*/
type PacketEndPoint struct {
	out WriteBuffer
}

/*
SendPacket sends the packet to the mysql client.
*/
func (*PacketEndPoint) SendPacket(context.Context, MysqlWritePacket, bool) error {

	return nil
}

// ======= reader & writer =======

type Chunk interface{}
type Chunks interface{}

var _ Chunk = &vector.Vector{}
var _ Chunks = &batch.Batch{}

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
	Close(context.Context) error
	Flush(context.Context) error
}

/*
ChunksWriter writes the Chunks into the destination.
The default implementation of the Writer.
*/
type ChunksWriter struct {
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
}

/*
CsvFormatWriter writes the Chunks into the csv file.
*/
type CsvFormatWriter struct {
	*ChunksWriter
}
