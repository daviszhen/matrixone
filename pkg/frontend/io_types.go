// Copyright 2023 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

type PacketIO struct {
	out WriteBuffer
}

type WriteBufferOptions struct {
	proto *MysqlProtocolImpl
}

type WriteBuffer interface {
	Open(context.Context, *WriteBufferOptions) error
	Write(context.Context, []byte, *WriteBufferOptions) error
	Flush(context.Context) error
	Close(context.Context) error
}

var _ WriteBuffer = &MysqlPayloadWriteBuffer{}

/*
MysqlPayloadWriteBuffer holds the bytes that will be encoded and written into the mysql client.
the data will split into mysql payloads.
*/
type MysqlPayloadWriteBuffer struct {
	proto *MysqlProtocolImpl
}

type MysqlWritePacketOptions struct {
	flush bool
	len   uint64

	column     Column
	capability uint32
	cmd        int
}

/*
MysqlWritePacket denotes the mysql packets.
*/
type MysqlWritePacket interface {
	Open(context.Context, *MysqlWritePacketOptions) error
	Write(context.Context, WriteBuffer, *MysqlWritePacketOptions) error
	Close(context.Context) error
}

var _ MysqlWritePacket = &ResultSetRowText{}
var _ MysqlWritePacket = &ResultSetRowBinary{}
var _ MysqlWritePacket = &LenEnc{}
var _ MysqlWritePacket = &ColumnDef{}

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

type LenEnc struct {
	buf [10]byte
}

type ColumnDef struct {
}

type Chunk *vector.Vector
type Chunks *batch.Batch

type WriterOptions struct {
	ses      *Session
	writeRow func(ctx context.Context, row []any, opts *WriterOptions) error
	packetIO *PacketIO
	colDef   []*MysqlColumn
	isBinary bool
}

/*
Writer writes the Chunks generated from the computation engine
to the destination.
*/
type Writer interface {
	Open(context.Context, *WriterOptions) error
	Write(context.Context, Chunks, *WriterOptions) error
	WriteBytes(context.Context, []byte, *WriterOptions) error
	Flush(context.Context) error
	Close(context.Context) error
}

/*
ChunksWriter writes the Chunks into the destination.
The default implementation of the Writer.
*/
type ChunksWriter struct {
	row           []any
	needCopyBytes bool
}

/*
MysqlRowWriter converts the Chunks to the mysql format
and writes them into the destination.
*/
type MysqlRowWriter struct {
	*ChunksWriter
	packetIO         *PacketIO
	textRow          *ResultSetRowText
	binRow           *ResultSetRowBinary
	lenEncBuffer     []byte
	strconvBuffer    []byte
	binaryNullBuffer []byte
	opts             *MysqlWritePacketOptions
}

/*
TableStatusWriter holds the data generated from the ShowTableStatus statement.
It will be regrouped again later.
*/
type TableStatusWriter struct {
	*ChunksWriter
	ses *Session
}
