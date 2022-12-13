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
	"github.com/fagongzi/goetty/v2"
	"math"
)

// Response Categories
const (
	// OkResponse OK message
	OkResponse = iota
	// ErrorResponse Error message
	ErrorResponse
	// EoFResponse EOF message
	EoFResponse
	// ResultResponse result message
	ResultResponse
)

type Request struct {
	//the command type from the client
	cmd CommandType
	// sequence num
	seq uint8
	//the data from the client
	data interface{}
}

func (req *Request) GetData() interface{} {
	return req.data
}

func (req *Request) SetData(data interface{}) {
	req.data = data
}

func (req *Request) GetCmd() CommandType {
	return req.cmd
}

func (req *Request) SetCmd(cmd CommandType) {
	req.cmd = cmd
}

type Response struct {
	//the category of the response
	category int
	//the status of executing the peer request
	status uint16
	//the command type which generates the response
	cmd int
	//the data of the response
	data interface{}

	/*
		ok response
	*/
	affectedRows, lastInsertId uint64
	warnings                   uint16
}

func NewResponse(category int, status uint16, cmd int, d interface{}) *Response {
	return &Response{
		category: category,
		status:   status,
		cmd:      cmd,
		data:     d,
	}
}

func NewGeneralErrorResponse(cmd CommandType, err error) *Response {
	return NewResponse(ErrorResponse, 0, int(cmd), err)
}

func NewGeneralOkResponse(cmd CommandType) *Response {
	return NewResponse(OkResponse, 0, int(cmd), nil)
}

func NewOkResponse(affectedRows, lastInsertId uint64, warnings, status uint16, cmd int, d interface{}) *Response {
	resp := &Response{
		category:     OkResponse,
		status:       status,
		cmd:          cmd,
		data:         d,
		affectedRows: affectedRows,
		lastInsertId: lastInsertId,
		warnings:     warnings,
	}

	return resp
}

func (resp *Response) GetData() interface{} {
	return resp.data
}

func (resp *Response) SetData(data interface{}) {
	resp.data = data
}

func (resp *Response) GetStatus() uint16 {
	return resp.status
}

func (resp *Response) SetStatus(status uint16) {
	resp.status = status
}

func (resp *Response) GetCategory() int {
	return resp.category
}

func (resp *Response) SetCategory(category int) {
	resp.category = category
}

var _ MysqlProtocol = &FakeProtocol{}

const (
	fakeConnectionID uint32 = math.MaxUint32
)

// FakeProtocol works for the background transaction that does not use the network protocol.
type FakeProtocol struct {
	username string
	database string
}

func (fp *FakeProtocol) GetCapability() uint32 {
	return DefaultCapability
}

func (fp *FakeProtocol) IsTlsEstablished() bool {
	return true
}

func (fp *FakeProtocol) SetTlsEstablished() {

}

func (fp *FakeProtocol) HandleHandshake(ctx context.Context, payload []byte) (bool, error) {
	return false, nil
}

func (fp *FakeProtocol) GetTcpConnection() goetty.IOSession {
	return nil
}

func (fp *FakeProtocol) GetConciseProfile() string {
	return "fake protocol"
}

func (fp *FakeProtocol) GetSequenceId() uint8 {
	return 0
}

func (fp *FakeProtocol) SetSequenceID(value uint8) {
}

func (fp *FakeProtocol) makeProfile(profileTyp profileType) {
}

func (fp *FakeProtocol) getProfile(profileTyp profileType) string {
	return ""
}

func (fp *FakeProtocol) SendPrepareResponse(ctx context.Context, stmt *PrepareStmt) error {
	return nil
}

func (fp *FakeProtocol) ParseExecuteData(ctx context.Context, stmt *PrepareStmt, data []byte, pos int) (names []string, vars []any, err error) {
	return nil, nil, nil
}

func (fp *FakeProtocol) SendResultSetTextBatchRow(mrs *MysqlResultSet, cnt uint64) error {
	return nil
}

func (fp *FakeProtocol) SendResultSetTextBatchRowSpeedup(mrs *MysqlResultSet, cnt uint64) error {
	return nil
}

func (fp *FakeProtocol) SendColumnDefinitionPacket(ctx context.Context, column Column, cmd int) error {
	return nil
}

func (fp *FakeProtocol) SendColumnCountPacket(count uint64) error {
	return nil
}

func (fp *FakeProtocol) SendEOFPacketIf(warnings uint16, status uint16) error {
	return nil
}

func (fp *FakeProtocol) sendOKPacket(affectedRows uint64, lastInsertId uint64, status uint16, warnings uint16, message string) error {
	return nil
}

func (fp *FakeProtocol) sendEOFOrOkPacket(warnings uint16, status uint16) error {
	return nil
}

func (fp *FakeProtocol) ResetStatistics() {}

func (fp *FakeProtocol) GetStats() string {
	return ""
}

func (fp *FakeProtocol) IsEstablished() bool {
	return true
}

func (fp *FakeProtocol) SetEstablished() {}

func (fp *FakeProtocol) GetRequest(payload []byte) *Request {
	return nil
}

func (fp *FakeProtocol) SendResponse(ctx context.Context, resp *Response) error {
	return nil
}

func (fp *FakeProtocol) ConnectionID() uint32 {
	return fakeConnectionID
}

func (fp *FakeProtocol) Peer() (string, string, string, string) {
	return "", "", "", ""
}

func (fp *FakeProtocol) GetDatabaseName() string {
	return fp.database
}

func (fp *FakeProtocol) SetDatabaseName(s string) {
	fp.database = s
}

func (fp *FakeProtocol) GetUserName() string {
	return fp.username
}

func (fp *FakeProtocol) SetUserName(s string) {
	fp.username = s
}

func (fp *FakeProtocol) Quit() {}
