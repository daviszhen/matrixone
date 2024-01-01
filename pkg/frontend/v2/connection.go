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
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

var (
	feConns *Connections
)

func init() {
	feConns = NewConnections()
}

func NewConnections() *Connections {
	conns := &Connections{}
	conns.mu.connId2Conn = make(map[uint32]*Connection)
	conns.mu.conns = make(map[goetty.IOSession]*Connection)
	return conns
}

func (conns *Connections) Created(rs goetty.IOSession) {
	logutil.Debugf("get the connection from %s", rs.RemoteAddress())
	conn := &Connection{
		client:     rs,
		salt:       generate_salt(20),
		capability: DefaultCapability,
	}

	var cancel context.CancelFunc
	conn.connCtx, cancel = context.WithCancel(conns.ctx)
	conn.setConnCancel(cancel)
	//send handshake to the client
	connId, err := getConnID(conn.connCtx, fePu.HAKeeperClient)
	if err != nil {
		logutil.Errorf("failed to get connection ID from HAKeeper: %v", err)
		return
	}
	conn.connId = connId

	// XXX MPOOL pass in a nil mpool.
	// XXX MPOOL can choose to use a Mid sized mpool, if, we know
	// this mpool will be deleted.  Maybe in the following Closed method.
	ses := NewSession(conn, nil, GSysVariables, true)
	ses.SetFromRealUser(true)
	conn.ses = ses
	if feBaseService != nil {
		conn.connCtx = context.WithValue(conn.connCtx, defines.NodeIDKey{}, feBaseService.ID())
	}
	ses.timestampMap[TSCreatedStart] = time.Now()
	defer func() {
		ses.timestampMap[TSCreatedEnd] = time.Now()
		v2.CreatedDurationHistogram.Observe(float64(ses.timestampMap[TSCreatedEnd].Sub(ses.timestampMap[TSCreatedStart]).Milliseconds()))
	}()

	// With proxy module enabled, we try to update salt value and label info from proxy.
	if fePu.SV.ProxyEnabled {
		//FIXME: update salt value and label info from proxy
		// pro.receiveExtraInfo(rs)
	}
	conns.saveConn(conn)
}

/*
When the io is closed, the Closed will be called.
*/
func (conns *Connections) Closed(rs goetty.IOSession) {
	logutil.Debugf("clean resource of the connection %d:%s", rs.ID(), rs.RemoteAddress())
	defer func() {
		logutil.Debugf("resource of the connection %d:%s has been cleaned", rs.ID(), rs.RemoteAddress())
	}()
	conn := conns.deleteConn(rs)

	if conn != nil {
		ses := conn.ses
		if ses != nil {
			conn.decreaseCount(func() {
				account := ses.GetTenantInfo()
				accountName := sysAccountName
				if account != nil {
					accountName = account.GetTenant()
				}
				metric.ConnectionCounter(accountName).Dec()
				feAccounts.deleteConn(int64(account.GetTenantID()), conn)
			})
			feSessionManager.RemoveSession(ses)
			logDebugf(ses.GetDebugString(), "the io session was closed.")
		}
		conn.cleanup()
	}
}

func (conns *Connections) Handler(rs goetty.IOSession, msg interface{}, received uint64) error {
	return nil
}

func (conns *Connections) getConn(rs goetty.IOSession) *Connection {
	conns.mu.RLock()
	defer conns.mu.RUnlock()
	return conns.mu.conns[rs]
}

func (conns *Connections) saveConn(conn *Connection) {
	conns.mu.Lock()
	defer conns.mu.Unlock()
	conns.mu.conns[conn.client] = conn
	conns.mu.connId2Conn[conn.connId] = conn
}

func (conns *Connections) deleteConn(rs goetty.IOSession) *Connection {
	var conn *Connection
	var ok bool
	conns.mu.Lock()
	defer conns.mu.Unlock()
	conn, ok = conns.mu.conns[rs]
	if ok {
		delete(conns.mu.conns, rs)
	}
	delete(conns.mu.connId2Conn, conn.connId)
	return conn
}

func getConnID(ctx context.Context, hkClient logservice.CNHAKeeperClient) (uint32, error) {
	// Only works in unit test.
	if hkClient == nil {
		return nextConnectionID(), nil
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second*2)
	defer cancel()
	connID, err := hkClient.AllocateIDByKey(ctx, ConnIDAllocKey)
	if err != nil {
		return 0, err
	}
	// Convert uint64 to uint32 to adapt MySQL protocol.
	return uint32(connID), nil
}

func (conn *Connection) consumeHandshakeRsp(handrsp *HandshakeResponse) {
	conn.capability = handrsp.capability

	conn.maxClientPacketSize = handrsp.maxClientPacketSize
	conn.connectAttrs = handrsp.connectAttrs

	conn.ses.collationID = handrsp.collationID
	conn.ses.collationName = handrsp.collationName
	conn.ses.charset = handrsp.charset
	conn.ses.SetUserName(handrsp.username)
	conn.ses.SetDatabaseName(handrsp.database)
}

func (conn *Connection) run() error {
	payloadWriter := &MysqlPayloadWriteBuffer{}
	err := payloadWriter.Open(conn.connCtx,
		WithConn(conn.client),
		WithSequenceId(&conn.sequenceId),
	)
	if err != nil {
		return err
	}
	endPoint := &PacketEndPoint{
		conn:       conn.client,
		out:        payloadWriter,
		sequenceId: &conn.sequenceId,
	}

	hand := &Handshake{}
	err = hand.Open(conn.connCtx,
		WithVersionPrefix(fePu.SV.ServerVersionPrefix),
		WithSalt(conn.salt),
		WithConnectionID(conn.connId),
		WithCapability(conn.capability),
	)
	if err != nil {
		return err
	}
	err = endPoint.SendPacket(conn.connCtx, hand, true)
	if err != nil {
		return err
	}

	//receive handshake response
	handrsp := &HandshakeResponse{
		capability: conn.capability,
	}
	handrsp.Open(conn.connCtx)
	defer handrsp.Close(conn.connCtx)
	err = endPoint.ReceivePacket(conn.connCtx, handrsp)
	if err != nil {
		return err
	}

	//SSL/TLS
	if handrsp.isAskForTlsHeader {
		//TODO:
	}

	if handrsp.needChangAuthMethod {
		//TODO: change auth method
	}

	//update conn
	conn.consumeHandshakeRsp(handrsp)

	//default account
	reqCtx, reqCancel := context.WithTimeout(conn.connCtx, fePu.SV.SessionTimeout.Duration)
	reqCtx = context.WithValue(reqCtx, defines.TenantIDKey{}, uint32(sysAccountID))
	reqCtx = context.WithValue(reqCtx, defines.UserIDKey{}, uint32(rootID))
	reqCtx = context.WithValue(reqCtx, defines.RoleIDKey{}, uint32(moAdminRoleID))
	conn.req.ctx.SetCtx(reqCtx, reqCancel)

	//authen user
	err = conn.authen(endPoint, handrsp.authResponse)
	if err != nil {
		return err
	}

	handrsp.Close(conn.connCtx)
	var payload *mysqlPayload

	for {
		payload, err = endPoint.ReceivePayload(conn.connCtx)
		if err != nil {
			return err
		}
		if payload == nil {
			break
		}

		if payload.GetCmd() == COM_QUIT {
			break
		}

		reqCtx, reqCancel := context.WithTimeout(conn.connCtx, fePu.SV.SessionTimeout.Duration)
		if feBaseService != nil {
			reqCtx = context.WithValue(reqCtx, defines.NodeIDKey{}, feBaseService.ID())
		}
		tenant := conn.ses.GetTenantInfo()
		reqCtx = context.WithValue(reqCtx, defines.TenantIDKey{}, tenant.GetTenantID())
		reqCtx = context.WithValue(reqCtx, defines.UserIDKey{}, tenant.GetUserID())
		reqCtx = context.WithValue(reqCtx, defines.RoleIDKey{}, tenant.GetDefaultRoleID())
		conn.req.ctx.SetCtx(reqCtx, reqCancel)
		conn.req.payload = payload

		err = conn.handleCommand(endPoint)
		if err != nil {
			return err
		}
	}

	return err
}

// the server get the auth string from HandShakeResponse
// pwd is SHA1(SHA1(password)), AUTH is from client
// hash1 = AUTH XOR SHA1( slat + pwd)
// hash2 = SHA1(hash1)
// check(hash2, hpwd)
func checkPassword(pwd, salt, auth []byte) bool {
	sha := sha1.New()
	_, err := sha.Write(salt)
	if err != nil {
		logutil.Errorf("SHA1(salt) failed.")
		return false
	}
	_, err = sha.Write(pwd)
	if err != nil {
		logutil.Errorf("SHA1(hpwd) failed.")
		return false
	}
	hash1 := sha.Sum(nil)

	if len(auth) != len(hash1) {
		return false
	}

	for i := range hash1 {
		hash1[i] ^= auth[i]
	}

	hash2 := HashSha1(hash1)
	return bytes.Equal(pwd, hash2)
}

// the server authenticate that the client can connect and use the database
func (conn *Connection) authenticateUser(authrsp []byte) error {
	var psw []byte
	var err error
	var tenant *TenantInfo

	ses := conn.ses
	if !fePu.SV.SkipCheckUser {
		// logDebugf(mp.getDebugStringUnsafe(), "authenticate user 1")
		psw, err = ses.AuthenticateUser(conn.connCtx, ses.GetUserName(), ses.GetDatabaseName(), authrsp, conn.salt, checkPassword)
		if err != nil {
			return err
		}
		// logDebugf(mp.getDebugStringUnsafe(), "authenticate user 2")

		//TO Check password
		if checkPassword(psw, conn.salt, authrsp) {
			// logDebugf(mp.getDebugStringUnsafe(), "check password succeeded")
			ses.InitGlobalSystemVariables()
		} else {
			return moerr.NewInternalError(conn.connCtx, "check password failed")
		}
	} else {
		// logDebugf(mp.getDebugStringUnsafe(), "skip authenticate user")
		//Get tenant info
		tenant, err = GetTenantInfo(conn.connCtx, ses.GetUserName())
		if err != nil {
			return err
		}

		if ses != nil {
			ses.SetTenantInfo(tenant)

			//TO Check password
			if len(psw) == 0 || checkPassword(psw, conn.salt, authrsp) {
				// logInfo(mp.ses, mp.ses.GetDebugString(), "check password succeeded")
			} else {
				return moerr.NewInternalError(conn.connCtx, "check password failed")
			}
		}
	}
	return nil
}

/*
the server sends the Error packet

information from https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
mysql version 8.0.23
usually it is in the directory /usr/local/include/mysql/mysqld_error.h

Error information includes several elements: an error code, SQLSTATE value, and message string.

	Error code: This value is numeric. It is MySQL-specific and is not portable to other database systems.
	SQLSTATE value: This value is a five-character string (for example, '42S02'). SQLSTATE values are taken from ANSI SQL and ODBC and are more standardized than the numeric error codes.
	Message string: This string provides a textual description of the error.
*/
func (conn *Connection) sendErrPacket(endPoint *PacketEndPoint, errorCode uint16, sqlState, errorMessage string) error {
	if conn.ses != nil {
		conn.ses.GetErrInfo().push(errorCode, errorMessage)
	}
	errPkt := ERRPacket{}
	err := errPkt.Open(conn.connCtx,
		WithCapability(conn.capability),
		WithErrorCode(errorCode),
		WithSqlState(sqlState),
		WithErrorMessage(errorMessage))
	if err != nil {
		return err
	}
	defer func() {
		_ = errPkt.Close(conn.connCtx)
	}()
	return endPoint.SendPacket(conn.connCtx, &errPkt, true)
}

func (conn *Connection) authen(endPoint *PacketEndPoint, authrsp []byte) (err error) {
	ses := conn.ses
	ses.timestampMap[TSAuthenticateStart] = time.Now()
	defer func() {
		ses.timestampMap[TSAuthenticateEnd] = time.Now()
		v2.AuthenticateDurationHistogram.Observe(float64(ses.timestampMap[TSAuthenticateEnd].Sub(ses.timestampMap[TSAuthenticateStart]).Milliseconds()))
	}()

	// logDebugf(mp.getDebugStringUnsafe(), "authenticate user")
	if err := conn.authenticateUser(authrsp); err != nil {
		logutil.Errorf("authenticate user failed.error:%v", err)
		errorCode, sqlState, msg := RewriteError(err, ses.GetUserName())
		err2 := conn.sendErrPacket(endPoint, errorCode, sqlState, msg)
		if err2 != nil {
			logutil.Errorf("send err packet failed.error:%v", err2)
			return err2
		}
		return err
	} else {
		// logDebugf(mp.getDebugStringUnsafe(), "handle handshake end")
		ok := OKPacket{}
		err = ok.Open(conn.connCtx, WithCapability(conn.capability))
		if err != nil {
			return err
		}
		defer func() {
			_ = ok.Close(conn.connCtx)
		}()
		err = endPoint.SendPacket(conn.connCtx, &ok, true)
		if err != nil {
			return err
		}
		// logDebugf(mp.getDebugStringUnsafe(), "handle handshake response ok")
	}

	return err
}

func (conn *Connection) handleCommand(endPoint *PacketEndPoint) (err error) {
	switch conn.req.payload.GetCmd() {
	case COM_QUIT:
		break
	case COM_QUERY:
		var query = string(conn.req.payload.GetData())
		conn.addSqlCount(1)
		conn.req.userInput = &UserInput{sql: query}
		err = conn.doComQuery(endPoint)
		if err != nil {
			return err
		}
	case COM_INIT_DB:
		var dbname = string(conn.req.payload.GetData())
		conn.addSqlCount(1)
		query := "use `" + dbname + "`"
		conn.req.userInput = &UserInput{sql: query}
		err = conn.doComQuery(endPoint)
		if err != nil {
			return err
		}
	case COM_FIELD_LIST:
		var payload = string(conn.req.payload.GetData())
		conn.addSqlCount(1)
		query := makeCmdFieldListSql(payload)
		conn.req.userInput = &UserInput{sql: query}
		err = conn.doComQuery(endPoint)
		if err != nil {
			return err
		}
	case COM_PING:
		err = conn.doComPing(endPoint)
		if err != nil {
			return err
		}
	case COM_STMT_PREPARE:
		sql := string(conn.req.payload.GetData())
		conn.addSqlCount(1)

		// rewrite to "Prepare stmt_name from 'xxx'"
		newLastStmtID := conn.ses.GenNewStmtId()
		newStmtName := getPrepareStmtName(newLastStmtID)
		sql = fmt.Sprintf("prepare %s from %s", newStmtName, sql)
		// logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))
		conn.req.userInput = &UserInput{sql: sql}
		err = conn.doComQuery(endPoint)
		if err != nil {
			return err
		}
	case COM_STMT_EXECUTE:
		data := conn.req.payload.GetData()
		var prepareStmt *PrepareStmt
		var sql string
		reqCtx, _ := conn.req.ctx.Ctx()
		sql, prepareStmt, err = conn.parseStmtExecute(reqCtx, data)
		if err != nil {
			// return NewGeneralErrorResponse(COM_STMT_EXECUTE, conn.ses.GetServerStatus(), err)
			return err
		}
		conn.req.userInput = &UserInput{sql: sql}
		err = conn.doComQuery(endPoint)
		if err != nil {
			// resp = NewGeneralErrorResponse(COM_STMT_EXECUTE, mce.ses.GetServerStatus(), err)
			return err
		}
		if prepareStmt.params != nil {
			prepareStmt.params.GetNulls().Reset()
			for k := range prepareStmt.getFromSendLongData {
				delete(prepareStmt.getFromSendLongData, k)
			}
		}
	case COM_STMT_SEND_LONG_DATA:
		data := conn.req.payload.GetData()
		reqCtx, _ := conn.req.ctx.Ctx()
		err = conn.parseStmtSendLongData(reqCtx, data)
		if err != nil {
			// resp = NewGeneralErrorResponse(COM_STMT_SEND_LONG_DATA, mce.ses.GetServerStatus(), err)
			// return resp, nil
			return err
		}
	case COM_STMT_CLOSE:
		data := conn.req.payload.GetData()

		// rewrite to "deallocate Prepare stmt_name"
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		stmtName := getPrepareStmtName(stmtID)
		sql := fmt.Sprintf("deallocate prepare %s", stmtName)
		// logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))
		conn.req.userInput = &UserInput{sql: sql}
		err = conn.doComQuery(endPoint)
		if err != nil {
			// resp = NewGeneralErrorResponse(COM_STMT_CLOSE, mce.ses.GetServerStatus(), err)
			return err
		}

	case COM_STMT_RESET:
		data := conn.req.payload.GetData()

		//Payload of COM_STMT_RESET
		stmtID := binary.LittleEndian.Uint32(data[0:4])
		stmtName := getPrepareStmtName(stmtID)
		sql := fmt.Sprintf("reset prepare %s", stmtName)
		//logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))
		conn.req.userInput = &UserInput{sql: sql}
		err = conn.doComQuery(endPoint)
		if err != nil {
			// resp = NewGeneralErrorResponse(COM_STMT_RESET, mce.ses.GetServerStatus(), err)
			return err
		}
	default:
		//error
	}
	return err
}

func (conn *Connection) parseStmtExecute(requestCtx context.Context, data []byte) (string, *PrepareStmt, error) {
	// see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html
	pos := 0
	if len(data) < 4 {
		return "", nil, moerr.NewInvalidInput(requestCtx, "sql command contains malformed packet")
	}
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmtName := fmt.Sprintf("%s_%d", prefixPrepareStmtName, stmtID)
	ses := conn.ses
	preStmt, err := ses.GetPrepareStmt(stmtName)
	if err != nil {
		return "", nil, err
	}

	sql := fmt.Sprintf("execute %s", stmtName)
	logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))
	//TODO:fix here
	err = ses.GetMysqlProtocol().ParseExecuteData(requestCtx, ses.GetTxnCompileCtx().GetProcess(), preStmt, data, pos)
	if err != nil {
		return "", nil, err
	}
	return sql, preStmt, nil
}

func (conn *Connection) parseStmtSendLongData(requestCtx context.Context, data []byte) error {
	// see https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_send_long_data.html
	pos := 0
	if len(data) < 4 {
		return moerr.NewInvalidInput(requestCtx, "sql command contains malformed packet")
	}
	stmtID := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	stmtName := fmt.Sprintf("%s_%d", prefixPrepareStmtName, stmtID)
	ses := conn.ses
	preStmt, err := ses.GetPrepareStmt(stmtName)
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("send long data for stmt %s", stmtName)
	logDebug(ses, ses.GetDebugString(), "query trace", logutil.ConnectionIdField(ses.GetConnectionID()), logutil.QueryField(sql))
	//TODO:fix here
	err = ses.GetMysqlProtocol().ParseSendLongData(requestCtx, ses.GetTxnCompileCtx().GetProcess(), preStmt, data, pos)
	if err != nil {
		return err
	}
	return nil
}

func (conn *Connection) doComQuery(endPoint *PacketEndPoint) (err error) {
	exec := &GeneralExecutor{}
	reqCtx, _ := conn.req.ctx.Ctx()
	err = exec.Open(reqCtx, WithConnection(conn), WithEndPoint(endPoint))
	if err != nil {
		return err
	}
	defer func() {
		_ = exec.Close(reqCtx)
	}()

	return exec.Exec(reqCtx, conn.req.userInput)
}

func (conn *Connection) doComPing(endPoint *PacketEndPoint) (err error) {
	ok := &OKPacket{}
	reqCtx, _ := conn.req.ctx.Ctx()
	err = ok.Open(reqCtx, WithCapability(conn.capability))
	if err != nil {
		return err
	}
	defer func() {
		_ = ok.Close(reqCtx)
	}()
	err = endPoint.SendPacket(reqCtx, ok, true)
	if err != nil {
		return err
	}
	return err
}

// get new process id
func (conn *Connection) getNextProcessId() string {
	/*
		temporary method:
		routineId + sqlCount
	*/
	return fmt.Sprintf("%d%d", conn.connId, conn.getSqlCount())
}

func (conn *Connection) getSqlCount() uint64 {
	return conn.ses.sqlCount
}

func (conn *Connection) addSqlCount(a uint64) {
	conn.ses.sqlCount += a
}

func (conn *Connection) needPrintSessionInfo() bool {
	if conn.printInfoOnce {
		conn.printInfoOnce = false
		return true
	}
	return false
}

func (conn *Connection) setResricted(val bool) {
	conn.restricted.Store(val)
}

func (conn *Connection) isRestricted() bool {
	return conn.restricted.Load()
}

func (conn *Connection) increaseCount(counter func()) {
	if conn.connectionBeCounted.CompareAndSwap(false, true) {
		if counter != nil {
			counter()
		}
	}
}

func (conn *Connection) decreaseCount(counter func()) {
	if conn.connectionBeCounted.CompareAndSwap(true, false) {
		if counter != nil {
			counter()
		}
	}
}

func (conn *Connection) setCancelled(b bool) bool {
	return conn.cancelled.Swap(b)
}

func (conn *Connection) isCancelled() bool {
	return conn.cancelled.Load()
}

func (conn *Connection) setInProcessRequest(b bool) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	conn.mu.inProcessRequest = b
}

// execCallbackInProcessRequestOnly denotes if inProcessRequest is true,
// then the callback will be called.
// It has used the mutex.
func (conn *Connection) execCallbackBasedOnRequest(want bool, callback func()) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.mu.inProcessRequest == want {
		if callback != nil {
			callback()
		}
	}
}

func (conn *Connection) setConnCancel(cf context.CancelFunc) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	conn.mu.connCancel = cf

}

func (conn *Connection) cancelConnCtx() {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.mu.connCancel != nil {
		conn.mu.connCancel()
	}
}

func (conn *Connection) updateGoroutineId() {
	if conn.goroutineID == 0 {
		conn.goroutineID = GetRoutineId()
	}
}

func (conn *Connection) cancelRequestCtx() {
	_, reqCancel := conn.req.ctx.Ctx()
	if reqCancel != nil {
		reqCancel()
	}
}

func (conn *Connection) reportSystemStatus() (r bool) {
	now := time.Now()
	defer func() {
		if r {
			feReportSystemStatusTime.Store(&now)
		}
	}()
	last := feReportSystemStatusTime.Load()
	if last == nil {
		r = true
		return
	}
	if now.Sub(*last) > time.Minute {
		r = true
		return
	}
	return
}

// killQuery if there is a running query, just cancel it.
func (conn *Connection) killQuery(killMyself bool, statementId string) {
	if !killMyself {
		//1,cancel request ctx
		conn.cancelRequestCtx()
		//2.cancel txn ctx
		ses := conn.ses
		if ses != nil {
			ses.SetQueryInExecute(false)
			logutil.Infof("set query status on the connection %d", conn.connId)
			txnHandler := ses.GetTxnHandler()
			if txnHandler != nil {
				txnHandler.cancelTxnCtx()
			}
		}
	}
}

// killConnection close the network connection
// myself: true -- the client kill itself.
// myself: false -- the client kill another connection.
func (conn *Connection) killConnection(killMyself bool) {
	//Case 1: kill the connection itself. Do not close the network connection here.
	//label the connection with the cancelled tag
	//if it was cancelled, do nothing
	if conn.setCancelled(true) {
		return
	}

	//Case 2: kill another connection. Close the network here.
	//    if the connection is processing the request, the response may be dropped.
	//    if the connection is not processing the request, it has no effect.
	if !killMyself {
		//If it is in processing the request, cancel the root context of the connection.
		//At the same time, it cancels all the contexts
		//(includes the request context) derived from the root context.
		//After the context is cancelled. In handleRequest, the network
		//will be closed finally.
		conn.cancelConnCtx()

		//If it is in processing the request, it responds to the client normally
		//before closing the network to avoid the mysql client to be hung.
		closeConn := func() {
			//If it is not in processing the request, just close the network
			// proto := rt.protocol
			// if proto != nil {
			// 	proto.Quit()
			// }
		}

		conn.execCallbackBasedOnRequest(false, closeConn)
	}
}

// cleanup When the io is closed, the cleanup will be called in callback Closed().
// cleanup releases the resources only once.
// both the client and the server can close the connection.
func (conn *Connection) cleanup() {
	//step 1: cancel the query if there is a running query.
	//step 2: close the connection.
	conn.closeOnce.Do(func() {
		ses := conn.ses
		//step A: rollback the txn
		if ses != nil {
			err := ses.txn.RollbackTxn()
			if err != nil {
				logError(ses, ses.GetDebugString(),
					"Failed to rollback txn",
					zap.Error(err))
			}
		}

		//step B: cancel the query
		conn.killQuery(false, "")

		//step C: cancel the root context of the connection.
		//At the same time, it cancels all the contexts
		//(includes the request context) derived from the root context.
		conn.cancelConnCtx()

		//step D: clean protocol
		// rt.protocol.Quit()

		//step E: release the resources related to the session
		if ses != nil {
			ses.Close()
		}
	})
}
