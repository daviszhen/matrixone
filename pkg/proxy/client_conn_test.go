// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"context"
	"encoding/binary"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fagongzi/goetty/v2"
	"github.com/fagongzi/goetty/v2/buf"
	"github.com/lni/goutils/leaktest"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/stretchr/testify/require"
)

type mockNetConn struct {
	localIP    string
	localPort  int
	remoteIP   string
	remotePort int
	c          net.Conn
}

func newMockNetConn(
	localIP string, localPort int, remoteIP string, remotePort int, c net.Conn,
) *mockNetConn {
	return &mockNetConn{
		localIP:    localIP,
		localPort:  localPort,
		remoteIP:   remoteIP,
		remotePort: remotePort,
		c:          c,
	}
}

func (c *mockNetConn) SetRemote(addr string) {
	c.remoteIP = addr
}

func (c *mockNetConn) Read(b []byte) (n int, err error) {
	return c.c.Read(b)
}

func (c *mockNetConn) Write(b []byte) (n int, err error) {
	return c.c.Write(b)
}

func (c *mockNetConn) Close() error {
	return nil
}

func (c *mockNetConn) LocalAddr() net.Addr {
	return &net.TCPAddr{
		IP:   []byte(c.localIP),
		Port: c.localPort,
	}
}

func (c *mockNetConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{
		IP:   []byte(c.remoteIP),
		Port: c.remotePort,
	}
}

func (c *mockNetConn) SetDeadline(t time.Time) error {
	return nil
}

func (c *mockNetConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *mockNetConn) SetWriteDeadline(t time.Time) error {
	return nil
}

type mockClientConn struct {
	conn       net.Conn
	tenant     Tenant
	clientInfo clientInfo // need to set it explicitly
	router     Router
	tun        *tunnel
	redoStmts  []internalStmt
}

var _ ClientConn = (*mockClientConn)(nil)

func newMockClientConn(
	conn net.Conn, tenant Tenant, ci clientInfo, router Router, tun *tunnel,
) ClientConn {
	c := &mockClientConn{
		conn:       conn,
		tenant:     tenant,
		clientInfo: ci,
		router:     router,
		tun:        tun,
	}
	return c
}

func (c *mockClientConn) ConnID() uint32                     { return 0 }
func (c *mockClientConn) GetSalt() []byte                    { return nil }
func (c *mockClientConn) GetHandshakePack() *frontend.Packet { return nil }
func (c *mockClientConn) RawConn() net.Conn                  { return c.conn }
func (c *mockClientConn) GetTenant() Tenant                  { return c.tenant }
func (c *mockClientConn) SendErrToClient(err error)          {}
func (c *mockClientConn) BuildConnWithServer(_ string) (ServerConn, error) {
	var err error
	li := &c.clientInfo.labelInfo
	c.clientInfo.labelInfo = newLabelInfo(c.clientInfo.Tenant, li.Labels)
	c.clientInfo.hash, err = c.clientInfo.getHash()
	if err != nil {
		return nil, err
	}
	cn, err := c.router.Route(context.TODO(), "", c.clientInfo, nil)
	if err != nil {
		return nil, err
	}
	cn.salt = testSlat
	sc, _, err := c.router.Connect(cn, testPacket, c.tun)
	if err != nil {
		return nil, err
	}
	// Set the use defined variables, including session variables and user variables.
	for _, stmt := range c.redoStmts {
		if _, err := sc.ExecStmt(stmt, nil); err != nil {
			return nil, err
		}
	}
	return sc, nil
}

func (c *mockClientConn) HandleEvent(ctx context.Context, e IEvent, resp chan<- []byte) error {
	defer e.notify()
	switch ev := e.(type) {
	case *killEvent:
		cn, err := c.router.SelectByConnID(ev.connID)
		if err != nil {
			sendResp([]byte(err.Error()), resp)
			return err
		}
		sendResp([]byte(cn.addr), resp)
		return nil
	case *setVarEvent:
		c.redoStmts = append(c.redoStmts, internalStmt{cmdType: cmdQuery, s: ev.stmt})
		sendResp([]byte("ok"), resp)
		return nil
	case *quitEvent:
		sendResp([]byte("ok"), resp)
		return nil
	default:
		sendResp([]byte("type not supported"), resp)
		return moerr.NewInternalErrorNoCtx("type not supported")
	}
}
func (c *mockClientConn) Close() error { return nil }

func testStartClient(t *testing.T, tp *testProxyHandler, ci clientInfo, cn *CNServer) func() {
	if cn.salt == nil || len(cn.salt) != 20 {
		cn.salt = testSlat
	}
	clientProxy, client := net.Pipe()
	go func(ctx context.Context) {
		b := make([]byte, 10)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			_, _ = client.Read(b)
		}
	}(tp.ctx)
	tu := newTunnel(tp.ctx, tp.logger, tp.counterSet)
	sc, _, err := tp.ru.Connect(cn, testPacket, tu)
	require.NoError(t, err)
	cc := newMockClientConn(clientProxy, "t1", ci, tp.ru, tu)
	err = tu.run(cc, sc)
	require.NoError(t, err)
	select {
	case err := <-tu.errC:
		t.Fatalf("tunnel error: %v", err)
	default:
	}
	return func() {
		_ = tu.Close()
		_ = sc.Close()
	}
}

func copyCNServer(dst, src *CNServer) {
	dst.connID = src.connID
	dst.salt = make([]byte, len(src.salt))
	copy(dst.salt, src.salt[:])
	dst.reqLabel = src.reqLabel
	dst.cnLabel = make(map[string]metadata.LabelList)
	for k, v := range src.cnLabel {
		dst.cnLabel[k] = v
	}
	dst.hash = src.hash
	dst.uuid = src.uuid
	dst.addr = src.addr
	dst.internalConn = src.internalConn
	dst.clientAddr = src.clientAddr
}

func testStartNClients(t *testing.T, tp *testProxyHandler, ci clientInfo, cn *CNServer, n int) func() {
	var cleanFns []func()
	for i := 0; i < n; i++ {
		newCN := &CNServer{}
		copyCNServer(newCN, cn)
		c := testStartClient(t, tp, ci, newCN)
		cleanFns = append(cleanFns, c)
	}
	return func() {
		for _, f := range cleanFns {
			f()
		}
	}
}

func TestAccountParser(t *testing.T) {
	cases := []struct {
		str      string
		tenant   string
		username string
		hasErr   bool
	}{
		{
			str:      "t1:u1",
			tenant:   "t1",
			username: "u1",
			hasErr:   false,
		},
		{
			str:      "t1#u1",
			tenant:   "t1",
			username: "u1",
			hasErr:   false,
		},
		{
			str:      ":u1",
			tenant:   "",
			username: "",
			hasErr:   true,
		},
		{
			str:      "a:",
			tenant:   "",
			username: "",
			hasErr:   true,
		},
		{
			str:      "u1",
			tenant:   frontend.GetDefaultTenant(),
			username: "u1",
			hasErr:   false,
		},
		{
			str:      "t1:u1?a=1",
			tenant:   "t1",
			username: "u1",
			hasErr:   false,
		},
	}
	for _, item := range cases {
		a := clientInfo{}
		err := a.parse(item.str)
		if item.hasErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, string(a.labelInfo.Tenant), item.tenant)
		require.Equal(t, a.username, item.username)
	}
}

func newTestPu() *config.ParameterUnit {
	fp := config.FrontendParameters{}
	fp.SetDefaultValues()
	pu := config.NewParameterUnit(&fp, nil, nil, nil)
	return pu
}

func createNewClientConn(t *testing.T) (ClientConn, func()) {
	s := goetty.NewIOSession(goetty.WithSessionConn(1,
		newMockNetConn("127.0.0.1", 30001,
			"127.0.0.1", 30010, nil)),
		goetty.WithSessionCodec(WithProxyProtocolCodec(frontend.NewSqlCodec())))
	ctx, cancel := context.WithCancel(context.Background())
	frontend.SetSessionAlloc("", frontend.NewSessionAllocator(newTestPu()))
	clientBaseConnID = 90
	rt := runtime.DefaultRuntime()
	logger := rt.Logger()
	cs := newCounterSet()
	cc, err := newClientConn(
		ctx, &Config{}, logger, cs, s,
		nil, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, cc)
	return cc, func() {
		cancel()
		_ = cc.Close()
	}
}

func TestNewClientConn(t *testing.T) {
	cc, cleanup := createNewClientConn(t)
	defer cleanup()
	require.Equal(t, 91, int(cc.ConnID()))
	require.Equal(t, 20, len(cc.GetSalt()))
	require.NotNil(t, cc.RawConn())
}

func makeClientHandshakeResp() []byte {
	payload := make([]byte, 200)
	pos := 0
	copy(payload[pos:], []byte{141, 162, 10, 0}) // Capabilities Flags
	pos += 4
	copy(payload[pos:], []byte{0, 0, 0, 0}) // maximum packet size
	pos += 4
	payload[pos] = 45 // client charset
	pos += 1
	pos += 23 // filler
	username := "tenant1:user1"
	copy(payload[pos:], username) // login username
	pos += len(username)
	payload[pos] = 0 // the end of username
	pos += 1
	payload[pos] = 20 // length of auth response
	pos += 1
	pos += 20 // auth response
	dbname := "db1"
	copy(payload[pos:], dbname) // db name
	pos += len(dbname)
	payload[pos] = 0 // end of db name
	pos += 1
	plugin := "mysql_native_password"
	copy(payload[pos:], plugin)
	pos += 1 + len(plugin)
	data := make([]byte, pos+4)
	data[0] = uint8(pos)
	data[1] = uint8(pos >> 8)
	data[2] = uint8(pos >> 16)
	data[3] = 1
	copy(data[4:], payload)
	return data
}

func TestClientConn_ConnectToBackend(t *testing.T) {
	defer leaktest.AfterTest(t)()

	runtime.SetupServiceBasedRuntime("", runtime.DefaultRuntime())
	rt := runtime.DefaultRuntime()
	logger := rt.Logger()

	t.Run("cannot connect", func(t *testing.T) {
		nilC := (*clientConn)(nil)
		require.Equal(t, "", string(nilC.GetTenant()))
		require.Nil(t, nilC.RawConn())

		cc := &clientConn{
			log: logger,
		}
		cc.testHelper.connectToBackend = func() (ServerConn, error) {
			return nil, moerr.NewInternalErrorNoCtx("123 456")
		}

		sc, err := cc.BuildConnWithServer("aaa")
		require.ErrorContains(t, err, "123 456")
		require.Nil(t, sc)
	})

	t.Run("ok connect", func(t *testing.T) {
		local, remote := net.Pipe()
		require.NotNil(t, local)
		require.NotNil(t, remote)

		cc, cleanup := createNewClientConn(t)
		defer cleanup()
		c, ok := cc.(*clientConn)
		require.True(t, ok)
		require.NotNil(t, c)
		c.conn.UseConn(local)
		c.mysqlProto.UseConn(local)
		require.Equal(t, "", string(cc.GetTenant()))

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			b := make([]byte, 100)
			// client reads init handshake.
			n, err := remote.Read(b)
			require.NoError(t, err)
			require.NotEqual(t, 0, n)

			// client sends handshake resp.
			resp := makeClientHandshakeResp()
			n, err = remote.Write(resp)
			require.NoError(t, err)
			require.Equal(t, len(resp), n)
		}()

		_, err := cc.BuildConnWithServer("")
		require.Error(t, err) // just test client, no router set
		require.Equal(t, "tenant1", string(cc.GetTenant()))
		require.NotNil(t, cc.GetHandshakePack())
		wg.Wait()
	})
}

func TestClientConn_ReadPacket(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cc, cleanup := createNewClientConn(t)
	defer cleanup()
	c, ok := cc.(*clientConn)
	require.True(t, ok)
	require.NotNil(t, c)

	local, remote := net.Pipe()
	require.NotNil(t, local)
	require.NotNil(t, remote)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		addr := &ProxyAddr{
			SourceAddress: []byte{10, 10, 10, 10},
			SourcePort:    1000,
			TargetAddress: []byte{20, 20, 20, 20},
			TargetPort:    2000,
		}

		b := buf.NewByteBuf(1000)

		b.WriteString(ProxyProtocolV2Signature)
		err := b.WriteByte(0)
		require.NoError(t, err)
		err = b.WriteByte(0)
		require.NoError(t, err)
		b.WriteUint16(12)
		n, err := b.Write(addr.SourceAddress)
		require.Equal(t, 4, n)
		require.NoError(t, err)
		n, err = b.Write(addr.TargetAddress)
		require.Equal(t, 4, n)
		require.NoError(t, err)
		b.WriteUint16(addr.SourcePort)
		b.WriteUint16(addr.TargetPort)

		n, d := b.ReadAll()
		require.Equal(t, 28, n)
		err = binary.Write(remote, binary.BigEndian, d)
		require.NoError(t, err)

		// little endian
		err = b.WriteByte(9)
		require.NoError(t, err)
		err = b.WriteByte(0)
		require.NoError(t, err)
		err = b.WriteByte(0)
		require.NoError(t, err)
		err = b.WriteByte(0)
		require.NoError(t, err)
		err = b.WriteByte(3)
		require.NoError(t, err)
		b.WriteString("select 1")

		n, d = b.ReadAll()
		require.Equal(t, 13, n)
		err = binary.Write(remote, binary.LittleEndian, d)
		require.NoError(t, err)
	}()

	c.conn.UseConn(local)
	ret, err := c.readPacket()
	require.NoError(t, err)
	require.NotNil(t, ret)
	require.Equal(t, 9, int(ret.Length))
	require.Equal(t, 0, int(ret.SequenceID))
	require.Equal(t, 3, int(ret.Payload[0]))
	require.Equal(t, "select 1", string(ret.Payload[1:]))

	wg.Wait()
}

func TestClientConn_ConnID(t *testing.T) {
	parallel := 100
	clientBaseConnID = 1
	var wg sync.WaitGroup
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			nextClientConnID()
			defer wg.Done()
		}()
	}
	wg.Wait()
	require.Equal(t, 101, int(clientBaseConnID))
}

func TestClientConn_SendErrToClient(t *testing.T) {
	local, remote := net.Pipe()
	require.NotNil(t, local)
	require.NotNil(t, remote)

	cc, cleanup := createNewClientConn(t)
	defer cleanup()
	c, ok := cc.(*clientConn)
	require.True(t, ok)
	require.NotNil(t, c)
	c.conn.UseConn(local)
	c.mysqlProto.UseConn(local)
	require.Equal(t, "", string(cc.GetTenant()))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		b := make([]byte, 100)
		// client reads init handshake.
		n, err := remote.Read(b)
		require.NoError(t, err)
		require.NotEqual(t, 0, n)

		// client sends handshake resp.
		resp := makeClientHandshakeResp()
		n, err = remote.Write(resp)
		require.NoError(t, err)
		require.Equal(t, len(resp), n)

		n, err = remote.Read(b)
		require.NoError(t, err)
		require.Equal(t, 33, n)
		require.True(t, strings.Contains(string(b[4+1+2+1+5:n]), "internal error: msg1"))
	}()

	_, err := cc.BuildConnWithServer("")
	require.Error(t, err) // just test client, no router set
	require.Equal(t, "tenant1", string(cc.GetTenant()))
	require.NotNil(t, cc.GetHandshakePack())
	cc.SendErrToClient(moerr.NewInternalErrorNoCtx("msg1"))
	wg.Wait()
}

var _ Router = &testRouter{}

const (
	routerReturnErrSecondTime = 1
)

type testRouter struct {
	mod int
	cnt int
}

func (router *testRouter) Route(ctx context.Context, sid string, client clientInfo, filter func(string) bool) (*CNServer, error) {
	if router.mod == routerReturnErrSecondTime {
		if router.cnt >= 1 {
			return nil, moerr.NewInternalErrorNoCtx("route return error")
		}
		router.cnt++
	}
	return &CNServer{}, nil
}

func (router *testRouter) SelectByConnID(connID uint32) (*CNServer, error) {
	//TODO implement me
	panic("implement me")
}

func (router *testRouter) AllServers(sid string) ([]*CNServer, error) {
	//TODO implement me
	panic("implement me")
}

func (router *testRouter) Connect(c *CNServer, handshakeResp *frontend.Packet, t *tunnel) (ServerConn, []byte, error) {
	return newMockServerConn(nil), nil, nil
}

var _ client.QueryClient = &testQueryClient{}

type testQueryClient struct {
}

func (client *testQueryClient) ServiceID() string {
	//TODO implement me
	panic("implement me")
}

func (client *testQueryClient) SendMessage(ctx context.Context, address string, req *query.Request) (*query.Response, error) {
	return nil, moerr.NewInternalErrorNoCtx("return error")
}

func (client *testQueryClient) NewRequest(method query.CmdMethod) *query.Request {
	return &query.Request{}
}

func (client *testQueryClient) Release(response *query.Response) {
	//TODO implement me
	panic("implement me")
}

func (client *testQueryClient) Close() error {
	//TODO implement me
	panic("implement me")
}

var _ clusterservice.MOCluster = &testCluster{}

type testCluster struct {
}

func (cluster *testCluster) GetCNService(selector clusterservice.Selector, apply func(metadata.CNService) bool) {
}

func (cluster *testCluster) GetTNService(selector clusterservice.Selector, apply func(metadata.TNService) bool) {
	//TODO implement me
	panic("implement me")
}

func (cluster *testCluster) GetAllTNServices() []metadata.TNService {
	//TODO implement me
	panic("implement me")
}

func (cluster *testCluster) GetCNServiceWithoutWorkingState(selector clusterservice.Selector, apply func(metadata.CNService) bool) {
	//TODO implement me
	panic("implement me")
}

func (cluster *testCluster) ForceRefresh(sync bool) {
	//TODO implement me
	panic("implement me")
}

func (cluster *testCluster) Close() {
	//TODO implement me
	panic("implement me")
}

func (cluster *testCluster) DebugUpdateCNLabel(uuid string, kvs map[string][]string) error {
	//TODO implement me
	panic("implement me")
}

func (cluster *testCluster) DebugUpdateCNWorkState(uuid string, state int) error {
	//TODO implement me
	panic("implement me")
}

func (cluster *testCluster) RemoveCN(id string) {
	//TODO implement me
	panic("implement me")
}

func (cluster *testCluster) AddCN(service metadata.CNService) {
	//TODO implement me
	panic("implement me")
}

func (cluster *testCluster) UpdateCN(service metadata.CNService) {
	//TODO implement me
	panic("implement me")
}

func Test_connectToBackend(t *testing.T) {
	rt := runtime.DefaultRuntime()
	logger := rt.Logger()
	cConn := &clientConn{
		ctx:         context.Background(),
		router:      &testRouter{mod: routerReturnErrSecondTime},
		mysqlProto:  &frontend.MysqlProtocolImpl{},
		queryClient: &testQueryClient{},
		moCluster:   &testCluster{},
		log:         logger,
	}
	sConn, err := cConn.connectToBackend("127.0.0.1")
	require.Error(t, err)
	require.Nil(t, sConn)
}

func TestHandleSetVar(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var cc clientConn
	cc.migration.setVarStmtMap = make(map[string]struct{})
	e0 := &setVarEvent{
		baseEvent: baseEvent{waitC: make(chan struct{}, 5)},
		stmt:      "set autocommit=0",
	}
	require.NoError(t, cc.handleSetVar(e0))
	require.Equal(t, 1, len(cc.migration.setVarStmtMap))
	require.Equal(t, 1, len(cc.migration.setVarStmts))
	require.Equal(t, e0.stmt, cc.migration.setVarStmts[len(cc.migration.setVarStmts)-1])

	require.NoError(t, cc.handleSetVar(e0))
	require.Equal(t, 1, len(cc.migration.setVarStmtMap))
	require.Equal(t, 1, len(cc.migration.setVarStmts))
	require.Equal(t, e0.stmt, cc.migration.setVarStmts[len(cc.migration.setVarStmts)-1])

	e1 := &setVarEvent{
		baseEvent: baseEvent{waitC: make(chan struct{}, 5)},
		stmt:      "set autocommit=1",
	}
	require.NoError(t, cc.handleSetVar(e1))
	require.Equal(t, 2, len(cc.migration.setVarStmtMap))
	require.Equal(t, 2, len(cc.migration.setVarStmts))
	require.Equal(t, e1.stmt, cc.migration.setVarStmts[len(cc.migration.setVarStmts)-1])

	require.NoError(t, cc.handleSetVar(e0))
	require.Equal(t, 2, len(cc.migration.setVarStmtMap))
	require.Equal(t, 2, len(cc.migration.setVarStmts))
	require.Equal(t, e0.stmt, cc.migration.setVarStmts[len(cc.migration.setVarStmts)-1])

	require.NoError(t, cc.handleSetVar(e1))
	require.Equal(t, 2, len(cc.migration.setVarStmtMap))
	require.Equal(t, 2, len(cc.migration.setVarStmts))
	require.Equal(t, e1.stmt, cc.migration.setVarStmts[len(cc.migration.setVarStmts)-1])
}
