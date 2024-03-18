package main

import (
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	fmt.Println("test2/main")
	go func() {
		http.ListenAndServe(":6060", nil)
	}()

	var err error
	var testPool *mpool.MPool
	//parameter
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	_, err = toml.DecodeFile("/home/pengzhen/Documents/matrixone/pkg/frontend/test/system_vars_config.toml", pu.SV)
	if err != nil {
		panic(err)
	}
	pu.SV.SetDefaultValues()
	pu.SV.SaveQueryResult = "on"
	testPool, err = mpool.NewMPool("testPool", pu.SV.GuestMmuLimitation, mpool.NoFixed)
	if err != nil {
		panic(err)
	}
	proto := frontend.NewMysqlClientProtocol(0, nil, 1024, pu.SV)
	testutil.SetupAutoIncrService()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGTERM, syscall.SIGINT)
	tr := time.NewTimer(time.Second * 10)
	quit := false
	val := 0
	for {
		if val > 1000000 {
			_, _ = fmt.Fprintln(os.Stderr, "1M")
			break
		}
		select {
		case sig := <-sigchan:
			_, _ = fmt.Fprintln(os.Stderr, "signal "+sig.String())
			quit = true
			break
		case <-tr.C:
			_, _ = fmt.Fprintln(os.Stderr, "timer")
			quit = true
			break
		default:
		}
		if quit {
			break
		}
		//fmt.Println("new session")
		ses := newTestSession(proto, testPool, pu)
		ses.ClearAllMysqlResultSet()
		//ses.Close()
		//time.Sleep(time.Second * 3)
		if val%10000 == 0 {
			fmt.Println(val)
		}
		val++
	}
}

func newTestSession(proto frontend.Protocol, mp *mpool.MPool, pu *config.ParameterUnit) *frontend.Session {
	//new session
	ses := frontend.NewSession(proto, mp, pu, frontend.GSysVariables, true, nil, nil)
	return ses
}

func main2() {
	go func() {
		http.ListenAndServe(":6060", nil)
	}()
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-sigchan:
		_, _ = fmt.Fprintln(os.Stderr, "signal "+sig.String())
		break
	}
}
