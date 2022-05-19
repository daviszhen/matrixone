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
	goErrors "errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/moengine"
	"github.com/matrixorigin/matrixone/pkg/vm/mempool"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
)

var (
	errorTaeTxnBeginInBegan           = goErrors.New("begin txn in the began txn")
	errorTaeTxnHasNotBeenBegan        = goErrors.New("the txn has not been began")
	errorTaeTxnAutocommitInAutocommit = goErrors.New("start autocommit txn in the autocommit txn")
	errorTaeTxnBeginInAutocommit      = goErrors.New("begin txn in the autocommit txn")
	errorTaeTxnAutocommitInBegan      = goErrors.New("start autocommit txn in the txn has been began")
	errorIsNotAutocommitTxn           = goErrors.New("it is not autocommit txn")
	errorIsNotBeginCommitTxn          = goErrors.New("it is not the begin/commit txn ")
)

const (
	TxnInit = iota
	TxnBegan
	TxnAutocommit
	TxnEnd
	TxnErr
	TxnNil
)

// TxnState represents for Transaction Machine
type TxnState struct {
	state     int
	fromState int
	err       error
}

func InitTxnState() *TxnState {
	return &TxnState{
		state:     TxnInit,
		fromState: TxnNil,
		err:       nil,
	}
}

func (ts *TxnState) isState(s int) bool {
	return ts.state == s
}

func (ts *TxnState) switchToState(s int, err error) {
	ts.fromState = ts.state
	ts.state = s
	ts.err = err
}

func (ts *TxnState) getState() int {
	return ts.state
}

func (ts *TxnState) getFromState() int {
	return ts.fromState
}

func (ts *TxnState) getError() error {
	return ts.err
}

func (ts *TxnState) String() string {
	return fmt.Sprintf("state:%d fromState:%d err:%v", ts.state, ts.fromState, ts.err)
}

var _ moengine.Txn = &TaeTxnDumpImpl{}

//TaeTxnDumpImpl is just a placeholder and does nothing
type TaeTxnDumpImpl struct {
}

func InitTaeTxnImpl() *TaeTxnDumpImpl {
	return &TaeTxnDumpImpl{}
}

func (tti *TaeTxnDumpImpl) GetCtx() []byte {
	return nil
}

func (tti *TaeTxnDumpImpl) GetID() uint64 {
	return 0
}

func (tti *TaeTxnDumpImpl) Commit() error {
	return nil
}

func (tti *TaeTxnDumpImpl) Rollback() error {
	return nil
}

func (tti *TaeTxnDumpImpl) String() string {
	return "TaeTxnDumpImpl"
}

func (tti *TaeTxnDumpImpl) Repr() string {
	return "TaeTxnDumpImpl.Repr"
}

func (tti *TaeTxnDumpImpl) GetError() error {
	return nil
}

type TxnHandler struct {
	//tae txn
	//TODO: add aoe dump impl of Txn interface for unifying the logic of txn
	taeTxn   moengine.Txn
	txnState *TxnState
}

func InitTxnHandler() *TxnHandler {
	return &TxnHandler{
		taeTxn:   InitTaeTxnImpl(),
		txnState: InitTxnState(),
	}
}

type Session struct {
	//protocol layer
	protocol Protocol

	//epoch gc handler
	pdHook *PDCallbackImpl

	//cmd from the client
	Cmd int

	//for test
	Mrs *MysqlResultSet

	GuestMmu *guest.Mmu
	Mempool  *mempool.Mempool

	Pu *config.ParameterUnit

	ep *tree.ExportParam

	closeRef   *CloseExportData
	txnHandler *TxnHandler
}

func NewSession(proto Protocol, pdHook *PDCallbackImpl, gm *guest.Mmu, mp *mempool.Mempool, PU *config.ParameterUnit) *Session {
	return &Session{
		protocol: proto,
		pdHook:   pdHook,
		GuestMmu: gm,
		Mempool:  mp,
		Pu:       PU,
		ep: &tree.ExportParam{
			Outfile: false,
			Fields:  &tree.Fields{},
			Lines:   &tree.Lines{},
		},
		txnHandler: InitTxnHandler(),
	}
}

func (ses *Session) GetEpochgc() *PDCallbackImpl {
	return ses.pdHook
}

func (ses *Session) GetTxnHandler() *TxnHandler {
	return ses.txnHandler
}

func (th *TxnHandler) getTxnState() int {
	return th.txnState.getState()
}

func (th *TxnHandler) isTxnState(s int) bool {
	return th.txnState.isState(s)
}

func (th *TxnHandler) switchToTxnState(s int, err error) {
	th.txnState.switchToState(s, err)
}

func (th *TxnHandler) getFromTxnState() int {
	return th.txnState.getFromState()
}

func (th *TxnHandler) getTxnStateError() error {
	return th.txnState.getError()
}

func (th *TxnHandler) getTxnStateString() string {
	return th.txnState.String()
}

// IsInTaeTxn checks the session executes a txn
func (th *TxnHandler) IsInTaeTxn() bool {
	st := th.getTxnState()
	if st == TxnAutocommit || st == TxnBegan {
		return true
	}
	return false
}

func (th *TxnHandler) IsTaeEngine() bool {
	_, ok := config.StorageEngine.(moengine.TxnEngine)
	return ok
}

func (th *TxnHandler) Begin() error {
	logutil.Infof("begin begin")
	var err error
	if taeEng, ok := config.StorageEngine.(moengine.TxnEngine); ok {
		switch th.txnState.getState() {
		case TxnInit, TxnEnd, TxnErr:
			//begin a transaction
			th.taeTxn, err = taeEng.StartTxn(nil)
		case TxnBegan:
			err = errorTaeTxnBeginInBegan
		case TxnAutocommit:
			err = errorTaeTxnBeginInAutocommit
		}
	} else {
		th.taeTxn = InitTaeTxnImpl()
	}

	if err == nil {
		th.txnState.switchToState(TxnBegan, err)
	} else {
		th.txnState.switchToState(TxnErr, err)
	}
	return err
}

func (th *TxnHandler) BeginAutocommit() error {
	logutil.Infof("begin autocommit")
	var err error
	if taeEng, ok := config.StorageEngine.(moengine.TxnEngine); ok {
		switch th.txnState.getState() {
		case TxnInit, TxnEnd, TxnErr:
			//begin a transaction
			th.taeTxn, err = taeEng.StartTxn(nil)
		case TxnAutocommit:
			err = errorTaeTxnAutocommitInAutocommit
		case TxnBegan:
			err = errorTaeTxnAutocommitInBegan
		}
	} else {
		th.taeTxn = InitTaeTxnImpl()
	}

	if err == nil {
		th.txnState.switchToState(TxnAutocommit, err)
	} else {
		th.txnState.switchToState(TxnErr, err)
	}
	return err
}

// BeginAutocommitIfNeeded starts a new txn or uses an existed txn
// true denotes a new txn
func (th *TxnHandler) BeginAutocommitIfNeeded() (bool, error) {
	logutil.Infof("begin autocommit if needed")
	var err error
	if th.IsInTaeTxn() {
		return false, nil
	}
	if taeEng, ok := config.StorageEngine.(moengine.TxnEngine); ok {
		switch th.txnState.getState() {
		case TxnInit, TxnEnd, TxnErr:
			//begin a transaction
			th.taeTxn, err = taeEng.StartTxn(nil)
		case TxnAutocommit:
			err = errorTaeTxnAutocommitInAutocommit
		case TxnBegan:
			err = errorTaeTxnAutocommitInBegan
		}
	} else {
		th.taeTxn = InitTaeTxnImpl()
	}

	if err == nil {
		th.txnState.switchToState(TxnAutocommit, err)
	} else {
		th.txnState.switchToState(TxnErr, err)
	}
	return true, err
}

func (th *TxnHandler) GetTxn() moengine.Txn {
	return th.taeTxn
}

// CommitAfterBegin commits the tae txn started by the BEGIN statement
func (th *TxnHandler) CommitAfterBegin() error {
	logutil.Infof("commit began")
	var err error
	switch th.getTxnState() {
	case TxnBegan:
		err = th.taeTxn.Commit()
	case TxnAutocommit:
		err = errorIsNotAutocommitTxn
	case TxnInit, TxnEnd, TxnErr:
		err = errorTaeTxnHasNotBeenBegan
	}

	if err == nil {
		th.txnState.switchToState(TxnEnd, err)
	} else {
		th.txnState.switchToState(TxnErr, err)
	}
	return err
}

// CommitAfterAutocommit commits the tae txn started by autocommit
func (th *TxnHandler) CommitAfterAutocommit() error {
	logutil.Infof("commit autocommit")
	var err error
	switch th.getTxnState() {
	case TxnAutocommit:
		err = th.taeTxn.Commit()
	case TxnBegan:
		err = errorIsNotBeginCommitTxn
	case TxnInit, TxnEnd, TxnErr:
		err = errorTaeTxnHasNotBeenBegan
	}

	if err == nil {
		th.txnState.switchToState(TxnEnd, err)
	} else {
		th.txnState.switchToState(TxnErr, err)
	}
	return err
}

// CommitAfterAutocommitOnly commits the tae txn started by autocommit
// Do not check TxnBegan
func (th *TxnHandler) CommitAfterAutocommitOnly() error {
	logutil.Infof("commit autocommit only")
	var err error
	switch th.getTxnState() {
	case TxnAutocommit:
		err = th.taeTxn.Commit()
	case TxnInit, TxnEnd, TxnErr:
		err = errorTaeTxnHasNotBeenBegan
	}

	if th.getTxnState() != TxnBegan {
		if err == nil {
			th.txnState.switchToState(TxnEnd, err)
		} else {
			th.txnState.switchToState(TxnErr, err)
		}
	}

	return err
}

func (th *TxnHandler) Rollback() error {
	logutil.Infof("rollback ")
	var err error
	switch th.getTxnState() {
	case TxnBegan, TxnAutocommit:
		err = th.taeTxn.Rollback()
	case TxnInit, TxnEnd, TxnErr:
		return errorTaeTxnHasNotBeenBegan
	}

	if err == nil {
		th.txnState.switchToState(TxnEnd, err)
	} else {
		th.txnState.switchToState(TxnErr, err)
	}
	return err
}

func (th *TxnHandler) RollbackAfterAutocommitOnly() error {
	logutil.Infof("rollback autocommit only")
	var err error
	switch th.getTxnState() {
	case TxnAutocommit:
		err = th.taeTxn.Rollback()
	case TxnInit, TxnEnd, TxnErr:
		return errorTaeTxnHasNotBeenBegan
	}

	if th.txnState.getState() != TxnBegan {
		if err == nil {
			th.txnState.switchToState(TxnEnd, err)
		} else {
			th.txnState.switchToState(TxnErr, err)
		}
	}

	return err
}

//ClearTxn commits the tae txn when the errors happen during the txn
func (th *TxnHandler) ClearTxn() error {
	logutil.Infof("clear tae txn")
	var err error
	switch th.txnState.getState() {
	case TxnInit, TxnEnd, TxnErr:
		th.taeTxn = InitTaeTxnImpl()
	case TxnBegan:
		logutil.Infof("can not commit a began txn without obvious COMMIT or ROLLBACK")
	case TxnAutocommit:
		err = th.CommitAfterAutocommit()
		th.taeTxn = InitTaeTxnImpl()
		th.txnState.switchToState(TxnInit, err)
	}
	return err
}
