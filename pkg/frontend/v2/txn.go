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
	"fmt"

	"github.com/google/uuid"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/memorystorage"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/metric"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

var (
	dumpUUID = uuid.UUID{}
)

func NewTxn() *Txn {
	txn := &Txn{}
	txn.setServerStatusUnsafe(SERVER_STATUS_AUTOCOMMIT)
	txn.setOptionBitsUnsafe(OPTION_AUTOCOMMIT)
	txn.storage = fePu.StorageEngine
	return txn
}

func (txn *Txn) AttachTempStorageToTxnCtx() {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.mu.txnCtx = context.WithValue(txn.createTxnCtxUnsafe(), defines.TemporaryTN{}, txn.ses.GetTempTableStorage())
}

func (txn *Txn) SetTempEngine(te engine.Engine) {
	ee := txn.storage.(*engine.EntireEngine)
	ee.TempEngine = te
}

// isValidTxnOperatorUnsafe checks the txn operator is valid
func (txn *Txn) isValidTxnOperatorUnsafe() bool {
	return txn.mu.txnOp != nil && txn.mu.txnCtx != nil
}

// IsValidTxnOperator checks the txn operator is valid
func (txn *Txn) IsValidTxnOperator() bool {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.isValidTxnOperatorUnsafe()
}

func (txn *Txn) SetTxnOperatorInvalid() {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.setTxnOperatorInvalidUnsafe()
}

func (txn *Txn) setTxnOperatorInvalidUnsafe() {
	txn.mu.txnOp = nil
	if txn.mu.txnCancel != nil {
		txn.mu.txnCancel()
		txn.mu.txnCancel = nil
	}
	txn.mu.txnCtx = nil
	txn.clearOptionBitsUnsafe(OPTION_BEGIN)
	txn.clearServerStatusUnsafe(SERVER_STATUS_IN_TRANS)
}

func (txn *Txn) GetTxnOperator() (context.Context, TxnOperator) {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.getTxnOperatorUnsafe()
}

func (txn *Txn) getTxnOperatorUnsafe() (context.Context, TxnOperator) {
	return txn.createTxnCtxUnsafe(), txn.mu.txnOp
}

// createTxnCtx creates a new txn context. unsafe
func (txn *Txn) createTxnCtxUnsafe() context.Context {
	if txn.mu.txnCtx == nil {
		txn.mu.txnCtx, txn.mu.txnCancel = context.WithTimeout(txn.ses.conn.connCtx, fePu.SV.SessionTimeout.Duration)
	}

	reqCtx, _ := txn.ses.conn.req.ctx.Ctx()
	retTxnCtx := txn.mu.txnCtx

	if v := reqCtx.Value(defines.TenantIDKey{}); v != nil {
		retTxnCtx = context.WithValue(retTxnCtx, defines.TenantIDKey{}, v)
	}
	if v := reqCtx.Value(defines.UserIDKey{}); v != nil {
		retTxnCtx = context.WithValue(retTxnCtx, defines.UserIDKey{}, v)
	}
	if v := reqCtx.Value(defines.RoleIDKey{}); v != nil {
		retTxnCtx = context.WithValue(retTxnCtx, defines.RoleIDKey{}, v)
	}
	if v := reqCtx.Value(defines.NodeIDKey{}); v != nil {
		retTxnCtx = context.WithValue(retTxnCtx, defines.NodeIDKey{}, v)
	}
	retTxnCtx = trace.ContextWithSpan(retTxnCtx, trace.SpanFromContext(reqCtx))
	if txn.ses != nil && txn.ses.tenant != nil && txn.ses.tenant.User == db_holder.MOLoggerUser {
		retTxnCtx = context.WithValue(retTxnCtx, defines.IsMoLogger{}, true)
	}

	if storage, ok := reqCtx.Value(defines.TemporaryTN{}).(*memorystorage.Storage); ok {
		retTxnCtx = context.WithValue(retTxnCtx, defines.TemporaryTN{}, storage)
	} else if txn.ses.IfInitedTempEngine() {
		retTxnCtx = context.WithValue(retTxnCtx, defines.TemporaryTN{}, txn.ses.GetTempTableStorage())
	}
	return retTxnCtx
}

// newTxnOperator creates a new txn operator using TxnClient
func (txn *Txn) newTxnOperatorUnsafe() (context.Context, TxnOperator, error) {
	var err error
	if feTxnClient == nil {
		panic("must set txn client")
	}

	reqCtx, _ := txn.ses.conn.req.ctx.Ctx()

	if txn.isValidTxnOperatorUnsafe() {
		return nil, nil, moerr.NewInternalError(reqCtx, "NewTxnOperator: there is a valid txn already")
	}

	var opts []client.TxnOption
	rt := moruntime.ProcessLevelRuntime()
	if rt != nil {
		if v, ok := rt.GetGlobalVariables(moruntime.TxnOptions); ok {
			opts = v.([]client.TxnOption)
		}
	}

	txnCtx := txn.createTxnCtxUnsafe()
	if txnCtx == nil {
		panic("context should not be nil")
	}
	opts = append(opts,
		client.WithTxnCreateBy(fmt.Sprintf("frontend-session-%p", txn.ses)))

	if txn.ses != nil && txn.ses.fromRealUser {
		opts = append(opts,
			client.WithUserTxn())
	}

	if txn.ses != nil {
		varVal, err := txn.ses.GetSessionVar("transaction_operator_open_log")
		if err != nil {
			return nil, nil, err
		}
		if gsv, ok := GSysVariables.GetDefinitionOfSysVar("transaction_operator_open_log"); ok {
			if svbt, ok2 := gsv.GetType().(SystemVariableBoolType); ok2 {
				if svbt.IsTrue(varVal) {
					opts = append(opts, client.WithTxnOpenLog())
				}
			}
		}

	}

	txn.mu.txnOp, err = feTxnClient.New(
		txnCtx,
		txn.ses.getLastCommitTS(),
		opts...)
	if err != nil {
		return nil, nil, err
	}
	if txn.mu.txnOp == nil {
		return nil, nil, moerr.NewInternalError(txn.ses.GetRequestContext(), "NewTxnOperator: txnClient new a null txn")
	}
	return txnCtx, txn.mu.txnOp, err
}

/*
NewTxn creates the transaction implicitly and idempotent

When it is in multi-statement transaction mode:

	Set SERVER_STATUS_IN_TRANS bit;
	Starts a new transaction if there is none. returns erros if there is one.

When it is not in single statement transaction mode:

	Starts a new transaction if there is none. returns erros if there is one.
*/
func (txn *Txn) NewTxn(opts ...TxnOpt) (context.Context, TxnOperator, error) {
	var err error
	var txnCtx context.Context
	var txnOp TxnOperator

	reqCtx, _ := txn.ses.conn.req.ctx.Ctx()

	newopts := &TxnOptions{}
	for _, opt := range opts {
		opt(newopts)
	}

	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.isValidTxnOperatorUnsafe() {
		return nil, nil, moerr.NewInternalError(reqCtx, "there is a valid txn operator")
	}

	txn.setTxnOperatorInvalidUnsafe()
	defer func() {
		if err != nil {
			tenant := txn.ses.GetTenantName()
			incTransactionErrorsCounter(tenant, metric.SQLTypeBegin)
		}
	}()
	txnCtx, txnOp, err = txn.newTxnOperatorUnsafe()
	if err != nil {
		return nil, nil, err
	}

	err = txn.storage.New(txnCtx, txnOp)
	if err != nil {
		//fail
		txn.ses.SetTxnId(dumpUUID[:])
	} else {
		//success
		txn.setServerStatusUnsafe(SERVER_STATUS_IN_TRANS)
		if newopts.begin {
			txn.setOptionBitsUnsafe(OPTION_BEGIN)
		}
		txn.ses.SetTxnId(txnOp.Txn().ID)
	}
	return txnCtx, txnOp, err
}

func (txn *Txn) commitTxnUnsafe(reqCtx context.Context) error {
	if !txn.isValidTxnOperatorUnsafe() {
		return nil
	}
	val, e := txn.ses.GetSessionVar("mo_pk_check_by_dn")
	if e != nil {
		return e
	}
	defer txn.setTxnOperatorInvalidUnsafe()
	ses := txn.ses
	sessionInfo := ses.GetDebugString()
	txnCtx, txnOp := txn.getTxnOperatorUnsafe()
	if txnOp == nil {
		logError(ses, sessionInfo, "CommitTxn: txn operator is null")
	}
	if txnCtx == nil {
		panic("context should not be nil")
	}
	if ses.tempTablestorage != nil {
		txnCtx = context.WithValue(txnCtx, defines.TemporaryTN{}, ses.tempTablestorage)
	}
	storage := txn.storage
	ctx2, cancel := context.WithTimeout(
		txnCtx,
		storage.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	if val != nil {
		ctx2 = context.WithValue(ctx2, defines.PkCheckByTN{}, val.(int8))
	}
	var err error
	defer func() {
		// metric count
		tenant := ses.GetTenantName()
		incTransactionCounter(tenant)
		if err != nil {
			incTransactionErrorsCounter(tenant, metric.SQLTypeCommit)
		}
	}()

	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		txnId := txnOp.Txn().DebugString()
		logDebugf(sessionInfo, "CommitTxn txnId:%s", txnId)
		defer func() {
			logDebugf(sessionInfo, "CommitTxn exit txnId:%s", txnId)
		}()
	}
	if txnOp != nil {
		txn.ses.SetTxnId(txnOp.Txn().ID)
		err = txnOp.Commit(ctx2)
		if err != nil { /*
				fix issue 6024.
				When we get a w-w conflict during commit the txn,
				we convert the error into a readable error.
			*/
			if moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict) {
				return moerr.NewInternalError(reqCtx, writeWriteConflictsErrorInfo())
			}
			txnId := txnOp.Txn().DebugString()
			logError(ses, sessionInfo,
				"CommitTxn: txn operator commit failed",
				zap.String("txnId", txnId),
				zap.Error(err))
		}
		ses.updateLastCommitTS(txnOp.Txn().CommitTS)
	}
	txn.ses.SetTxnId(dumpUUID[:])
	return err
}

// CommitTxn commit anyway
func (txn *Txn) CommitTxn() error {
	reqCtx, _ := txn.ses.conn.req.ctx.Ctx()
	_, span := trace.Start(reqCtx, "Txn.CommitTxn",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End(trace.WithStatementExtra(txn.ses.GetTxnId(), txn.ses.GetStmtId(), txn.ses.GetSqlOfStmt()))

	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.commitTxnUnsafe(reqCtx)
}

// CommitTxn commit conditionally
func (txn *Txn) CommitTxnCond(cond func() bool) error {
	reqCtx, _ := txn.ses.conn.req.ctx.Ctx()
	_, span := trace.Start(reqCtx, "Txn.CommitTxn",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End(trace.WithStatementExtra(txn.ses.GetTxnId(), txn.ses.GetStmtId(), txn.ses.GetSqlOfStmt()))

	txn.mu.Lock()
	defer txn.mu.Unlock()

	if !cond() {
		return nil
	}

	return txn.commitTxnUnsafe(reqCtx)
}

func (txn *Txn) rollbackTxnUnsafe() error {
	if !txn.isValidTxnOperatorUnsafe() {
		return nil
	}
	defer txn.setTxnOperatorInvalidUnsafe()
	ses := txn.ses
	sessionInfo := ses.GetDebugString()
	txnCtx, txnOp := txn.getTxnOperatorUnsafe()
	if txnOp == nil {
		logError(ses, ses.GetDebugString(),
			"RollbackTxn: txn operator is null",
			zap.String("sessionInfo", sessionInfo))
	}
	if txnCtx == nil {
		panic("context should not be nil")
	}
	if ses.tempTablestorage != nil {
		txnCtx = context.WithValue(txnCtx, defines.TemporaryTN{}, ses.tempTablestorage)
	}
	storage := txn.storage
	ctx2, cancel := context.WithTimeout(
		txnCtx,
		storage.Hints().CommitOrRollbackTimeout,
	)
	defer cancel()
	var err error
	defer func() {
		// metric count
		tenant := ses.GetTenantName()
		incTransactionCounter(tenant)
		incTransactionErrorsCounter(tenant, metric.SQLTypeOther) // exec rollback cnt
		if err != nil {
			incTransactionErrorsCounter(tenant, metric.SQLTypeRollback)
		}
	}()
	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		txnId := txnOp.Txn().DebugString()
		logDebugf(sessionInfo, "RollbackTxn txnId:%s", txnId)
		defer func() {
			logDebugf(sessionInfo, "RollbackTxn exit txnId:%s", txnId)
		}()
	}
	if txnOp != nil {
		txn.ses.SetTxnId(txnOp.Txn().ID)
		err = txnOp.Rollback(ctx2)
		if err != nil {
			txnId := txnOp.Txn().DebugString()
			logError(ses, ses.GetDebugString(),
				"RollbackTxn: txn operator commit failed",
				zap.String("txnId", txnId),
				zap.Error(err))
		}
	}
	txn.ses.SetTxnId(dumpUUID[:])
	return err
}

// RollbackTxn rollback anyway
func (txn *Txn) RollbackTxn() error {
	reqCtx, _ := txn.ses.conn.req.ctx.Ctx()
	_, span := trace.Start(reqCtx, "Txn.RollbackTxn",
		trace.WithKind(trace.SpanKindStatement))
	defer span.End(trace.WithStatementExtra(txn.ses.GetTxnId(), txn.ses.GetStmtId(), txn.ses.GetSqlOfStmt()))

	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.rollbackTxnUnsafe()
}

func (txn *Txn) cancelTxnCtx() {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if txn.mu.txnCancel != nil {
		txn.mu.txnCancel()
	}
}

func (txn *Txn) startStmt() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if !txn.isValidTxnOperatorUnsafe() {
		return nil
	}
	txn.mu.inStmt = false
	txnCtx, txnOp := txn.getTxnOperatorUnsafe()
	if txnOp != nil {
		txnOp.GetWorkspace().StartStatement()
		err := txnOp.GetWorkspace().IncrStatementID(txnCtx, false)
		if err != nil {
			return err
		}
		txn.mu.inStmt = true
	}
	return nil
}

func (txn *Txn) endStmt() {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if !txn.isValidTxnOperatorUnsafe() {
		return
	}
	defer func() {
		txn.mu.inStmt = false
	}()
	_, txnOp := txn.getTxnOperatorUnsafe()
	if txnOp != nil {
		txnOp.GetWorkspace().EndStatement()
	}
}

func (txn *Txn) rollbackStmt() error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	if !txn.isValidTxnOperatorUnsafe() {
		return nil
	}
	defer func() {
		txn.mu.inStmt = false
	}()
	txnCtx, txnOp := txn.getTxnOperatorUnsafe()
	if txnOp != nil {
		err := txnOp.GetWorkspace().RollbackLastStatement(txnCtx)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
InActiveTransaction checks if it is in an active transaction.
*/
func (txn *Txn) InActiveTransaction() bool {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.inActiveTransactionUnsafe()
}

func (txn *Txn) inActiveTransactionUnsafe() bool {
	return txn.serverStatusIsSetUnsafe(SERVER_STATUS_IN_TRANS) || txn.isValidTxnOperatorUnsafe()
}

func (txn *Txn) setAutocommitUnsafe(on bool) error {
	if on {
		// x -> 1
		if txn.optionBitsIsSetUnsafe(OPTION_AUTOCOMMIT) {
			//1 -> 1
			//nothing
		} else {
			//0 -> 1
			txnCtx, _ := txn.getTxnOperatorUnsafe()
			err := txn.commitTxnUnsafe(txnCtx)
			if err != nil {
				return err
			}
			txn.setOptionBitsUnsafe(OPTION_AUTOCOMMIT)
			txn.setServerStatusUnsafe(SERVER_STATUS_AUTOCOMMIT)
		}
	} else {
		// x -> 0
		if txn.optionBitsIsSetUnsafe(OPTION_AUTOCOMMIT) {
			//1 -> 0
			txn.clearOptionBitsUnsafe(OPTION_AUTOCOMMIT)
			txn.clearServerStatusUnsafe(SERVER_STATUS_AUTOCOMMIT)
		} else {
			//0 -> 0
			//nothing
		}
	}
	return nil
}

/*
SetAutocommit sets the value of the system variable 'autocommit'.

The rule is that we can not execute the statement 'set parameter = value' in
an active transaction whichever it is started by BEGIN or in 'set autocommit = 0;'.
*/
func (txn *Txn) SetAutocommit(on bool) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.SetAutocommit(on)
}

func (txn *Txn) setOptionBitsUnsafe(bit uint32) {
	txn.mu.optionBits |= bit
}

func (txn *Txn) SetOptionBits(bit uint32) {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.setOptionBitsUnsafe(bit)
}

func (txn *Txn) clearOptionBitsUnsafe(bit uint32) {
	txn.mu.optionBits &= ^bit
}

func (txn *Txn) optionBitsIsSetUnsafe(bit uint32) bool {
	return txn.mu.optionBits&bit != 0
}

func (txn *Txn) getOptionBitsUnsafe() uint32 {
	return txn.mu.optionBits
}

func (txn *Txn) GetOptionBits() uint32 {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.getOptionBitsUnsafe()
}

func (txn *Txn) setServerStatusUnsafe(bit uint16) {
	txn.mu.serverStatus |= bit
}

func (txn *Txn) SetServerStatus(bit uint16) {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.setServerStatusUnsafe(bit)
}

func (txn *Txn) clearServerStatusUnsafe(bit uint16) {
	txn.mu.serverStatus &= ^bit
}

func (txn *Txn) serverStatusIsSetUnsafe(bit uint16) bool {
	return txn.mu.serverStatus&bit != 0
}

func (txn *Txn) getServerStatusUnsafe() uint16 {
	return txn.mu.serverStatus
}

func (txn *Txn) GetServerStatus() uint16 {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.mu.serverStatus
}
