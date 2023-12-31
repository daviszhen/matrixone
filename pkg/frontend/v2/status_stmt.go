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
	"errors"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func newopts(opts []ExecutorOpt) *ExecutorOptions {
	o := &ExecutorOptions{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

var _ Executor = &UseExecutor{}

type BeginTxnExecutor struct {
	bt *tree.BeginTransaction
}

type CommitTxnExecutor struct {
	ct *tree.CommitTransaction
}

type RollbackTxnExecutor struct {
	rt *tree.RollbackTransaction
}

type SetRoleExecutor struct {
	sr *tree.SetRole
}

type UseExecutor struct {
	newopts *ExecutorOptions
	u       *tree.Use
	db      string
}

func (use *UseExecutor) Open(ctx context.Context, opts ...ExecutorOpt) error {
	use.newopts = newopts(opts)
	var v interface{}
	var err error
	v, err = use.newopts.ses.GetGlobalVar("lower_case_table_names")
	if err != nil {
		return err
	}
	use.u.Name.SetConfig(v.(int64))

	use.db = use.u.Name.Compare()
	return err
}
func (use *UseExecutor) Label() Label {
	ret := Label(TxnExistsAferExc)
	if !use.u.IsUseRole() {
		/*
			These statements can not be executed in an uncommitted transaction:
				USE SECONDARY ROLE { ALL | NONE }
				USE ROLE role;
		*/
		ret |= CanExecInUncommittedTxnTxn
	}
	return ret
}

func (use *UseExecutor) doUse(ctx context.Context) (err error) {
	var txnCtx context.Context
	var txn TxnOperator
	var dbMeta engine.Database
	txnCtx, txn = use.newopts.ses.txn.GetTxnOperator()
	//TODO: check meta data
	if dbMeta, err = fePu.StorageEngine.Database(txnCtx, use.db, txn); err != nil {
		//echo client. no such database
		return moerr.NewBadDB(ctx, use.db)
	}
	if dbMeta.IsSubscription(ctx) {
		_, err = checkSubscriptionValid(ctx, use.newopts.ses, dbMeta.GetCreateSql(ctx))
		if err != nil {
			return err
		}
	}
	// oldDB := use.newopts.ses.GetDatabaseName()
	use.newopts.ses.SetDatabaseName(use.db)

	// logDebugf(use.newopts.ses.GetDebugString(), "User %s change database from [%s] to [%s]", ses.GetUserName(), oldDB, ses.GetDatabaseName())

	err = changeVersion(ctx, use.newopts.ses, use.u.Name.Compare())
	if err != nil {
		return err
	}
	return err
}

func (use *UseExecutor) Next(ctx context.Context) (err error) {
	err = use.doUse(ctx)
	if err != nil {
		err2 := use.newopts.endPoint.SendErrorPacket(ctx, use.newopts.ses.conn, err)
		if err != nil {
			return errors.Join(err, err2)
		}
	} else {
		status := use.newopts.ses.txn.GetServerStatus()
		if !use.newopts.isLastStmt {
			status |= SERVER_MORE_RESULTS_EXISTS
		}
		err = use.newopts.endPoint.SendOkPacket(ctx, use.newopts.ses.conn,
			0, 0, status, 0, "")
	}
	return err
}
func (use *UseExecutor) Close(context.Context) error {
	use.u = nil
	use.newopts = nil
	use.db = ""
	return nil
}

type DropDatabaseExecutor struct {
	dd *tree.DropDatabase
}

type PrepareStmtExecutor struct {
	ps          *tree.PrepareStmt
	prepareStmt *PrepareStmt
}

type PrepareStringExecutor struct {
	ps          *tree.PrepareString
	prepareStmt *PrepareStmt
}

type DeallocateExecutor struct {
	d *tree.Deallocate
}
type SetVarExecutor struct {
	sv *tree.SetVar
}

type DeleteExecutor struct {
	d *tree.Delete
}

type UpdateExecutor struct {
	u *tree.Update
}

type DropPublicationExecutor struct {
	dp *tree.DropPublication
}

type AlterPublicationExecutor struct {
	ap *tree.AlterPublication
}

type CreatePublicationExecutor struct {
	cp *tree.CreatePublication
}

type CreateAccountExecutor struct {
	ca *tree.CreateAccount
}

type DropAccountExecutor struct {
	da *tree.DropAccount
}

type AlterAccountExecutor struct {
	aa *tree.AlterAccount
}

type CreateUserExecutor struct {
	cu *tree.CreateUser
}

type DropUserExecutor struct {
	du *tree.DropUser
}

type AlterUserExecutor struct {
	au *tree.AlterUser
}

type CreateRoleExecutor struct {
	cr *tree.CreateRole
}

type DropRoleExecutor struct {
	dr *tree.DropRole
}

type GrantExecutor struct {
	g *tree.Grant
}

type RevokeExecutor struct {
	r *tree.Revoke
}

type CreateTableExecutor struct {
	ct *tree.CreateTable
}

type DropTableExecutor struct {
	dt *tree.DropTable
}

type CreateDatabaseExecutor struct {
	cd *tree.CreateDatabase
}

type CreateIndexExecutor struct {
	ci *tree.CreateIndex
}

type DropIndexExecutor struct {
	di *tree.DropIndex
}

type CreateViewExecutor struct {
	cv *tree.CreateView
}

type AlterViewExecutor struct {
	av *tree.AlterView
}

type CreateSequenceExecutor struct {
	cs *tree.CreateSequence
}

type DropSequenceExecutor struct {
	ds *tree.DropSequence
}

type AlterSequenceExecutor struct {
	cs *tree.AlterSequence
}

type DropViewExecutor struct {
	dv *tree.DropView
}

type AlterTableExecutor struct {
	at *tree.AlterTable
}

type InsertExecutor struct {
	i *tree.Insert
}

type ReplaceExecutor struct {
	r *tree.Replace
}

type LoadExecutor struct {
	l *tree.Load
}

type SetDefaultRoleExecutor struct {
	sdr *tree.SetDefaultRole
}

type SetPasswordExecutor struct {
	sp *tree.SetPassword
}

type TruncateTableExecutor struct {
	tt *tree.TruncateTable
}
