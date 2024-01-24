package frontend

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func (mce *MysqlCmdExecutor) selfHandle(requestCtx context.Context,
	ses *Session,
	proc *process.Process,
	proto MysqlProtocol,
	pu *config.ParameterUnit,
	execCtx *ExecCtx,
) (err error) {
	switch execCtx.stmt.(type) {
	case *tree.BeginTransaction:
		err = ses.TxnBegin()
		if err != nil {
			return
		}
		RecordStatementTxnID(requestCtx, ses)
	case *tree.CommitTransaction:
		err = ses.TxnCommit()
		if err != nil {
			return
		}
	case *tree.RollbackTransaction:
		err = ses.TxnRollback()
		if err != nil {
			return
		}
	}

	switch st := execCtx.stmt.(type) {
	case *InternalCmdFieldList:
		if err = mce.handleCmdFieldList(requestCtx, st); err != nil {
			return
		}
	case *tree.CreatePublication:
		if err = mce.handleCreatePublication(requestCtx, st); err != nil {
			return
		}
	case *tree.AlterPublication:
		if err = mce.handleAlterPublication(requestCtx, st); err != nil {
			return
		}
	case *tree.DropPublication:
		if err = mce.handleDropPublication(requestCtx, st); err != nil {
			return
		}
	case *tree.ShowSubscriptions:
		if err = mce.handleShowSubscriptions(requestCtx, st, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.CreateStage:
		if err = mce.handleCreateStage(requestCtx, st); err != nil {
			return
		}
	case *tree.DropStage:
		if err = mce.handleDropStage(requestCtx, st); err != nil {
			return
		}
	case *tree.AlterStage:
		if err = mce.handleAlterStage(requestCtx, st); err != nil {
			return
		}
	case *tree.CreateAccount:
		ses.InvalidatePrivilegeCache()
		if err = mce.handleCreateAccount(requestCtx, st); err != nil {
			return
		}
	case *tree.DropAccount:
		ses.InvalidatePrivilegeCache()
		if err = mce.handleDropAccount(requestCtx, st); err != nil {
			return
		}
	case *tree.AlterAccount:
		ses.InvalidatePrivilegeCache()
		if err = mce.handleAlterAccount(requestCtx, st); err != nil {
			return
		}
	case *tree.AlterDataBaseConfig:
		ses.InvalidatePrivilegeCache()
		if st.IsAccountLevel {
			if err = mce.handleAlterAccountConfig(requestCtx, ses, st); err != nil {
				return
			}
		} else {
			if err = mce.handleAlterDataBaseConfig(requestCtx, ses, st); err != nil {
				return
			}
		}
	case *tree.CreateUser:
		ses.InvalidatePrivilegeCache()
		if err = mce.handleCreateUser(requestCtx, st); err != nil {
			return
		}
	case *tree.DropUser:
		ses.InvalidatePrivilegeCache()
		if err = mce.handleDropUser(requestCtx, st); err != nil {
			return
		}
	case *tree.AlterUser: //TODO
		ses.InvalidatePrivilegeCache()
		if err = mce.handleAlterUser(requestCtx, st); err != nil {
			return
		}
	case *tree.CreateRole:
		ses.InvalidatePrivilegeCache()
		if err = mce.handleCreateRole(requestCtx, st); err != nil {
			return
		}
	case *tree.DropRole:
		ses.InvalidatePrivilegeCache()
		if err = mce.handleDropRole(requestCtx, st); err != nil {
			return
		}
	case *tree.CreateFunction:
		if err = st.Valid(); err != nil {
			return err
		}
		if err = mce.handleCreateFunction(requestCtx, st); err != nil {
			return
		}
	case *tree.DropFunction:
		if err = mce.handleDropFunction(requestCtx, st, proc); err != nil {
			return
		}
	case *tree.CreateProcedure:
		if err = mce.handleCreateProcedure(requestCtx, st); err != nil {
			return
		}
	case *tree.DropProcedure:
		if err = mce.handleDropProcedure(requestCtx, st); err != nil {
			return
		}
	case *tree.CallStmt:
		if err = mce.handleCallProcedure(requestCtx, st, proc); err != nil {
			return
		}

	case *tree.Grant:
		ses.InvalidatePrivilegeCache()
		switch st.Typ {
		case tree.GrantTypeRole:
			if err = mce.handleGrantRole(requestCtx, &st.GrantRole); err != nil {
				return
			}
		case tree.GrantTypePrivilege:
			if err = mce.handleGrantPrivilege(requestCtx, &st.GrantPrivilege); err != nil {
				return
			}
		}
	case *tree.Revoke:
		ses.InvalidatePrivilegeCache()
		switch st.Typ {
		case tree.RevokeTypeRole:
			if err = mce.handleRevokeRole(requestCtx, &st.RevokeRole); err != nil {
				return
			}
		case tree.RevokeTypePrivilege:
			if err = mce.handleRevokePrivilege(requestCtx, &st.RevokePrivilege); err != nil {
				return
			}
		}
	case *tree.Kill:
		ses.InvalidatePrivilegeCache()
		if err = mce.handleKill(requestCtx, st); err != nil {
			return
		}
	case *tree.ShowAccounts:
		if err = mce.handleShowAccounts(requestCtx, st, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.ShowBackendServers:
		if err = mce.handleShowBackendServers(requestCtx, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.SetTransaction:
		//TODO: handle set transaction
	case *tree.LockTableStmt:

	case *tree.UnLockTableStmt:
	case *tree.BackupStart:
		if err = mce.handleStartBackup(requestCtx, st); err != nil {
			return
		}
	case *tree.EmptyStmt:
		if err = mce.handleEmptyStmt(requestCtx, st); err != nil {
			return
		}
	case *tree.SetRole:
		ses.InvalidatePrivilegeCache()
		//switch role
		err = mce.handleSwitchRole(requestCtx, st)
		if err != nil {
			return
		}
	case *tree.Use:
		var v interface{}
		v, err = ses.GetGlobalVar("lower_case_table_names")
		if err != nil {
			return
		}
		st.Name.SetConfig(v.(int64))
		//use database
		err = mce.handleChangeDB(requestCtx, st.Name.Compare())
		if err != nil {
			return
		}
		err = changeVersion(requestCtx, ses, st.Name.Compare())
		if err != nil {
			return
		}
	case *tree.MoDump:
		//dump
		err = mce.handleDump(requestCtx, st)
		if err != nil {
			return
		}
	case *tree.PrepareStmt:
		execCtx.prepareStmt, err = mce.handlePrepareStmt(requestCtx, st, execCtx.sqlOfStmt)
		if err != nil {
			return
		}
		err = authenticateUserCanExecutePrepareOrExecute(requestCtx, ses, execCtx.prepareStmt.PrepareStmt, execCtx.prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
		if err != nil {
			mce.GetSession().RemovePrepareStmt(execCtx.prepareStmt.Name)
			return
		}
	case *tree.PrepareString:
		execCtx.prepareStmt, err = mce.handlePrepareString(requestCtx, st)
		if err != nil {
			return
		}
		err = authenticateUserCanExecutePrepareOrExecute(requestCtx, ses, execCtx.prepareStmt.PrepareStmt, execCtx.prepareStmt.PreparePlan.GetDcl().GetPrepare().GetPlan())
		if err != nil {
			mce.GetSession().RemovePrepareStmt(execCtx.prepareStmt.Name)
			return
		}
	case *tree.CreateConnector:
		err = mce.handleCreateConnector(requestCtx, st)
		if err != nil {
			return
		}
	case *tree.PauseDaemonTask:
		err = mce.handlePauseDaemonTask(requestCtx, st)
		if err != nil {
			return
		}
	case *tree.CancelDaemonTask:
		err = mce.handleCancelDaemonTask(requestCtx, st.TaskID)
		if err != nil {
			return
		}
	case *tree.ResumeDaemonTask:
		err = mce.handleResumeDaemonTask(requestCtx, st)
		if err != nil {
			return
		}
	case *tree.DropConnector:
		err = mce.handleDropConnector(requestCtx, st)
		if err != nil {
			return
		}
	case *tree.ShowConnectors:
		if err = mce.handleShowConnectors(requestCtx, execCtx.isLastStmt); err != nil {
			return
		}
	case *tree.Deallocate:
		err = mce.handleDeallocate(requestCtx, st)
		if err != nil {
			return
		}
	case *tree.Reset:
		err = mce.handleReset(requestCtx, st)
		if err != nil {
			return
		}
	case *tree.SetVar:
		err = mce.handleSetVar(requestCtx, st, execCtx.sqlOfStmt)
		if err != nil {
			return
		}
	case *tree.ShowVariables:
		err = mce.handleShowVariables(st, proc, execCtx.isLastStmt)
		if err != nil {
			return
		}
	case *tree.ShowErrors, *tree.ShowWarnings:
		err = mce.handleShowErrors(execCtx.isLastStmt)
		if err != nil {
			return
		}
	case *tree.AnalyzeStmt:
		if err = mce.handleAnalyzeStmt(requestCtx, ses, st); err != nil {
			return
		}
	case *tree.ExplainStmt:
		if err = mce.handleExplainStmt(requestCtx, st); err != nil {
			return
		}
	}
	return
}
