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
