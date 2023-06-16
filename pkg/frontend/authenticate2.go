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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// verifyAccountCanOperateClusterTable determines the account can operate
// the cluster table
func verifyAccountCanOperateClusterTable(account *TenantInfo,
	dbName string,
	clusterTableOperation clusterTableOperationType) bool {
	if account.IsSysTenant() {
		//sys account can do anything on the cluster table.
		if dbName == moCatalog {
			return true
		}
	} else {
		//the general account can only read the cluster table
		if dbName == moCatalog {
			switch clusterTableOperation {
			case clusterTableNone, clusterTableSelect:
				return true
			}
		}
	}
	return false
}

// verifyLightPrivilege checks the privilege that does not need to
// access the privilege tables.
// case1 : checks if a real user from client is modifying the catalog databases (mo_catalog,information_schema,system,
// system_metric,mysql).
// case2 : checks if the user operates the cluster table.
func verifyLightPrivilege(ses *Session,
	dbName string,
	writeDBTableDirect bool,
	isClusterTable bool,
	clusterTableOperation clusterTableOperationType) bool {
	var ok bool
	if ses.GetFromRealUser() && writeDBTableDirect {
		if len(dbName) == 0 {
			dbName = ses.GetDatabaseName()
		}
		if ok2 := isBannedDatabase(dbName); ok2 {
			if isClusterTable {
				ok = verifyAccountCanOperateClusterTable(ses.GetTenantInfo(), dbName, clusterTableOperation)
			} else {
				ok = false
			}
		} else {
			ok = !isClusterTable
		}
	} else {
		ok = true
	}
	return ok
}

// getDefaultAccount returns the internal account
func getDefaultAccount() *TenantInfo {
	return &TenantInfo{
		Tenant:        sysAccountName,
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      sysAccountID,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
		delimiter:     ':',
	}
}

// stmtPrivilege denotes the kind of statement we can execute
type stmtPrivilege int

const (
	PrivilegeNone stmtPrivilege = iota
	CreateAccount
	DropAccount
	AlterAccount
	CreateUser
	CreateUserWithRole
	DropUser
	AlterUser
	CreateRole
	DropRole
	GrantRole
	GrantPrivilege
	RevokeRole
	RevokePrivilege
	CreateDatabase
	DropDatabase
	ShowDatabases
	ShowSequences
	Use
	ShowTables
	ShowCreateTable
	ShowColumns
	ShowCreateView
	ShowCreateDatabase
	ShowCreatePublications
	CreateTable
	CreateView
	CreateSequence
	AlterView
	AlterDataBaseConfig
	CreateFunction
	AlterTable
	CreateProcedure
	CallStmt
	DropTable
	DropView
	DropSequence
	DropFunction
	DropProcedure
	Select
	Do
	Insert
	Replace
	Load
	Update
	Delete
	CreateIndex
	DropIndex
	ShowProcessList
	ShowErrors
	ShowWarnings
	ShowVariables
	ShowStatus
	ShowTarget
	ShowTableStatusStmt
	ShowGrants
	ShowCollation
	ShowIndex
	ShowTableNumber
	ShowColumnNumber
	ShowTableValues
	ShowNodeList
	ShowRolesStmt
	ShowLocks
	ShowFunctionOrProcedureStatus
	ShowPublications
	ShowSubscriptions
	ShowBackendServers
	ShowAccounts
	ExplainFor
	ExplainAnalyze
	ExplainStmt
	BeginTransaction
	CommitTransaction
	RollbackTransaction
	SetVar
	SetDefaultRole
	SetRole
	SetPassword
	PrepareStmtPt
	PrepareString
	Deallocate
	Reset
	ExecutePt
	Declare
	InternalCmdFieldListPt
	ValuesStatement
	TruncateTable
	MoDump
	Kill
	LockTableStmt
	UnLockTableStmt
	CreatePublication
	DropPublication
	AlterPublication
	PrivilegeEnd
)

/*
if the item is in the cache,
return the true.

if the item is not in the cache,
1. evaluate the value.
2. if the value is true, put it into the cache.

*/
// privilegeCache cache privileges on table
type privilegeCache struct {
	store map[stmtPrivilege]any
}

func (pc *privilegeCache) has(priv stmtPrivilege) (bool, any) {
	value, ok := pc.store[priv]
	return ok, value
}

func (pc *privilegeCache) update(priv stmtPrivilege, value any) {
	pc.store[priv] = value
}

// invalidate makes the cache empty
func (pc *privilegeCache) invalidate() {

}

func hasStmtPrivilege(ctx context.Context, ses *Session, stmt tree.Statement, onlyCheckPlan bool, p *plan.Plan, priv stmtPrivilege) error {
	var err error
	//cache := ses.GetPrivilegeCache()
	//ok, _ := cache.has(priv)
	//if ok {
	//	return nil
	//}

	if !onlyCheckPlan {
		err = authenticateUserCanExecuteStatement(ctx, ses, stmt)
		if err != nil {
			return err
		}
	}

	if p != nil {
		err = authenticateCanExecuteStatementAndPlan(ctx, ses, stmt, p)
		if err != nil {
			return err
		}
	}
	//cache.update(priv, true)
	return err
}

func canExecStatement(ctx context.Context, ses *Session, stmt tree.Statement, onlyCheckPlan bool, p *plan.Plan) error {
	switch st := stmt.(type) {
	case *tree.CreateAccount:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, CreateAccount)
	case *tree.DropAccount:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, DropAccount)
	case *tree.AlterAccount:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, AlterAccount)
	case *tree.CreateUser:
		if st.Role == nil {
			return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, CreateUser)
		} else {
			return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, CreateUserWithRole)
		}
	case *tree.DropUser:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, DropUser)
	case *tree.AlterUser:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, AlterUser)
	case *tree.CreateRole:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, CreateRole)
	case *tree.DropRole:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, DropRole)
	case *tree.Grant:
		if st.Typ == tree.GrantTypeRole {
			return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, GrantRole)
		} else if st.Typ == tree.GrantTypePrivilege {
			return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, GrantPrivilege)
		}
	case *tree.GrantRole:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, GrantRole)
	case *tree.GrantPrivilege:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, GrantPrivilege)
	case *tree.Revoke:
		if st.Typ == tree.RevokeTypeRole {
			return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, RevokeRole)
		} else if st.Typ == tree.RevokeTypePrivilege {
			return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, RevokePrivilege)
		}
	case *tree.RevokeRole:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, RevokeRole)
	case *tree.RevokePrivilege:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, RevokePrivilege)
	case *tree.CreateDatabase:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, CreateDatabase)
	case *tree.DropDatabase:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, DropDatabase)
	case *tree.ShowDatabases:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowDatabases)
	case *tree.ShowSequences:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowSequences)
	case *tree.Use:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, Use)
	case *tree.ShowTables:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowTables)
	case *tree.ShowCreateTable:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowCreateTable)
	case *tree.ShowColumns:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowColumns)
	case *tree.ShowCreateView:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowCreateView)
	case *tree.ShowCreateDatabase:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowCreateDatabase)
	case *tree.ShowCreatePublications:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowCreatePublications)
	case *tree.CreateTable:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, CreateTable)
	case *tree.CreateView:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, CreateView)
	case *tree.CreateSequence:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, CreateSequence)
	case *tree.AlterView:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, AlterView)
	case *tree.AlterDataBaseConfig:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, AlterDataBaseConfig)
	case *tree.CreateFunction:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, CreateFunction)
	case *tree.AlterTable:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, AlterTable)
	case *tree.CreateProcedure:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, CreateProcedure)
	case *tree.CallStmt:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, CallStmt)
	case *tree.DropTable:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, DropTable)
	case *tree.DropView:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, DropView)
	case *tree.DropSequence:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, DropSequence)
	case *tree.DropFunction:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, DropFunction)
	case *tree.DropProcedure:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, DropProcedure)
	case *tree.Select:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, Select)
	case *tree.Do:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, Do)
	case *tree.Insert:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, Insert)
	case *tree.Replace:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, Replace)
	case *tree.Load:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, Load)
	case *tree.Update:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, Update)
	case *tree.Delete:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, Delete)
	case *tree.CreateIndex:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, CreateIndex)
	case *tree.DropIndex:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, DropIndex)
	case *tree.ShowProcessList:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowProcessList)
	case *tree.ShowErrors:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowErrors)
	case *tree.ShowWarnings:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowWarnings)
	case *tree.ShowVariables:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowVariables)
	case *tree.ShowStatus:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowStatus)
	case *tree.ShowTarget:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowTarget)
	case *tree.ShowTableStatus:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowTableStatusStmt)
	case *tree.ShowGrants:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowGrants)
	case *tree.ShowCollation:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowCollation)
	case *tree.ShowIndex:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowIndex)
	case *tree.ShowTableNumber:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowTableNumber)
	case *tree.ShowColumnNumber:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowColumnNumber)
	case *tree.ShowTableValues:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowTableValues)
	case *tree.ShowNodeList:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowNodeList)
	case *tree.ShowRolesStmt:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowRolesStmt)
	case *tree.ShowLocks:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowLocks)
	case *tree.ShowFunctionOrProcedureStatus:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowFunctionOrProcedureStatus)
	case *tree.ShowPublications:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowPublications)
	case *tree.ShowSubscriptions:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowSubscriptions)
	case *tree.ShowBackendServers:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowBackendServers)
	case *tree.ShowAccounts:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ShowAccounts)
	case *tree.ExplainFor:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ExplainFor)
	case *tree.ExplainAnalyze:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ExplainAnalyze)
	case *tree.ExplainStmt:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ExplainStmt)
	case *tree.BeginTransaction:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, BeginTransaction)
	case *tree.CommitTransaction:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, CommitTransaction)
	case *tree.RollbackTransaction:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, RollbackTransaction)
	case *tree.SetVar:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, SetVar)
	case *tree.SetDefaultRole:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, SetDefaultRole)
	case *tree.SetRole:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, SetRole)
	case *tree.SetPassword:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, SetPassword)
	case *tree.PrepareStmt:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, PrepareStmtPt)
	case *tree.PrepareString:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, PrepareString)
	case *tree.Deallocate:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, Deallocate)
	case *tree.Reset:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, Reset)
	case *tree.Execute:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ExecutePt)
	case *tree.Declare:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, Declare)
	case *InternalCmdFieldList:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, InternalCmdFieldListPt)
	case *tree.ValuesStatement:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, ValuesStatement)
	case *tree.TruncateTable:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, TruncateTable)
	case *tree.MoDump:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, MoDump)
	case *tree.Kill:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, Kill)
	case *tree.LockTableStmt:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, LockTableStmt)
	case *tree.UnLockTableStmt:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, UnLockTableStmt)
	case *tree.CreatePublication:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, CreatePublication)
	case *tree.DropPublication:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, DropPublication)
	case *tree.AlterPublication:
		return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, AlterPublication)
	default:
		panic(fmt.Sprintf("does not have the privilege definition of the statement %s", stmt))
	}
	return nil
}

