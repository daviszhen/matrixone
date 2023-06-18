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

/*
The Design of the Privilege Cache.

1. Background

From a business perspective, the MO just want to know the user can execute
the statement or not. The MO does not care about the details why the user can.

We design a corresponding stmtPrivilege with the same name for every statement below.
Before executing the statement, the MO check its stmtPrivilege first. The background
logic will evaluate if the user has the stmtPrivilege.

2. The content of the privilege cache

cache key: the stmtPrivilege
cache value: nil or special data structure based on the statement


*/
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

func (sp stmtPrivilege) String() string {
	switch sp {
	case PrivilegeNone:
		return "PrivilegeNone "
	case CreateAccount:
		return "CreateAccount"
	case DropAccount:
		return "DropAccount"
	case AlterAccount:
		return "AlterAccount"
	case CreateUser:
		return "CreateUser"
	case CreateUserWithRole:
		return "CreateUserWithRole"
	case DropUser:
		return "DropUser"
	case AlterUser:
		return "AlterUser"
	case CreateRole:
		return "CreateRole"
	case DropRole:
		return "DropRole"
	case GrantRole:
		return "GrantRole"
	case GrantPrivilege:
		return "GrantPrivilege"
	case RevokeRole:
		return "RevokeRole"
	case RevokePrivilege:
		return "RevokePrivilege"
	case CreateDatabase:
		return "CreateDatabase"
	case DropDatabase:
		return "DropDatabase"
	case ShowDatabases:
		return "ShowDatabases"
	case ShowSequences:
		return "ShowSequences"
	case Use:
		return "Use"
	case ShowTables:
		return "ShowTables"
	case ShowCreateTable:
		return "ShowCreateTable"
	case ShowColumns:
		return "ShowColumns"
	case ShowCreateView:
		return "ShowCreateView"
	case ShowCreateDatabase:
		return "ShowCreateDatabase"
	case ShowCreatePublications:
		return "ShowCreatePublications"
	case CreateTable:
		return "CreateTable"
	case CreateView:
		return "CreateView"
	case CreateSequence:
		return "CreateSequence"
	case AlterView:
		return "AlterView"
	case AlterDataBaseConfig:
		return "AlterDataBaseConfig"
	case CreateFunction:
		return "CreateFunction"
	case AlterTable:
		return "AlterTable"
	case CreateProcedure:
		return "CreateProcedure"
	case CallStmt:
		return "CallStmt"
	case DropTable:
		return "DropTable"
	case DropView:
		return "DropView"
	case DropSequence:
		return "DropSequence"
	case DropFunction:
		return "DropFunction"
	case DropProcedure:
		return "DropProcedure"
	case Select:
		return "Select"
	case Do:
		return "Do"
	case Insert:
		return "Insert"
	case Replace:
		return "Replace"
	case Load:
		return "Load"
	case Update:
		return "Update"
	case Delete:
		return "Delete"
	case CreateIndex:
		return "CreateIndex"
	case DropIndex:
		return "DropIndex"
	case ShowProcessList:
		return "ShowProcessList"
	case ShowErrors:
		return "ShowErrors"
	case ShowWarnings:
		return "ShowWarnings"
	case ShowVariables:
		return "ShowVariables"
	case ShowStatus:
		return "ShowStatus"
	case ShowTarget:
		return "ShowTarget"
	case ShowTableStatusStmt:
		return "ShowTableStatusStmt"
	case ShowGrants:
		return "ShowGrants"
	case ShowCollation:
		return "ShowCollation"
	case ShowIndex:
		return "ShowIndex"
	case ShowTableNumber:
		return "ShowTableNumber"
	case ShowColumnNumber:
		return "ShowColumnNumber"
	case ShowTableValues:
		return "ShowTableValues"
	case ShowNodeList:
		return "ShowNodeList"
	case ShowRolesStmt:
		return "ShowRolesStmt"
	case ShowLocks:
		return "ShowLocks"
	case ShowFunctionOrProcedureStatus:
		return "ShowFunctionOrProcedureStatus"
	case ShowPublications:
		return "ShowPublications"
	case ShowSubscriptions:
		return "ShowSubscriptions"
	case ShowBackendServers:
		return "ShowBackendServers"
	case ShowAccounts:
		return "ShowAccounts"
	case ExplainFor:
		return "ExplainFor"
	case ExplainAnalyze:
		return "ExplainAnalyze"
	case ExplainStmt:
		return "ExplainStmt"
	case BeginTransaction:
		return "BeginTransaction"
	case CommitTransaction:
		return "CommitTransaction"
	case RollbackTransaction:
		return "RollbackTransaction"
	case SetVar:
		return "SetVar"
	case SetDefaultRole:
		return "SetDefaultRole"
	case SetRole:
		return "SetRole"
	case SetPassword:
		return "SetPassword"
	case PrepareStmtPt:
		return "PrepareStmtPt"
	case PrepareString:
		return "PrepareString"
	case Deallocate:
		return "Deallocate"
	case Reset:
		return "Reset"
	case ExecutePt:
		return "ExecutePt"
	case Declare:
		return "Declare"
	case InternalCmdFieldListPt:
		return "InternalCmdFieldListPt"
	case ValuesStatement:
		return "ValuesStatement"
	case TruncateTable:
		return "TruncateTable"
	case MoDump:
		return "MoDump"
	case Kill:
		return "Kill"
	case LockTableStmt:
		return "LockTableStmt"
	case UnLockTableStmt:
		return "UnLockTableStmt"
	case CreatePublication:
		return "CreatePublication"
	case DropPublication:
		return "DropPublication"
	case AlterPublication:
		return "AlterPublication"
	case PrivilegeEnd:
		return "PrivilegeEnd"
	default:
		return fmt.Sprintf("unknown privilege %d", sp)
	}
}

type stmtPrivliegeOption struct {
	writeDatabaseAndTableDirectly bool
	dbName                        string
	tableName                     string
	clusterTable                  bool
	clusterTableOperation         clusterTableOperationType
}

func (spo *stmtPrivliegeOption) String() string {
	return fmt.Sprintf("writeDatabaseAndTableDirectly: %v, dbName: %s, tableName: %s, clusterTable: %v, clusterTableOperation: %v",
		spo.writeDatabaseAndTableDirectly,
		spo.dbName,
		spo.tableName,
		spo.clusterTable,
		spo.clusterTableOperation)
}

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

func newPrivilegeCache() *privilegeCache {
	return &privilegeCache{store: make(map[stmtPrivilege]any)}
}

func (pc *privilegeCache) has(priv stmtPrivilege) (bool, any) {
	item, ok := pc.store[priv]
	return ok, item
}

func (pc *privilegeCache) update(priv stmtPrivilege, item any) {
	pc.store[priv] = item
}

// invalidate makes the cache empty
func (pc *privilegeCache) invalidate() {

}

type dbCacheItem struct {
	clusterTable   bool
	clusterTableOp clusterTableOperationType
}

type databaseCache struct {
	store map[string]*dbCacheItem
}

func (dc *databaseCache) has(dbName string) (bool, *dbCacheItem) {
	if v, ok := dc.store[dbName]; ok {
		return ok, v
	}
	return false, nil
}

func (dc *databaseCache) add(dbName string, item *dbCacheItem) {
	dc.store[dbName] = item
}

func hasStmtPrivilege(ctx context.Context, ses *Session, stmt tree.Statement, onlyCheckPlan bool, p *plan.Plan, priv stmtPrivilege, option stmtPrivliegeOption) error {
	var err error
	fmt.Println("[[[[[[", priv, option)
	defer func() {
		fmt.Println("]]]]]]", priv, option)
	}()
	cache := ses.GetPrivilegeCache()
	ok, item := cache.has(priv)
	if ok {
		if option.writeDatabaseAndTableDirectly {
			switch priv {
			case DropDatabase, CreateTable, CreateView,
				CreateSequence, AlterView, CreateFunction,
				AlterTable, CreateProcedure, CallStmt,
				DropTable, DropView, DropSequence,
				DropFunction, DropProcedure, Load,
				CreateIndex, DropIndex, TruncateTable:
				//check database name
				dbName := option.dbName
				if len(dbName) == 0 {
					dbName = ses.GetDatabaseName()
				}
				fmt.Println("--->2", priv, dbName)
				if dbCache, ok2 := item.(*databaseCache); ok2 {
					if ok3, value := dbCache.has(dbName); ok3 {
						fmt.Println("--->3", priv, "database ", dbName, "has been cached")
						if priv == CreateTable {
							if value != nil {
								if value.clusterTable == option.clusterTable &&
									value.clusterTableOp == option.clusterTableOperation {
									return nil
								}
							}
						} else {
							return nil
						}
					}
				}
			case Insert, Replace, Update, Delete:
			}
		} else {
			fmt.Println("--->", priv, "has been cached")
			return nil
		}
	}

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
	fmt.Println("+++>", priv, "has been evaluated")
	if option.writeDatabaseAndTableDirectly {
		switch priv {
		case DropDatabase, CreateTable, CreateView:
			//check database name
			dbName := option.dbName
			if len(dbName) == 0 {
				dbName = ses.GetDatabaseName()
			}
			if item == nil {
				item = &databaseCache{store: make(map[string]*dbCacheItem)}
			}
			fmt.Println("+++>2", priv, dbName)
			if dbCache, ok2 := item.(*databaseCache); ok2 {
				fmt.Println("+++>3", priv, "database ", dbName, "has been cached")
				var value *dbCacheItem
				if priv == CreateTable {
					value = &dbCacheItem{
						clusterTable:   option.clusterTable,
						clusterTableOp: option.clusterTableOperation}
				}
				dbCache.add(dbName, value)
			}
		}
	}
	cache.update(priv, item)
	return err
}

func canExecStatement(ctx context.Context, ses *Session, stmt tree.Statement, onlyCheckPlan bool, p *plan.Plan) error {
	var stmtPriv = PrivilegeNone
	var option stmtPrivliegeOption
	switch st := stmt.(type) {
	case *tree.CreateAccount:
		stmtPriv = CreateAccount
	case *tree.DropAccount:
		stmtPriv = DropAccount
	case *tree.AlterAccount:
		stmtPriv = AlterAccount
	case *tree.CreateUser:
		if st.Role == nil {
			stmtPriv = CreateUser
		} else {
			stmtPriv = CreateUserWithRole
		}
	case *tree.DropUser:
		stmtPriv = DropUser
	case *tree.AlterUser:
		stmtPriv = AlterUser
	case *tree.CreateRole:
		stmtPriv = CreateRole
	case *tree.DropRole:
		stmtPriv = DropRole
	case *tree.Grant:
		if st.Typ == tree.GrantTypeRole {
			stmtPriv = GrantRole
		} else if st.Typ == tree.GrantTypePrivilege {
			stmtPriv = GrantPrivilege
		}
	case *tree.GrantRole:
		stmtPriv = GrantRole
	case *tree.GrantPrivilege:
		stmtPriv = GrantPrivilege
	case *tree.Revoke:
		if st.Typ == tree.RevokeTypeRole {
			stmtPriv = RevokeRole
		} else if st.Typ == tree.RevokeTypePrivilege {
			stmtPriv = RevokePrivilege
		}
	case *tree.RevokeRole:
		stmtPriv = RevokeRole
	case *tree.RevokePrivilege:
		stmtPriv = RevokePrivilege
	case *tree.CreateDatabase:
		stmtPriv = CreateDatabase
	case *tree.DropDatabase:
		stmtPriv = DropDatabase
		option.writeDatabaseAndTableDirectly = true
		option.dbName = string(st.Name)
	case *tree.ShowDatabases:
		stmtPriv = ShowDatabases
	case *tree.ShowSequences:
		stmtPriv = ShowSequences
	case *tree.Use:
		stmtPriv = Use
	case *tree.ShowTables:
		stmtPriv = ShowTables
	case *tree.ShowCreateTable:
		stmtPriv = ShowCreateTable
	case *tree.ShowColumns:
		stmtPriv = ShowColumns
	case *tree.ShowCreateView:
		stmtPriv = ShowCreateView
	case *tree.ShowCreateDatabase:
		stmtPriv = ShowCreateDatabase
	case *tree.ShowCreatePublications:
		stmtPriv = ShowCreatePublications
	case *tree.CreateTable:
		stmtPriv = CreateTable
		option.writeDatabaseAndTableDirectly = true
		option.dbName = string(st.Table.SchemaName)
		if st.IsClusterTable {
			option.clusterTable = true
			option.clusterTableOperation = clusterTableCreate
		}
	case *tree.CreateView:
		stmtPriv = CreateView
		option.writeDatabaseAndTableDirectly = true
		if st.Name != nil {
			option.dbName = string(st.Name.SchemaName)
		}
	case *tree.CreateSequence:
		stmtPriv = CreateSequence
		option.writeDatabaseAndTableDirectly = true
		if st.Name != nil {
			option.dbName = string(st.Name.SchemaName)
		}
	case *tree.AlterView:
		stmtPriv = AlterView
		option.writeDatabaseAndTableDirectly = true
		if st.Name != nil {
			option.dbName = string(st.Name.SchemaName)
		}
	case *tree.AlterDataBaseConfig:
		stmtPriv = AlterDataBaseConfig
	case *tree.CreateFunction:
		stmtPriv = CreateFunction
		option.writeDatabaseAndTableDirectly = true
	case *tree.AlterTable:
		stmtPriv = AlterTable
		option.writeDatabaseAndTableDirectly = true
		if st.Table != nil {
			option.dbName = string(st.Table.SchemaName)
		}
	case *tree.CreateProcedure:
		stmtPriv = CreateProcedure
		option.writeDatabaseAndTableDirectly = true
	case *tree.CallStmt:
		stmtPriv = CallStmt
		option.writeDatabaseAndTableDirectly = true
	case *tree.DropTable:
		stmtPriv = DropTable
		option.writeDatabaseAndTableDirectly = true
		if len(st.Names) != 0 {
			option.dbName = string(st.Names[0].SchemaName)
		}
	case *tree.DropView:
		stmtPriv = DropView
		option.writeDatabaseAndTableDirectly = true
		if len(st.Names) != 0 {
			option.dbName = string(st.Names[0].SchemaName)
		}
	case *tree.DropSequence:
		stmtPriv = DropSequence
		option.writeDatabaseAndTableDirectly = true
		if len(st.Names) != 0 {
			option.dbName = string(st.Names[0].SchemaName)
		}
	case *tree.DropFunction:
		stmtPriv = DropFunction
		option.writeDatabaseAndTableDirectly = true
	case *tree.DropProcedure:
		stmtPriv = DropProcedure
		option.writeDatabaseAndTableDirectly = true
	case *tree.Select:
		stmtPriv = Select
	case *tree.Do:
		stmtPriv = Do
	case *tree.Insert:
		stmtPriv = Insert
		option.writeDatabaseAndTableDirectly = true
	case *tree.Replace:
		stmtPriv = Replace
		option.writeDatabaseAndTableDirectly = true
	case *tree.Load:
		stmtPriv = Load
		option.writeDatabaseAndTableDirectly = true
		if st.Table != nil {
			option.dbName = string(st.Table.SchemaName)
		}
	case *tree.Update:
		stmtPriv = Update
		option.writeDatabaseAndTableDirectly = true
	case *tree.Delete:
		stmtPriv = Delete
		option.writeDatabaseAndTableDirectly = true
	case *tree.CreateIndex:
		stmtPriv = CreateIndex
		option.writeDatabaseAndTableDirectly = true
	case *tree.DropIndex:
		stmtPriv = DropIndex
		option.writeDatabaseAndTableDirectly = true
	case *tree.ShowProcessList:
		stmtPriv = ShowProcessList
	case *tree.ShowErrors:
		stmtPriv = ShowErrors
	case *tree.ShowWarnings:
		stmtPriv = ShowWarnings
	case *tree.ShowVariables:
		stmtPriv = ShowVariables
	case *tree.ShowStatus:
		stmtPriv = ShowStatus
	case *tree.ShowTarget:
		stmtPriv = ShowTarget
	case *tree.ShowTableStatus:
		stmtPriv = ShowTableStatusStmt
	case *tree.ShowGrants:
		stmtPriv = ShowGrants
	case *tree.ShowCollation:
		stmtPriv = ShowCollation
	case *tree.ShowIndex:
		stmtPriv = ShowIndex
	case *tree.ShowTableNumber:
		stmtPriv = ShowTableNumber
	case *tree.ShowColumnNumber:
		stmtPriv = ShowColumnNumber
	case *tree.ShowTableValues:
		stmtPriv = ShowTableValues
	case *tree.ShowNodeList:
		stmtPriv = ShowNodeList
	case *tree.ShowRolesStmt:
		stmtPriv = ShowRolesStmt
	case *tree.ShowLocks:
		stmtPriv = ShowLocks
	case *tree.ShowFunctionOrProcedureStatus:
		stmtPriv = ShowFunctionOrProcedureStatus
	case *tree.ShowPublications:
		stmtPriv = ShowPublications
	case *tree.ShowSubscriptions:
		stmtPriv = ShowSubscriptions
	case *tree.ShowBackendServers:
		stmtPriv = ShowBackendServers
	case *tree.ShowAccounts:
		stmtPriv = ShowAccounts
	case *tree.ExplainFor:
		stmtPriv = ExplainFor
	case *tree.ExplainAnalyze:
		stmtPriv = ExplainAnalyze
	case *tree.ExplainStmt:
		stmtPriv = ExplainStmt
	case *tree.BeginTransaction:
		stmtPriv = BeginTransaction
	case *tree.CommitTransaction:
		stmtPriv = CommitTransaction
	case *tree.RollbackTransaction:
		stmtPriv = RollbackTransaction
	case *tree.SetVar:
		stmtPriv = SetVar
	case *tree.SetDefaultRole:
		stmtPriv = SetDefaultRole
	case *tree.SetRole:
		stmtPriv = SetRole
	case *tree.SetPassword:
		stmtPriv = SetPassword
	case *tree.PrepareStmt:
		stmtPriv = PrepareStmtPt
	case *tree.PrepareString:
		stmtPriv = PrepareString
	case *tree.Deallocate:
		stmtPriv = Deallocate
	case *tree.Reset:
		stmtPriv = Reset
	case *tree.Execute:
		stmtPriv = ExecutePt
	case *tree.Declare:
		stmtPriv = Declare
	case *InternalCmdFieldList:
		stmtPriv = InternalCmdFieldListPt
	case *tree.ValuesStatement:
		stmtPriv = ValuesStatement
	case *tree.TruncateTable:
		stmtPriv = TruncateTable
		option.writeDatabaseAndTableDirectly = true
		if st.Name != nil {
			option.dbName = string(st.Name.SchemaName)
		}
	case *tree.MoDump:
		stmtPriv = MoDump
	case *tree.Kill:
		stmtPriv = Kill
	case *tree.LockTableStmt:
		stmtPriv = LockTableStmt
	case *tree.UnLockTableStmt:
		stmtPriv = UnLockTableStmt
	case *tree.CreatePublication:
		stmtPriv = CreatePublication
	case *tree.DropPublication:
		stmtPriv = DropPublication
	case *tree.AlterPublication:
		stmtPriv = AlterPublication
	default:
		panic(fmt.Sprintf("does not have the privilege definition of the statement %s", stmt))
	}
	return hasStmtPrivilege(ctx, ses, stmt, onlyCheckPlan, p, stmtPriv, option)
}

