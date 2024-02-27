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

package tree

import "fmt"

type Statement interface {
	fmt.Stringer
	NodeFormatter
	StatementType
	ResultType() ResultType
	HandleType() HandleType
}

type StatementType interface {
	// GetStatementType return like insert, update, delete, begin, rename database, rename table, ...
	GetStatementType() string
	// GetQueryType return val like DQL, DML, DDL, ...
	GetQueryType() string
}

type ResultType int

const (
	RowSet ResultType = iota
	Status
	NoResp //like COM_QUIT, Deallocate
	Undefined
)

type HandleType int

const (
	InFrontend HandleType = iota
	InBackend
	Unknown
)

type statementImpl struct {
	Statement
}

/*
func (si *statementImpl) ResultType() ResultType {
	return Undefined
}

func (si *statementImpl) HandleType() HandleType {
	return Unknown
}
*/

const (
	// QueryTypeDQL (Data Query Language) Select, MoDump, ValuesStatement, With
	QueryTypeDQL = "DQL"
	// QueryTypeDDL (Data Definition Language): CreateDatabase, DropDatabase, DropTable,
	// Create/Drop/Alter/Rename Database/Table/View/Index/Function, TruncateTable,
	QueryTypeDDL = "DDL"
	// QueryTypeDML (Data Manipulation Language): Insert, Update, Delete, Load
	QueryTypeDML = "DML"
	// QueryTypeDCL (Data Control Language)
	// statement: Grant, Revoke
	// CreateAccount, CreateUser, CreateRole, AlterAccount, AlterUser, DropAccount, DropUser, DropRole
	QueryTypeDCL = "DCL"
	// QueryTypeTCL (Transaction Control Language): BeginTransaction, RollbackTransaction, CommitTransaction, Savepoint(Not Support)
	QueryTypeTCL = "TCL"
	// QueryTypeOth (Other.)
	// statement: AnalyzeStmt(Not Support), ExplainStmt, ExplainAnalyze, ExplainFor,
	// SetVar, SetDefaultRole, SetRole, SetPassword, Declare, Do, TableFunction, Use, PrepareStmt, Execute, Deallocate, Kill
	// Show ..., ShowCreateTable, ShowColumns(Desc)
	QueryTypeOth = "Other"
)

func (node *Select) HandleType() HandleType {
	return InBackend
}

func (node *Select) ResultType() ResultType {
	if node.Ep != nil {
		return Status
	}
	return RowSet
}

func (node *Use) HandleType() HandleType {
	return InFrontend
}

func (node *Use) ResultType() ResultType {
	return Status
}

func (node *BeginTransaction) HandleType() HandleType {
	return InFrontend
}

func (node *BeginTransaction) ResultType() ResultType {
	return Status
}

func (node *CommitTransaction) HandleType() HandleType {
	return InFrontend
}

func (node *CommitTransaction) ResultType() ResultType {
	return Status
}

func (node *RollbackTransaction) HandleType() HandleType {
	return InFrontend
}

func (node *RollbackTransaction) ResultType() ResultType {
	return Status
}

func (node *CreatePublication) HandleType() HandleType {
	return InFrontend
}

func (node *CreatePublication) ResultType() ResultType {
	return Status
}

func (node *AlterPublication) HandleType() HandleType {
	return InFrontend
}

func (node *AlterPublication) ResultType() ResultType {
	return Status
}

func (node *DropPublication) HandleType() HandleType {
	return InFrontend
}

func (node *DropPublication) ResultType() ResultType {
	return Status
}

func (node *ShowSubscriptions) HandleType() HandleType {
	return InFrontend
}

func (node *ShowSubscriptions) ResultType() ResultType {
	return RowSet
}

func (node *CreateStage) HandleType() HandleType {
	return InFrontend
}

func (node *CreateStage) ResultType() ResultType {
	return Status
}

func (node *DropStage) HandleType() HandleType {
	return InFrontend
}

func (node *DropStage) ResultType() ResultType {
	return Status
}

func (node *AlterStage) HandleType() HandleType {
	return InFrontend
}

func (node *AlterStage) ResultType() ResultType {
	return Status
}

func (node *CreateAccount) HandleType() HandleType {
	return InFrontend
}

func (node *CreateAccount) ResultType() ResultType {
	return Status
}

func (node *DropAccount) HandleType() HandleType {
	return InFrontend
}

func (node *DropAccount) ResultType() ResultType {
	return Status
}

func (node *AlterAccount) HandleType() HandleType {
	return InFrontend
}

func (node *AlterAccount) ResultType() ResultType {
	return Status
}

func (node *AlterDataBaseConfig) HandleType() HandleType {
	return InFrontend
}

func (node *AlterDataBaseConfig) ResultType() ResultType {
	return Status
}

func (node *CreateUser) HandleType() HandleType {
	return InFrontend
}

func (node *CreateUser) ResultType() ResultType {
	return Status
}

func (node *DropUser) HandleType() HandleType {
	return InFrontend
}

func (node *DropUser) ResultType() ResultType {
	return Status
}

func (node *AlterUser) HandleType() HandleType {
	return InFrontend
}

func (node *AlterUser) ResultType() ResultType {
	return Status
}

func (node *CreateRole) HandleType() HandleType {
	return InFrontend
}

func (node *CreateRole) ResultType() ResultType {
	return Status
}

func (node *DropRole) HandleType() HandleType {
	return InFrontend
}

func (node *DropRole) ResultType() ResultType {
	return Status
}

func (node *CreateFunction) HandleType() HandleType {
	return InFrontend
}

func (node *CreateFunction) ResultType() ResultType {
	return Status
}

func (node *DropFunction) HandleType() HandleType {
	return InFrontend
}

func (node *DropFunction) ResultType() ResultType {
	return Status
}

func (node *CreateProcedure) HandleType() HandleType {
	return InFrontend
}

func (node *CreateProcedure) ResultType() ResultType {
	return Status
}

func (node *DropProcedure) HandleType() HandleType {
	return InFrontend
}

func (node *DropProcedure) ResultType() ResultType {
	return Status
}

func (node *CallStmt) HandleType() HandleType {
	return InFrontend
}

func (node *CallStmt) ResultType() ResultType {
	return Undefined
}

func (node *Grant) HandleType() HandleType {
	return InFrontend
}

func (node *Grant) ResultType() ResultType {
	return Status
}

func (node *Revoke) HandleType() HandleType {
	return InFrontend
}

func (node *Revoke) ResultType() ResultType {
	return Status
}

func (node *Kill) HandleType() HandleType {
	return InFrontend
}

func (node *Kill) ResultType() ResultType {
	return Status
}

func (node *ShowAccounts) HandleType() HandleType {
	return InFrontend
}

func (node *ShowAccounts) ResultType() ResultType {
	return RowSet
}

func (node *ShowBackendServers) HandleType() HandleType {
	return InFrontend
}

func (node *ShowBackendServers) ResultType() ResultType {
	return RowSet
}

func (node *SetTransaction) HandleType() HandleType {
	return InFrontend
}

func (node *SetTransaction) ResultType() ResultType {
	return Status
}

func (node *LockTableStmt) HandleType() HandleType {
	return InFrontend
}

func (node *LockTableStmt) ResultType() ResultType {
	return Status
}

func (node *UnLockTableStmt) HandleType() HandleType {
	return InFrontend
}

func (node *UnLockTableStmt) ResultType() ResultType {
	return Status
}

func (node *BackupStart) HandleType() HandleType {
	return InFrontend
}

func (node *BackupStart) ResultType() ResultType {
	return Status
}

func (node *EmptyStmt) HandleType() HandleType {
	return InFrontend
}

func (node *EmptyStmt) ResultType() ResultType {
	return Status
}

func (node *prepareImpl) HandleType() HandleType {
	return InFrontend
}

func (node *prepareImpl) ResultType() ResultType {
	return Status
}

func (node *Execute) HandleType() HandleType {
	return InBackend
}

func (node *Execute) ResultType() ResultType {
	return Undefined
}

func (node *Deallocate) HandleType() HandleType {
	return InFrontend
}

func (node *Deallocate) ResultType() ResultType {
	return NoResp
}

func (node *Update) HandleType() HandleType {
	return InBackend
}

func (node *Update) ResultType() ResultType {
	return Status
}

func (node *CreateDatabase) HandleType() HandleType {
	return InBackend
}

func (node *CreateDatabase) ResultType() ResultType {
	return Status
}

func (node *CreateTable) HandleType() HandleType {
	return InBackend
}

func (node *CreateTable) ResultType() ResultType {
	return Status
}

func (node *CreateView) HandleType() HandleType {
	return InBackend
}

func (node *CreateView) ResultType() ResultType {
	return Status
}

func (node *ShowDatabases) HandleType() HandleType {
	return InBackend
}

func (node *ShowDatabases) ResultType() ResultType {
	return RowSet
}

func (node *ShowTables) HandleType() HandleType {
	return InBackend
}

func (node *ShowTables) ResultType() ResultType {
	return RowSet
}

func (node *ShowCreateTable) HandleType() HandleType {
	return InBackend
}

func (node *ShowCreateTable) ResultType() ResultType {
	return RowSet
}

func (node *Insert) HandleType() HandleType {
	return InBackend
}

func (node *Insert) ResultType() ResultType {
	return Status
}

func (node *ShowVariables) HandleType() HandleType {
	return InFrontend
}

func (node *ShowVariables) ResultType() ResultType {
	return RowSet
}

func (node *ShowIndex) HandleType() HandleType {
	return InBackend
}

func (node *ShowIndex) ResultType() ResultType {
	return RowSet
}

func (node *ShowTarget) HandleType() HandleType {
	return InBackend
}

func (node *ShowTarget) ResultType() ResultType {
	return RowSet
}

func (node *ShowCollation) HandleType() HandleType {
	return InBackend
}

func (node *ShowCollation) ResultType() ResultType {
	return RowSet
}

func (node *ShowFunctionOrProcedureStatus) HandleType() HandleType {
	return InBackend
}

func (node *ShowFunctionOrProcedureStatus) ResultType() ResultType {
	return RowSet
}

func (node *ShowGrants) HandleType() HandleType {
	return InBackend
}

func (node *ShowGrants) ResultType() ResultType {
	return RowSet
}

func (node *ShowTableStatus) HandleType() HandleType {
	return InBackend
}

func (node *ShowTableStatus) ResultType() ResultType {
	return RowSet
}

func (node *ExplainStmt) HandleType() HandleType {
	return InFrontend
}

func (node *ExplainStmt) ResultType() ResultType {
	return RowSet
}

func (node *ExplainAnalyze) HandleType() HandleType {
	return InBackend
}

func (node *ExplainAnalyze) ResultType() ResultType {
	return RowSet
}

func (node *ExplainFor) HandleType() HandleType {
	return InBackend
}

func (node *ExplainFor) ResultType() ResultType {
	return RowSet
}

func (node *ShowColumns) HandleType() HandleType {
	return InBackend
}

func (node *ShowColumns) ResultType() ResultType {
	return RowSet
}

func (node *ShowStatus) HandleType() HandleType {
	return InBackend
}

func (node *ShowStatus) ResultType() ResultType {
	return RowSet
}

func (node *ShowWarnings) HandleType() HandleType {
	return InFrontend
}

func (node *ShowWarnings) ResultType() ResultType {
	return RowSet
}

func (node *ShowErrors) HandleType() HandleType {
	return InFrontend
}

func (node *ShowErrors) ResultType() ResultType {
	return RowSet
}

func (node *ShowProcessList) HandleType() HandleType {
	return InBackend
}

func (node *ShowProcessList) ResultType() ResultType {
	return RowSet
}

func (node *ShowCreateDatabase) HandleType() HandleType {
	return InBackend
}

func (node *ShowCreateDatabase) ResultType() ResultType {
	return RowSet
}

func (node *ShowStages) HandleType() HandleType {
	return InBackend
}

func (node *ShowStages) ResultType() ResultType {
	return RowSet
}

func (node *ShowCreatePublications) HandleType() HandleType {
	return InBackend
}

func (node *ShowCreatePublications) ResultType() ResultType {
	return RowSet
}

func (node *ShowPublications) HandleType() HandleType {
	return InBackend
}

func (node *ShowPublications) ResultType() ResultType {
	return RowSet
}

func (node *ShowTableSize) HandleType() HandleType {
	return InBackend
}

func (node *ShowTableSize) ResultType() ResultType {
	return RowSet
}

func (node *ShowRolesStmt) HandleType() HandleType {
	return InBackend
}

func (node *ShowRolesStmt) ResultType() ResultType {
	return RowSet
}

func (node *ShowConnectors) HandleType() HandleType {
	return InFrontend
}

func (node *ShowConnectors) ResultType() ResultType {
	return RowSet
}

func (node *AlterTable) HandleType() HandleType {
	return InBackend
}

func (node *AlterTable) ResultType() ResultType {
	return Status
}

func (node *CreateConnector) HandleType() HandleType {
	return InFrontend
}

func (node *CreateConnector) ResultType() ResultType {
	return Status
}

func (node *CreateSource) HandleType() HandleType {
	return InBackend
}

func (node *CreateSource) ResultType() ResultType {
	return Status
}

func (node *DropTable) HandleType() HandleType {
	return InBackend
}

func (node *DropTable) ResultType() ResultType {
	return Status
}

func (node *Load) HandleType() HandleType {
	return InBackend
}

func (node *Load) ResultType() ResultType {
	return Status
}

func (node *AlterView) HandleType() HandleType {
	return InBackend
}

func (node *AlterView) ResultType() ResultType {
	return Status
}

func (node *TruncateTable) HandleType() HandleType {
	return InBackend
}

func (node *TruncateTable) ResultType() ResultType {
	return Status
}

func (node *Delete) HandleType() HandleType {
	return InBackend
}

func (node *Delete) ResultType() ResultType {
	return Status
}

func (node *SetVar) HandleType() HandleType {
	return InFrontend
}

func (node *SetVar) ResultType() ResultType {
	return Status
}

func (node *Replace) HandleType() HandleType {
	return InBackend
}

func (node *Replace) ResultType() ResultType {
	return Status
}

func (node *CreateIndex) HandleType() HandleType {
	return InBackend
}

func (node *CreateIndex) ResultType() ResultType {
	return Status
}

func (node *DropDatabase) HandleType() HandleType {
	return InBackend
}

func (node *DropDatabase) ResultType() ResultType {
	return Status
}

func (node *SetDefaultRole) HandleType() HandleType {
	return InFrontend
}

func (node *SetDefaultRole) ResultType() ResultType {
	return Status
}

func (node *SetPassword) HandleType() HandleType {
	return Unknown
}

func (node *SetPassword) ResultType() ResultType {
	return Status
}

func (node *DropIndex) HandleType() HandleType {
	return InBackend
}

func (node *DropIndex) ResultType() ResultType {
	return Status
}

func (node *AnalyzeStmt) HandleType() HandleType {
	return InFrontend
}

func (node *AnalyzeStmt) ResultType() ResultType {
	return RowSet
}

func (node *SetRole) HandleType() HandleType {
	return InFrontend
}

func (node *SetRole) ResultType() ResultType {
	return Status
}

func (node *Do) HandleType() HandleType {
	return Unknown
}

func (node *Do) ResultType() ResultType {
	return Undefined
}

func (node *Declare) HandleType() HandleType {
	return Unknown
}

func (node *Declare) ResultType() ResultType {
	return Undefined
}

func (node *CreateExtension) HandleType() HandleType {
	return Unknown
}

func (node *CreateExtension) ResultType() ResultType {
	return Undefined
}

func (node *LoadExtension) HandleType() HandleType {
	return Unknown
}

func (node *LoadExtension) ResultType() ResultType {
	return Undefined
}

func (node *ValuesStatement) HandleType() HandleType {
	return InBackend
}

func (node *ValuesStatement) ResultType() ResultType {
	return RowSet
}

func (node *MoDump) HandleType() HandleType {
	return InFrontend
}

func (node *MoDump) ResultType() ResultType {
	return Status
}

func (node *CreateSequence) HandleType() HandleType {
	return InBackend
}

func (node *CreateSequence) ResultType() ResultType {
	return Status
}

func (node *AlterSequence) HandleType() HandleType {
	return InBackend
}

func (node *AlterSequence) ResultType() ResultType {
	return Status
}

func (node *Reset) HandleType() HandleType {
	return InFrontend
}

func (node *Reset) ResultType() ResultType {
	return Status
}

func (node *DropConnector) HandleType() HandleType {
	return InFrontend
}

func (node *DropConnector) ResultType() ResultType {
	return Status
}

func (node *ResumeDaemonTask) HandleType() HandleType {
	return InFrontend
}

func (node *ResumeDaemonTask) ResultType() ResultType {
	return Status
}

func (node *CancelDaemonTask) HandleType() HandleType {
	return InFrontend
}

func (node *CancelDaemonTask) ResultType() ResultType {
	return Status
}

func (node *PauseDaemonTask) HandleType() HandleType {
	return InFrontend
}

func (node *PauseDaemonTask) ResultType() ResultType {
	return Status
}

func (node *PrepareString) HandleType() HandleType {
	return InFrontend
}

func (node *PrepareString) ResultType() ResultType {
	return Status
}

func (node *PrepareStmt) HandleType() HandleType {
	return InFrontend
}

func (node *PrepareStmt) ResultType() ResultType {
	return Status
}

func (node *DropView) HandleType() HandleType {
	return InBackend
}

func (node *DropView) ResultType() ResultType {
	return Status
}