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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var _ Executor = &SelectExecutor{}

type SelectExecutor struct {
	sel *tree.Select
}

func (se *SelectExecutor) Open(context.Context, ...ExecutorOpt) error {
	return nil
}
func (se *SelectExecutor) Label() Label {
	return CanExecInUncommittedTxnTxn | KeepTxnNotNilAfterExec | KeepTxnNotNilAfterExec | TxnExistsAferExc
}
func (se *SelectExecutor) Next(context.Context) error {
	return nil
}
func (se *SelectExecutor) Close(context.Context) error { return nil }

type ValuesStmtExecutor struct {
	sel *tree.ValuesStatement
}

type ShowCreateTableExecutor struct {
	sct *tree.ShowCreateTable
}

type ShowCreateDatabaseExecutor struct {
	scd *tree.ShowCreateDatabase
}

type ShowTablesExecutor struct {
	st *tree.ShowTables
}

type ShowSequencesExecutor struct {
	ss *tree.ShowSequences
}

type ShowDatabasesExecutor struct {
	sd *tree.ShowDatabases
}

type ShowColumnsExecutor struct {
	sc *tree.ShowColumns
}

type ShowProcessListExecutor struct {
	spl *tree.ShowProcessList
}

type ShowStatusExecutor struct {
	ss *tree.ShowStatus
}

type ShowTableStatusExecutor struct {
	sts *tree.ShowTableStatus
}

type ShowGrantsExecutor struct {
	sg *tree.ShowGrants
}

type ShowIndexExecutor struct {
	si *tree.ShowIndex
}

type ShowCreateViewExecutor struct {
	scv *tree.ShowCreateView
}

type ShowTargetExecutor struct {
	st *tree.ShowTarget
}

type ExplainForExecutor struct {
	ef *tree.ExplainFor
}

type ExplainStmtExecutor struct {
	es *tree.ExplainStmt
}

type ShowVariablesExecutor struct {
	sv *tree.ShowVariables
}

type ShowErrorsExecutor struct {
	se *tree.ShowErrors
}

type ShowWarningsExecutor struct {
	sw *tree.ShowWarnings
}
type AnalyzeStmtExecutor struct {
	as *tree.AnalyzeStmt
}

type ExplainAnalyzeExecutor struct {
	ea *tree.ExplainAnalyze
}

type InternalCmdFieldListExecutor struct {
	icfl *InternalCmdFieldList
}
