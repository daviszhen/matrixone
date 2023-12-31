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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"

	"github.com/matrixorigin/matrixone/pkg/frontend/constant"
)

// CmdExecutor handle the command from the client
type CmdExecutor interface {
	SetSession(*Session)

	GetSession() *Session

	// ExecRequest execute the request and get the response
	ExecRequest(context.Context, *Session, *mysqlPayload) (*Response, error)

	//SetCancelFunc saves a cancel function for active request.
	SetCancelFunc(context.CancelFunc)

	// CancelRequest cancels the active request
	CancelRequest()

	Close()
}

type CmdExecutorImpl struct {
	CmdExecutor
}

// UserInput
// normally, just use the sql.
// for some special statement, like 'set_var', we need to use the stmt.
// if the stmt is not nil, we neglect the sql.
type UserInput struct {
	sql           string
	stmt          tree.Statement
	sqlSourceType []string
}

func (ui *UserInput) getSql() string {
	return ui.sql
}

// getStmt if the stmt is not nil, we neglect the sql.
func (ui *UserInput) getStmt() tree.Statement {
	return ui.stmt
}

func (ui *UserInput) getSqlSourceTypes() []string {
	return ui.sqlSourceType
}

// isInternal return true if the stmt is not nil.
// it means the statement is not from any client.
// currently, we use it to handle the 'set_var' statement.
func (ui *UserInput) isInternal() bool {
	return ui.getStmt() != nil
}

func (ui *UserInput) genSqlSourceType(ses *Session) {
	sql := ui.getSql()
	ui.sqlSourceType = nil
	if ui.getStmt() != nil {
		ui.sqlSourceType = append(ui.sqlSourceType, constant.InternalSql)
		return
	}
	tenant := ses.GetTenantInfo()
	if tenant == nil || strings.HasPrefix(sql, cmdFieldListSql) {
		ui.sqlSourceType = append(ui.sqlSourceType, constant.InternalSql)
		return
	}
	flag, _, _ := isSpecialUser(tenant.User)
	if flag {
		ui.sqlSourceType = append(ui.sqlSourceType, constant.InternalSql)
		return
	}
	for len(sql) > 0 {
		p1 := strings.Index(sql, "/*")
		p2 := strings.Index(sql, "*/")
		if p1 < 0 || p2 < 0 || p2 <= p1+1 {
			ui.sqlSourceType = append(ui.sqlSourceType, constant.ExternSql)
			return
		}
		source := strings.TrimSpace(sql[p1+2 : p2])
		if source == cloudUserTag {
			ui.sqlSourceType = append(ui.sqlSourceType, constant.CloudUserSql)
		} else if source == cloudNoUserTag {
			ui.sqlSourceType = append(ui.sqlSourceType, constant.CloudNoUserSql)
		} else if source == saveResultTag {
			ui.sqlSourceType = append(ui.sqlSourceType, constant.CloudUserSql)
		} else {
			ui.sqlSourceType = append(ui.sqlSourceType, constant.ExternSql)
		}
		sql = sql[p2+2:]
	}
}

func (ui *UserInput) getSqlSourceType(i int) string {
	sqlType := constant.ExternSql
	if i < len(ui.sqlSourceType) {
		sqlType = ui.sqlSourceType[i]
	}
	return sqlType
}

type doComQueryFunc func(context.Context, *UserInput) error
