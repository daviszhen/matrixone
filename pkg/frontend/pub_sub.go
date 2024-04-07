// Copyright 2024 Matrix Origin
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
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

func handleCreatePublication(ctx context.Context, ses TempInter, cp *tree.CreatePublication) error {
	return doCreatePublication(ctx, ses.(*Session), cp)
}

func handleAlterPublication(ctx context.Context, ses TempInter, ap *tree.AlterPublication) error {
	return doAlterPublication(ctx, ses.(*Session), ap)
}

func handleDropPublication(ctx context.Context, ses TempInter, dp *tree.DropPublication) error {
	return doDropPublication(ctx, ses.(*Session), dp)
}

func handleCreateStage(ctx context.Context, ses TempInter, cs *tree.CreateStage) error {
	return doCreateStage(ctx, ses.(*Session), cs)
}

func handleAlterStage(ctx context.Context, ses TempInter, as *tree.AlterStage) error {
	return doAlterStage(ctx, ses.(*Session), as)
}

func handleDropStage(ctx context.Context, ses TempInter, ds *tree.DropStage) error {
	return doDropStage(ctx, ses.(*Session), ds)
}

func doCreatePublication(ctx context.Context, ses *Session, cp *tree.CreatePublication) (err error) {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	const allTable = true
	var (
		sql         string
		erArray     []ExecResult
		datId       uint64
		datType     string
		tableList   string
		accountList string
		tenantInfo  *TenantInfo
	)

	tenantInfo = ses.GetTenantInfo()

	if !tenantInfo.IsAdminRole() {
		return moerr.NewInternalError(ctx, "only admin can create publication")
	}

	if cp.AccountsSet == nil || cp.AccountsSet.All {
		accountList = "all"
	} else {
		accts := make([]string, 0, len(cp.AccountsSet.SetAccounts))
		for _, acct := range cp.AccountsSet.SetAccounts {
			accName := string(acct)
			if accountNameIsInvalid(accName) {
				return moerr.NewInternalError(ctx, "invalid account name '%s'", accName)
			}
			accts = append(accts, accName)
		}
		sort.Strings(accts)
		accountList = strings.Join(accts, ",")
	}

	pubDb := string(cp.Database)

	if _, ok := sysDatabases[pubDb]; ok {
		return moerr.NewInternalError(ctx, "invalid database name '%s', not support publishing system database", pubDb)
	}

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}
	bh.ClearExecResultSet()

	sql, err = getSqlForGetDbIdAndType(ctx, pubDb, true, uint64(tenantInfo.TenantID))
	if err != nil {
		return err
	}
	err = bh.Exec(ctx, sql)
	if err != nil {
		return err
	}
	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return err
	}
	if !execResultArrayHasData(erArray) {
		return moerr.NewInternalError(ctx, "database '%s' does not exist", cp.Database)
	}
	datId, err = erArray[0].GetUint64(ctx, 0, 0)
	if err != nil {
		return err
	}
	datType, err = erArray[0].GetString(ctx, 0, 1)
	if err != nil {
		return err
	}
	if datType != "" { //TODO: check the dat_type
		return moerr.NewInternalError(ctx, "database '%s' is not a user database", cp.Database)
	}
	bh.ClearExecResultSet()
	sql, err = getSqlForInsertIntoMoPubs(ctx, string(cp.Name), pubDb, datId, allTable, tableList, accountList, tenantInfo.GetDefaultRoleID(), tenantInfo.GetUserID(), cp.Comment, true)
	if err != nil {
		return err
	}
	err = bh.Exec(ctx, sql)
	if err != nil {
		return err
	}
	return err
}

func doAlterPublication(ctx context.Context, ses *Session, ap *tree.AlterPublication) (err error) {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	var (
		allAccount     bool
		accountList    string
		accountListSep []string
		comment        string
		sql            string
		erArray        []ExecResult
		tenantInfo     *TenantInfo
	)

	tenantInfo = ses.GetTenantInfo()

	if !tenantInfo.IsAdminRole() {
		return moerr.NewInternalError(ctx, "only admin can alter publication")
	}

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}
	bh.ClearExecResultSet()
	sql, err = getSqlForGetPubInfo(ctx, string(ap.Name), true)
	if err != nil {
		return err
	}
	err = bh.Exec(ctx, sql)
	if err != nil {
		return err
	}
	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return err
	}
	if !execResultArrayHasData(erArray) {
		return moerr.NewInternalError(ctx, "publication '%s' does not exist", ap.Name)
	}

	accountList, err = erArray[0].GetString(ctx, 0, 0)
	if err != nil {
		return err
	}
	allAccount = accountList == "all"

	comment, err = erArray[0].GetString(ctx, 0, 1)
	if err != nil {
		return err
	}

	if ap.AccountsSet != nil {
		switch {
		case ap.AccountsSet.All:
			accountList = "all"
		case len(ap.AccountsSet.SetAccounts) > 0:
			/* do not check accountName if exists here */
			accts := make([]string, 0, len(ap.AccountsSet.SetAccounts))
			for _, acct := range ap.AccountsSet.SetAccounts {
				s := string(acct)
				if accountNameIsInvalid(s) {
					return moerr.NewInternalError(ctx, "invalid account name '%s'", s)
				}
				accts = append(accts, s)
			}
			sort.Strings(accts)
			accountList = strings.Join(accts, ",")
		case len(ap.AccountsSet.DropAccounts) > 0:
			if allAccount {
				return moerr.NewInternalError(ctx, "cannot drop accounts from all account option")
			}
			accountListSep = strings.Split(accountList, ",")
			for _, acct := range ap.AccountsSet.DropAccounts {
				if accountNameIsInvalid(string(acct)) {
					return moerr.NewInternalError(ctx, "invalid account name '%s'", acct)
				}
				idx := sort.SearchStrings(accountListSep, string(acct))
				if idx < len(accountListSep) && accountListSep[idx] == string(acct) {
					accountListSep = append(accountListSep[:idx], accountListSep[idx+1:]...)
				}
			}
			accountList = strings.Join(accountListSep, ",")
		case len(ap.AccountsSet.AddAccounts) > 0:
			if allAccount {
				return moerr.NewInternalError(ctx, "cannot add account from all account option")
			}
			accountListSep = strings.Split(accountList, ",")
			for _, acct := range ap.AccountsSet.AddAccounts {
				if accountNameIsInvalid(string(acct)) {
					return moerr.NewInternalError(ctx, "invalid account name '%s'", acct)
				}
				idx := sort.SearchStrings(accountListSep, string(acct))
				if idx == len(accountListSep) || accountListSep[idx] != string(acct) {
					accountListSep = append(accountListSep[:idx], append([]string{string(acct)}, accountListSep[idx:]...)...)
				}
			}
			accountList = strings.Join(accountListSep, ",")
		}
	}
	if ap.Comment != "" {
		comment = ap.Comment
	}
	sql, err = getSqlForUpdatePubInfo(ctx, string(ap.Name), accountList, comment, false)
	if err != nil {
		return err
	}
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return err
	}
	return err
}

func doDropPublication(ctx context.Context, ses *Session, dp *tree.DropPublication) (err error) {
	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()
	bh.ClearExecResultSet()
	var (
		sql        string
		erArray    []ExecResult
		tenantInfo *TenantInfo
	)

	tenantInfo = ses.GetTenantInfo()

	if !tenantInfo.IsAdminRole() {
		return moerr.NewInternalError(ctx, "only admin can drop publication")
	}

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}
	sql, err = getSqlForGetPubInfo(ctx, string(dp.Name), true)
	if err != nil {
		return err
	}
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return err
	}
	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return err
	}
	if !execResultArrayHasData(erArray) {
		return moerr.NewInternalError(ctx, "publication '%s' does not exist", dp.Name)
	}

	sql, err = getSqlForDropPubInfo(ctx, string(dp.Name), false)
	if err != nil {
		return err
	}

	err = bh.Exec(ctx, sql)
	if err != nil {
		return err
	}

	return err
}

// create subscription database
func createSubscriptionDatabase(ctx context.Context, bh BackgroundExec, newTenant *TenantInfo, ses *Session) error {
	ctx, span := trace.Debug(ctx, "createSubscriptionDatabase")
	defer span.End()

	var err error
	subscriptions := make([]string, 0)
	//process the syspublications
	_, syspublications_value, _ := ses.GetGlobalSysVars().GetGlobalSysVar("syspublications")
	if syspublications, ok := syspublications_value.(string); ok {
		if len(syspublications) == 0 {
			return err
		}
		subscriptions = strings.Split(syspublications, ",")
	}
	// if no subscriptions, return
	if len(subscriptions) == 0 {
		return err
	}

	//with new tenant
	ctx = defines.AttachAccount(ctx, uint32(newTenant.GetTenantID()), uint32(newTenant.GetUserID()), uint32(newTenant.GetDefaultRoleID()))

	createSubscriptionFormat := `create database %s from sys publication %s;`
	sqls := make([]string, 0, len(subscriptions))
	for _, subscription := range subscriptions {
		sqls = append(sqls, fmt.Sprintf(createSubscriptionFormat, subscription, subscription))
	}
	for _, sql := range sqls {
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}
	}
	return err
}
