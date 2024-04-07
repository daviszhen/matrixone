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
	"os"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/metric/mometric"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace"
)

// InitSysTenant initializes the tenant SYS before any tenants and accepting any requests
// during the system is booting.
func InitSysTenant(ctx context.Context) (err error) {
	var exists bool
	var mp *mpool.MPool
	pu := config.GetParameterUnit(ctx)

	tenant := &TenantInfo{
		Tenant:        sysAccountName,
		User:          rootName,
		DefaultRole:   moAdminRoleName,
		TenantID:      sysAccountID,
		UserID:        rootID,
		DefaultRoleID: moAdminRoleID,
	}

	ctx = defines.AttachAccount(ctx, uint32(sysAccountID), uint32(rootID), uint32(moAdminRoleID))

	mp, err = mpool.NewMPool("init_system_tenant", 0, mpool.NoFixed)
	if err != nil {
		return err
	}
	defer mpool.DeleteMPool(mp)
	//Note: it is special here. The connection ctx here is ctx also.
	//Actually, it is ok here. the ctx is moServerCtx instead of requestCtx
	upstream := &Session{
		connectCtx:   ctx,
		protocol:     &FakeProtocol{},
		seqCurValues: make(map[uint64]string),
		seqLastValue: new(string),
	}
	bh := NewBackgroundExec(ctx, upstream, mp)
	defer bh.Close()

	//USE the mo_catalog
	err = bh.Exec(ctx, "use mo_catalog;")
	if err != nil {
		return err
	}

	err = bh.Exec(ctx, createDbInformationSchemaSql)
	if err != nil {
		return err
	}

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()
	if err != nil {
		return err
	}

	exists, err = checkSysExistsOrNot(ctx, bh, pu)
	if err != nil {
		return err
	}

	if !exists {
		err = createTablesInMoCatalog(ctx, bh, tenant, pu)
		if err != nil {
			return err
		}
	}

	return err
}

// createTablesInMoCatalog creates catalog tables in the database mo_catalog.
func createTablesInMoCatalog(ctx context.Context, bh BackgroundExec, tenant *TenantInfo, pu *config.ParameterUnit) error {
	var err error
	var initMoAccount string
	var initDataSqls []string
	if !tenant.IsSysTenant() {
		return moerr.NewInternalError(ctx, "only sys tenant can execute the function")
	}

	addSqlIntoSet := func(sql string) {
		initDataSqls = append(initDataSqls, sql)
	}

	//create tables for the tenant
	for _, sql := range createSqls {
		addSqlIntoSet(sql)
	}

	//initialize the default data of tables for the tenant
	//step 1: add new tenant entry to the mo_account
	initMoAccount = fmt.Sprintf(initMoAccountFormat, sysAccountID, sysAccountName, sysAccountStatus, types.CurrentTimestamp().String2(time.UTC, 0), sysAccountComments)
	addSqlIntoSet(initMoAccount)

	//step 2:add new role entries to the mo_role

	initMoRole1 := fmt.Sprintf(initMoRoleFormat, moAdminRoleID, moAdminRoleName, rootID, moAdminRoleID, types.CurrentTimestamp().String2(time.UTC, 0), "")
	initMoRole2 := fmt.Sprintf(initMoRoleFormat, publicRoleID, publicRoleName, rootID, moAdminRoleID, types.CurrentTimestamp().String2(time.UTC, 0), "")
	addSqlIntoSet(initMoRole1)
	addSqlIntoSet(initMoRole2)

	//step 3:add new user entry to the mo_user

	defaultPassword := rootPassword
	if d := os.Getenv(defaultPasswordEnv); d != "" {
		defaultPassword = d
	}

	//encryption the password
	encryption := HashPassWord(defaultPassword)

	initMoUser1 := fmt.Sprintf(initMoUserFormat, rootID, rootHost, rootName, encryption, rootStatus, types.CurrentTimestamp().String2(time.UTC, 0), rootExpiredTime, rootLoginType, rootCreatorID, rootOwnerRoleID, rootDefaultRoleID)
	initMoUser2 := fmt.Sprintf(initMoUserFormat, dumpID, dumpHost, dumpName, encryption, dumpStatus, types.CurrentTimestamp().String2(time.UTC, 0), dumpExpiredTime, dumpLoginType, dumpCreatorID, dumpOwnerRoleID, dumpDefaultRoleID)
	addSqlIntoSet(initMoUser1)
	addSqlIntoSet(initMoUser2)

	//step4: add new entries to the mo_role_privs
	//moadmin role
	for _, t := range entriesOfMoAdminForMoRolePrivsFor {
		entry := privilegeEntriesMap[t]
		initMoRolePriv := fmt.Sprintf(initMoRolePrivFormat,
			moAdminRoleID, moAdminRoleName,
			entry.objType, entry.objId,
			entry.privilegeId, entry.privilegeId.String(), entry.privilegeLevel,
			rootID, types.CurrentTimestamp().String2(time.UTC, 0),
			entry.withGrantOption)
		addSqlIntoSet(initMoRolePriv)
	}

	//public role
	for _, t := range entriesOfPublicForMoRolePrivsFor {
		entry := privilegeEntriesMap[t]
		initMoRolePriv := fmt.Sprintf(initMoRolePrivFormat,
			publicRoleID, publicRoleName,
			entry.objType, entry.objId,
			entry.privilegeId, entry.privilegeId.String(), entry.privilegeLevel,
			rootID, types.CurrentTimestamp().String2(time.UTC, 0),
			entry.withGrantOption)
		addSqlIntoSet(initMoRolePriv)
	}

	//step5: add new entries to the mo_user_grant

	initMoUserGrant1 := fmt.Sprintf(initMoUserGrantFormat, moAdminRoleID, rootID, types.CurrentTimestamp().String2(time.UTC, 0), false)
	initMoUserGrant2 := fmt.Sprintf(initMoUserGrantFormat, publicRoleID, rootID, types.CurrentTimestamp().String2(time.UTC, 0), false)
	addSqlIntoSet(initMoUserGrant1)
	addSqlIntoSet(initMoUserGrant2)
	initMoUserGrant4 := fmt.Sprintf(initMoUserGrantFormat, moAdminRoleID, dumpID, types.CurrentTimestamp().String2(time.UTC, 0), false)
	initMoUserGrant5 := fmt.Sprintf(initMoUserGrantFormat, publicRoleID, dumpID, types.CurrentTimestamp().String2(time.UTC, 0), false)
	addSqlIntoSet(initMoUserGrant4)
	addSqlIntoSet(initMoUserGrant5)

	//setp6: add new entries to the mo_mysql_compatibility_mode
	for _, variable := range gSysVarsDefs {
		if _, ok := configInitVariables[variable.Name]; ok {
			addsql := addInitSystemVariablesSql(sysAccountID, sysAccountName, variable.Name)
			if len(addsql) != 0 {
				addSqlIntoSet(addsql)
			}
		} else {
			initMoMysqlCompatibilityMode := fmt.Sprintf(initMoMysqlCompatbilityModeWithoutDataBaseFormat, sysAccountID, sysAccountName, variable.Name, getVariableValue(variable.Default), true)
			addSqlIntoSet(initMoMysqlCompatibilityMode)
		}
	}

	//fill the mo_account, mo_role, mo_user, mo_role_privs, mo_user_grant, mo_mysql_compatibility_mode
	for _, sql := range initDataSqls {
		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}
	}
	return err
}

func checkTenantExistsOrNot(ctx context.Context, bh BackgroundExec, userName string) (bool, error) {
	var sqlForCheckTenant string
	var erArray []ExecResult
	var err error
	ctx, span := trace.Debug(ctx, "checkTenantExistsOrNot")
	defer span.End()
	sqlForCheckTenant, err = getSqlForCheckTenant(ctx, userName)
	if err != nil {
		return false, err
	}
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sqlForCheckTenant)
	if err != nil {
		return false, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return false, err
	}

	if execResultArrayHasData(erArray) {
		return true, nil
	}
	return false, nil
}

// handleCreateAccount creates a new user-level tenant in the context of the tenant SYS
// which has been initialized.
func handleCreateAccount(ctx context.Context, ses TempInter, ca *tree.CreateAccount) error {
	//step1 : create new account.
	return InitGeneralTenant(ctx, ses.(*Session), ca)
}

// InitGeneralTenant initializes the application level tenant
func InitGeneralTenant(ctx context.Context, ses *Session, ca *tree.CreateAccount) (err error) {
	var exists bool
	var newTenant *TenantInfo
	var newTenantCtx context.Context
	var mp *mpool.MPool
	ctx, span := trace.Debug(ctx, "InitGeneralTenant")
	defer span.End()
	tenant := ses.GetTenantInfo()

	if !(tenant.IsSysTenant() && tenant.IsMoAdminRole()) {
		return moerr.NewInternalError(ctx, "tenant %s user %s role %s do not have the privilege to create the new account", tenant.GetTenant(), tenant.GetUser(), tenant.GetDefaultRole())
	}

	//normalize the name
	err = normalizeNameOfAccount(ctx, ca)
	if err != nil {
		return err
	}

	ca.AuthOption.AdminName, err = normalizeName(ctx, ca.AuthOption.AdminName)
	if err != nil {
		return err
	}

	if ca.AuthOption.IdentifiedType.Typ == tree.AccountIdentifiedByPassword {
		if len(ca.AuthOption.IdentifiedType.Str) == 0 {
			return moerr.NewInternalError(ctx, "password is empty string")
		}
	}

	ctx = defines.AttachAccount(ctx, uint32(tenant.GetTenantID()), uint32(tenant.GetUserID()), uint32(tenant.GetDefaultRoleID()))

	_, st := trace.Debug(ctx, "InitGeneralTenant.init_general_tenant")
	mp, err = mpool.NewMPool("init_general_tenant", 0, mpool.NoFixed)
	if err != nil {
		st.End()
		return err
	}
	st.End()
	defer mpool.DeleteMPool(mp)

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	createNewAccount := func() (rtnErr error) {
		rtnErr = bh.Exec(ctx, "begin;")
		defer func() {
			rtnErr = finishTxn(ctx, bh, rtnErr)
		}()
		if rtnErr != nil {
			return rtnErr
		}

		//USE the mo_catalog
		// MOVE into txn, make sure only create ONE txn.
		rtnErr = bh.Exec(ctx, "use mo_catalog;")
		if rtnErr != nil {
			return rtnErr
		}

		// check account exists or not
		exists, rtnErr = checkTenantExistsOrNot(ctx, bh, ca.Name)
		if rtnErr != nil {
			return rtnErr
		}

		if exists {
			if !ca.IfNotExists { //do nothing
				return moerr.NewInternalError(ctx, "the tenant %s exists", ca.Name)
			}
			return rtnErr
		} else {
			newTenant, newTenantCtx, rtnErr = createTablesInMoCatalogOfGeneralTenant(ctx, bh, ca)
			if rtnErr != nil {
				return rtnErr
			}
		}

		// create some tables and databases for new account
		rtnErr = bh.Exec(newTenantCtx, createMoIndexesSql)
		if rtnErr != nil {
			return rtnErr
		}

		rtnErr = bh.Exec(newTenantCtx, createMoTablePartitionsSql)
		if rtnErr != nil {
			return rtnErr
		}

		rtnErr = bh.Exec(newTenantCtx, createAutoTableSql)
		if rtnErr != nil {
			return rtnErr
		}

		rtnErr = bh.Exec(newTenantCtx, createMoForeignKeysSql)
		if rtnErr != nil {
			return rtnErr
		}

		//create createDbSqls
		createDbSqls := []string{
			"create database " + motrace.SystemDBConst + ";",
			"create database " + mometric.MetricDBConst + ";",
			createDbInformationSchemaSql,
			"create database mysql;",
		}
		for _, db := range createDbSqls {
			rtnErr = bh.Exec(newTenantCtx, db)
			if rtnErr != nil {
				return rtnErr
			}
		}

		// create tables for new account
		rtnErr = createTablesInMoCatalogOfGeneralTenant2(bh, ca, newTenantCtx, newTenant)
		if rtnErr != nil {
			return rtnErr
		}
		rtnErr = createTablesInSystemOfGeneralTenant(newTenantCtx, bh, newTenant)
		if rtnErr != nil {
			return rtnErr
		}
		rtnErr = createTablesInInformationSchemaOfGeneralTenant(newTenantCtx, bh, newTenant)
		if rtnErr != nil {
			return rtnErr
		}
		return rtnErr
	}

	err = createNewAccount()
	if err != nil {
		return err
	}

	if !exists {
		//just skip nonexistent pubs
		_ = createSubscriptionDatabase(ctx, bh, newTenant, ses)
	}

	return err
}

// createTablesInMoCatalogOfGeneralTenant creates catalog tables in the database mo_catalog.
func createTablesInMoCatalogOfGeneralTenant(ctx context.Context, bh BackgroundExec, ca *tree.CreateAccount) (*TenantInfo, context.Context, error) {
	var err error
	var initMoAccount string
	var erArray []ExecResult
	var newTenantID int64
	var newUserId int64
	var comment = ""
	var newTenant *TenantInfo
	var newTenantCtx context.Context
	var sql string
	//var configuration string
	//var sql string
	ctx, span := trace.Debug(ctx, "createTablesInMoCatalogOfGeneralTenant")
	defer span.End()

	if nameIsInvalid(ca.Name) {
		return nil, nil, moerr.NewInternalError(ctx, "the account name is invalid")
	}

	if nameIsInvalid(ca.AuthOption.AdminName) {
		return nil, nil, moerr.NewInternalError(ctx, "the admin name is invalid")
	}

	//!!!NOTE : Insert into mo_account with original context.
	// Other operations with a new context with new tenant info
	//step 1: add new tenant entry to the mo_account
	if ca.Comment.Exist {
		comment = ca.Comment.Comment
	}

	//determine the status of the account
	status := sysAccountStatus
	if ca.StatusOption.Exist {
		if ca.StatusOption.Option == tree.AccountStatusSuspend {
			status = tree.AccountStatusSuspend.String()
		}
	}

	initMoAccount = fmt.Sprintf(initMoAccountWithoutIDFormat, ca.Name, status, types.CurrentTimestamp().String2(time.UTC, 0), comment)
	//execute the insert
	err = bh.Exec(ctx, initMoAccount)
	if err != nil {
		return nil, nil, err
	}

	//query the tenant id
	bh.ClearExecResultSet()
	sql, err = getSqlForCheckTenant(ctx, ca.Name)
	if err != nil {
		return nil, nil, err
	}
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, nil, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return nil, nil, err
	}

	if execResultArrayHasData(erArray) {
		newTenantID, err = erArray[0].GetInt64(ctx, 0, 0)
		if err != nil {
			return nil, nil, err
		}
	} else {
		return nil, nil, moerr.NewInternalError(ctx, "get the id of tenant %s failed", ca.Name)
	}

	newUserId = int64(GetAdminUserId())

	newTenant = &TenantInfo{
		Tenant:        ca.Name,
		User:          ca.AuthOption.AdminName,
		DefaultRole:   accountAdminRoleName,
		TenantID:      uint32(newTenantID),
		UserID:        uint32(newUserId),
		DefaultRoleID: accountAdminRoleID,
	}
	//with new tenant
	newTenantCtx = defines.AttachAccount(ctx, uint32(newTenantID), uint32(newUserId), uint32(accountAdminRoleID))
	return newTenant, newTenantCtx, err
}

func createTablesInMoCatalogOfGeneralTenant2(bh BackgroundExec, ca *tree.CreateAccount, newTenantCtx context.Context, newTenant *TenantInfo) error {
	var err error
	var initDataSqls []string
	newTenantCtx, span := trace.Debug(newTenantCtx, "createTablesInMoCatalogOfGeneralTenant2")
	defer span.End()
	//create tables for the tenant
	for _, sql := range createSqls {
		//only the SYS tenant has the table mo_account
		if strings.HasPrefix(sql, "create table mo_account") {
			continue
		}
		err = bh.Exec(newTenantCtx, sql)
		if err != nil {
			return err
		}
	}

	//initialize the default data of tables for the tenant
	addSqlIntoSet := func(sql string) {
		initDataSqls = append(initDataSqls, sql)
	}
	//step 2:add new role entries to the mo_role
	initMoRole1 := fmt.Sprintf(initMoRoleFormat, accountAdminRoleID, accountAdminRoleName, newTenant.GetUserID(), newTenant.GetDefaultRoleID(), types.CurrentTimestamp().String2(time.UTC, 0), "")
	initMoRole2 := fmt.Sprintf(initMoRoleFormat, publicRoleID, publicRoleName, newTenant.GetUserID(), newTenant.GetDefaultRoleID(), types.CurrentTimestamp().String2(time.UTC, 0), "")
	addSqlIntoSet(initMoRole1)
	addSqlIntoSet(initMoRole2)

	//step 3:add new user entry to the mo_user
	if ca.AuthOption.IdentifiedType.Typ != tree.AccountIdentifiedByPassword {
		err = moerr.NewInternalError(newTenantCtx, "only support password verification now")
		return err
	}
	name := ca.AuthOption.AdminName
	password := ca.AuthOption.IdentifiedType.Str
	if len(password) == 0 {
		err = moerr.NewInternalError(newTenantCtx, "password is empty string")
		return err
	}
	//encryption the password
	encryption := HashPassWord(password)
	status := rootStatus
	//TODO: fix the status of user or account
	if ca.StatusOption.Exist {
		if ca.StatusOption.Option == tree.AccountStatusSuspend {
			status = tree.AccountStatusSuspend.String()
		}
	}
	//the first user id in the general tenant
	initMoUser1 := fmt.Sprintf(initMoUserFormat, newTenant.GetUserID(), rootHost, name, encryption, status,
		types.CurrentTimestamp().String2(time.UTC, 0), rootExpiredTime, rootLoginType,
		newTenant.GetUserID(), newTenant.GetDefaultRoleID(), accountAdminRoleID)
	addSqlIntoSet(initMoUser1)

	//step4: add new entries to the mo_role_privs
	//accountadmin role
	for _, t := range entriesOfAccountAdminForMoRolePrivsFor {
		entry := privilegeEntriesMap[t]
		initMoRolePriv := fmt.Sprintf(initMoRolePrivFormat,
			accountAdminRoleID, accountAdminRoleName,
			entry.objType, entry.objId,
			entry.privilegeId, entry.privilegeId.String(), entry.privilegeLevel,
			newTenant.GetUserID(), types.CurrentTimestamp().String2(time.UTC, 0),
			entry.withGrantOption)
		addSqlIntoSet(initMoRolePriv)
	}

	//public role
	for _, t := range entriesOfPublicForMoRolePrivsFor {
		entry := privilegeEntriesMap[t]
		initMoRolePriv := fmt.Sprintf(initMoRolePrivFormat,
			publicRoleID, publicRoleName,
			entry.objType, entry.objId,
			entry.privilegeId, entry.privilegeId.String(), entry.privilegeLevel,
			newTenant.GetUserID(), types.CurrentTimestamp().String2(time.UTC, 0),
			entry.withGrantOption)
		addSqlIntoSet(initMoRolePriv)
	}

	//step5: add new entries to the mo_user_grant
	initMoUserGrant1 := fmt.Sprintf(initMoUserGrantFormat, accountAdminRoleID, newTenant.GetUserID(), types.CurrentTimestamp().String2(time.UTC, 0), true)
	addSqlIntoSet(initMoUserGrant1)
	initMoUserGrant2 := fmt.Sprintf(initMoUserGrantFormat, publicRoleID, newTenant.GetUserID(), types.CurrentTimestamp().String2(time.UTC, 0), true)
	addSqlIntoSet(initMoUserGrant2)

	//setp6: add new entries to the mo_mysql_compatibility_mode
	for _, variable := range gSysVarsDefs {
		if _, ok := configInitVariables[variable.Name]; ok {
			addsql := addInitSystemVariablesSql(int(newTenant.GetTenantID()), newTenant.GetTenant(), variable.Name)
			if len(addsql) != 0 {
				addSqlIntoSet(addsql)
			}
		} else {
			initMoMysqlCompatibilityMode := fmt.Sprintf(initMoMysqlCompatbilityModeWithoutDataBaseFormat, newTenant.GetTenantID(), newTenant.GetTenant(), variable.Name, getVariableValue(variable.Default), true)
			addSqlIntoSet(initMoMysqlCompatibilityMode)
		}
	}

	//fill the mo_role, mo_user, mo_role_privs, mo_user_grant, mo_role_grant
	for _, sql := range initDataSqls {
		bh.ClearExecResultSet()
		err = bh.Exec(newTenantCtx, sql)
		if err != nil {
			return err
		}
	}
	return nil
}

// createTablesInSystemOfGeneralTenant creates the database system and system_metrics as the external tables.
func createTablesInSystemOfGeneralTenant(ctx context.Context, bh BackgroundExec, newTenant *TenantInfo) error {
	ctx, span := trace.Debug(ctx, "createTablesInSystemOfGeneralTenant")
	defer span.End()

	var err error
	sqls := make([]string, 0)
	sqls = append(sqls, "use "+motrace.SystemDBConst+";")
	traceTables := motrace.GetSchemaForAccount(ctx, newTenant.GetTenant())
	sqls = append(sqls, traceTables...)
	sqls = append(sqls, "use "+mometric.MetricDBConst+";")
	metricTables := mometric.GetSchemaForAccount(ctx, newTenant.GetTenant())
	sqls = append(sqls, metricTables...)

	for _, sql := range sqls {
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}
	}
	return err
}

// createTablesInInformationSchemaOfGeneralTenant creates the database information_schema and the views or tables.
func createTablesInInformationSchemaOfGeneralTenant(ctx context.Context, bh BackgroundExec, newTenant *TenantInfo) error {
	ctx, span := trace.Debug(ctx, "createTablesInInformationSchemaOfGeneralTenant")
	defer span.End()
	//with new tenant
	//TODO: when we have the auto_increment column, we need new strategy.

	var err error
	sqls := make([]string, 0, len(sysview.InitInformationSchemaSysTables)+len(sysview.InitMysqlSysTables)+4)

	sqls = append(sqls, "use information_schema;")
	sqls = append(sqls, sysview.InitInformationSchemaSysTables...)
	sqls = append(sqls, "use mysql;")
	sqls = append(sqls, sysview.InitMysqlSysTables...)

	for _, sql := range sqls {
		bh.ClearExecResultSet()
		err = bh.Exec(ctx, sql)
		if err != nil {
			return err
		}
	}
	return err
}

// handleDropAccount drops a new user-level tenant
func handleDropAccount(ctx context.Context, ses TempInter, da *tree.DropAccount) error {
	return doDropAccount(ctx, ses.(*Session), da)
}

// handleDropAccount drops a new user-level tenant
func handleAlterAccount(ctx context.Context, ses TempInter, aa *tree.AlterAccount) error {
	return doAlterAccount(ctx, ses.(*Session), aa)
}

// handleAlterDatabaseConfig alter a database's mysql_compatibility_mode
func handleAlterDataBaseConfig(ctx context.Context, ses TempInter, ad *tree.AlterDataBaseConfig) error {
	return doAlterDatabaseConfig(ctx, ses.(*Session), ad)
}

// handleAlterAccountConfig alter a account's mysql_compatibility_mode
func handleAlterAccountConfig(ctx context.Context, ses TempInter, st *tree.AlterDataBaseConfig) error {
	return doAlterAccountConfig(ctx, ses.(*Session), st)
}

// handleCreateUser creates the user for the tenant
func handleCreateUser(ctx context.Context, ses TempInter, cu *tree.CreateUser) error {
	tenant := ses.GetTenantInfo()

	//step1 : create the user
	return InitUser(ctx, ses.(*Session), tenant, cu)
}

// handleDropUser drops the user for the tenant
func handleDropUser(ctx context.Context, ses TempInter, du *tree.DropUser) error {
	return doDropUser(ctx, ses.(*Session), du)
}

func handleAlterUser(ctx context.Context, ses TempInter, au *tree.AlterUser) error {
	return doAlterUser(ctx, ses.(*Session), au)
}

// handleCreateRole creates the new role
func handleCreateRole(ctx context.Context, ses TempInter, cr *tree.CreateRole) error {
	tenant := ses.GetTenantInfo()

	//step1 : create the role
	return InitRole(ctx, ses.(*Session), tenant, cr)
}

// handleDropRole drops the role
func handleDropRole(ctx context.Context, ses TempInter, dr *tree.DropRole) error {
	return doDropRole(ctx, ses.(*Session), dr)
}

// handleGrantRole grants the role
func handleGrantRole(ctx context.Context, ses TempInter, gr *tree.GrantRole) error {
	return doGrantRole(ctx, ses.(*Session), gr)
}

// handleRevokeRole revokes the role
func handleRevokeRole(ctx context.Context, ses TempInter, rr *tree.RevokeRole) error {
	return doRevokeRole(ctx, ses.(*Session), rr)
}

// handleGrantRole grants the privilege to the role
func handleGrantPrivilege(ctx context.Context, ses TempInter, gp *tree.GrantPrivilege) error {
	return doGrantPrivilege(ctx, ses, gp)
}

// handleRevokePrivilege revokes the privilege from the user or role
func handleRevokePrivilege(ctx context.Context, ses TempInter, rp *tree.RevokePrivilege) error {
	return doRevokePrivilege(ctx, ses, rp)
}

// handleSwitchRole switches the role to another role
func handleSwitchRole(ctx context.Context, ses TempInter, sr *tree.SetRole) error {
	return doSwitchRole(ctx, ses.(*Session), sr)
}
