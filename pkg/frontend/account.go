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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// handleCreateAccount creates a new user-level tenant in the context of the tenant SYS
// which has been initialized.
func handleCreateAccount(ctx context.Context, ses TempInter, ca *tree.CreateAccount) error {
	//step1 : create new account.
	return InitGeneralTenant(ctx, ses.(*Session), ca)
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
