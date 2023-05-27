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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"strings"
)

const (
	getAllAccountInfoFormat = "select " +
		"account_id as `account_id`, " +
		"account_name as `account_name`, " +
		"created_time as `created`, " +
		"status as `status`, " +
		"suspended_time as `suspended_time`, " +
		"comments as `comment` " +
		"from " +
		"mo_catalog.mo_account " +
		"%s" +
		";"

	getAccountInfoFormat = "select " +
		"account_id as `account_id`, " +
		"account_name as `account_name`, " +
		"created_time as `created`, " +
		"status as `status`, " +
		"suspended_time as `suspended_time`, " +
		"comments as `comment` " +
		"from " +
		"mo_catalog.mo_account " +
		"where account_id = %d;"

	// column index in the result set generated by
	// the sql getAllAccountInfoFormat, getAccountInfoFormat
	idxOfAccountId     = 0
	idxOfAccountName   = 1
	idxOfCreated       = 2
	idxOfStatus        = 3
	idxOfSuspendedTime = 4
	idxOfComment       = 5

	getTableStatsFormat = "select " +
		"( select " +
		"        mu2.user_name as `admin_name` " +
		"  from mo_catalog.mo_user as mu2 join " +
		"      ( select " +
		"              min(user_id) as `min_user_id` " +
		"        from mo_catalog.mo_user " +
		"      ) as mu1 on mu2.user_id = mu1.min_user_id " +
		") as `admin_name`, " +
		"count(distinct mt.reldatabase) as `db_count`, " +
		"count(distinct mt.relname) as `table_count`, " +
		"sum(mo_table_rows(mt.reldatabase,mt.relname)) as `row_count`, " +
		"cast(sum(mo_table_size(mt.reldatabase,mt.relname))/1048576  as decimal(29,3)) as `size` " +
		"from " +
		"mo_catalog.mo_tables as mt " +
		"where mt.account_id = %d;"

	// column index in the result set generated by
	// the sql getTableStatsFormat
	idxOfAdminName  = 0
	idxOfDBCount    = 1
	idxOfTableCount = 2
	idxOfRowCount   = 3
	idxOfSize       = 4

	// column index in the result set of the statement show accounts
	finalIdxOfAccountName   = 0
	finalIdxOfAdminName     = 1
	finalIdxOfCreated       = 2
	finalIdxOfStatus        = 3
	finalIdxOfSuspendedTime = 4
	finalIdxOfDBCount       = 5
	finalIdxOfTableCount    = 6
	finalIdxOfRowCount      = 7
	finalIdxOfSize          = 8
	finalIdxOfComment       = 9
	finalColumnCount        = 10
)

func getSqlForAllAccountInfo(like *tree.ComparisonExpr) string {
	var likePattern = ""
	if like != nil {
		likePattern = strings.TrimSpace(like.Right.String())
	}
	likeClause := ""
	if len(likePattern) != 0 {
		likeClause = fmt.Sprintf("where account_name like '%s'", likePattern)
	}
	return fmt.Sprintf(getAllAccountInfoFormat, likeClause)
}

func getSqlForAccountInfo(accountId uint64) string {
	return fmt.Sprintf(getAccountInfoFormat, accountId)
}

func getSqlForTableStats(accountId int32) string {
	return fmt.Sprintf(getTableStatsFormat, accountId)
}

func doShowAccounts(ctx context.Context, ses *Session, sa *tree.ShowAccounts) (err error){
	var sql string
	var accountIds [][]int32
	var allAccountInfo []*batch.Batch
	var eachAccountInfo []*batch.Batch
	var tempBatch *batch.Batch
	var MoAccountColumns, EachAccountColumns *plan.ResultColDef
	var outputBatches []*batch.Batch
	mp := ses.GetMemPool()

	defer func() {
		for _, b := range allAccountInfo {
			if b == nil {
				continue
			}
			b.Clean(mp)
		}
		for _, b := range outputBatches {
			if b == nil {
				continue
			}
			b.Clean(mp)
		}
		for _, b := range eachAccountInfo {
			if b == nil {
				continue
			}
			b.Clean(mp)
		}
		if tempBatch != nil {
			tempBatch.Clean(mp)
		}
	}()

	bh := ses.GetBackgroundHandlerWithBatchFetcher(ctx)
	defer bh.Close()

	account := ses.GetTenantInfo()

	err = bh.Exec(ctx, "begin;")
	defer func() {
		err = finishTxn(ctx, bh, err)
	}()

	if err != nil {
		return err
	}

	//step1: current account is sys or non-sys ?
	//the result of the statement show accounts is different
	//under the sys or non-sys.

	//step2:
	if account.IsSysTenant() {
		//under sys account
		//step2.1: get all account info from mo_account;

		sql = getSqlForAllAccountInfo(sa.Like)
		allAccountInfo, accountIds, err = getAccountInfo(ctx, bh, sql, true)
		if err != nil {
			return err
		}
		rsOfMoAccount := bh.ses.GetAllMysqlResultSet()[0]
		MoAccountColumns = bh.ses.rs
		bh.ClearExecResultSet()

		//step2.2: for all accounts, switch into an account,
		//get the admin_name, table size and table rows.
		//then merge into the final result batch
		outputBatches = make([]*batch.Batch, len(allAccountInfo))
		for i, ids := range accountIds {
			for _, id := range ids {
				newCtx := context.WithValue(ctx, defines.TenantIDKey{}, uint32(id))
				tempBatch, err = getTableStats(newCtx, bh, id)
				if err != nil {
					return err
				}
				eachAccountInfo = append(eachAccountInfo, tempBatch)
			}

			// merge result set from mo_account and table stats from each account
			outputBatches[i] = batch.NewWithSize(finalColumnCount)
			err = mergeOutputResult(ses, outputBatches[i], allAccountInfo[i], eachAccountInfo)
			if err != nil {
				return err
			}

			for _, b := range eachAccountInfo {
				b.Clean(mp)
			}
			eachAccountInfo = nil
		}
		rsOfEachAccount := bh.ses.GetAllMysqlResultSet()[0]
		EachAccountColumns = bh.ses.rs
		bh.ClearExecResultSet()

		//step3: generate mysql result set
		outputRS := &MysqlResultSet{}
		err = initOutputRs(outputRS, rsOfMoAccount, rsOfEachAccount, ctx)
		if err != nil {
			return err
		}
		oq := newFakeOutputQueue(outputRS)
		for _, b := range outputBatches {
			err = fillResultSet(oq, b, ses)
			if err != nil {
				return err
			}
		}
		ses.SetMysqlResultSet(outputRS)
	} else {
		if sa.Like != nil {
			return moerr.NewInternalError(ctx, "only sys account can use LIKE clause")
		}
		//under non-sys account
		//step2.1: switch into the sys account, get the account info
		newCtx := context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))
		sql = getSqlForAccountInfo(uint64(account.GetTenantID()))
		allAccountInfo, _, err = getAccountInfo(newCtx, bh, sql, false)
		if err != nil {
			return err
		}
		if len(allAccountInfo) != 1 {
			return moerr.NewInternalError(ctx, "no such account %v", account.TenantID)
		}
		rsOfMoAccount := bh.ses.GetAllMysqlResultSet()[0]
		MoAccountColumns = bh.ses.rs
		bh.ClearExecResultSet()

		//step2.2: get the admin_name, table size and table rows.
		//then merge into the final result batch
		tempBatch, err = getTableStats(ctx, bh, int32(account.GetTenantID()))
		if err != nil {
			return err
		}
		rsOfEachAccount := bh.ses.GetAllMysqlResultSet()[0]
		EachAccountColumns = bh.ses.rs
		bh.ClearExecResultSet()

		outputBatches = []*batch.Batch{batch.NewWithSize(finalColumnCount)}
		err = mergeOutputResult(ses, outputBatches[0], allAccountInfo[0], []*batch.Batch{tempBatch})
		if err != nil {
			return err
		}

		//step3: generate mysql result set
		outputRS := &MysqlResultSet{}
		err = initOutputRs(outputRS, rsOfMoAccount, rsOfEachAccount, ctx)
		if err != nil {
			return err
		}
		oq := newFakeOutputQueue(outputRS)
		err = fillResultSet(oq, outputBatches[0], ses)
		if err != nil {
			return err
		}
		ses.SetMysqlResultSet(outputRS)
	}

	ses.rs = mergeRsColumns(MoAccountColumns, EachAccountColumns)
	if openSaveQueryResult(ses) {
		err = saveResult(ses, outputBatches)
	}

	return err
}

func mergeRsColumns(rsOfMoAccountColumns *plan.ResultColDef, rsOfEachAccountColumns *plan.ResultColDef) *plan.ResultColDef {
	def := &plan.ResultColDef{
		ResultCols: make([]*plan.ColDef, finalColumnCount),
	}
	def.ResultCols[finalIdxOfAccountName] = rsOfMoAccountColumns.ResultCols[idxOfAccountName]
	def.ResultCols[finalIdxOfAdminName] = rsOfEachAccountColumns.ResultCols[idxOfAdminName]
	def.ResultCols[finalIdxOfCreated] = rsOfMoAccountColumns.ResultCols[idxOfCreated]
	def.ResultCols[finalIdxOfStatus] = rsOfMoAccountColumns.ResultCols[idxOfStatus]
	def.ResultCols[finalIdxOfSuspendedTime] = rsOfMoAccountColumns.ResultCols[idxOfSuspendedTime]
	def.ResultCols[finalIdxOfDBCount] = rsOfEachAccountColumns.ResultCols[idxOfDBCount]
	def.ResultCols[finalIdxOfTableCount] = rsOfEachAccountColumns.ResultCols[idxOfTableCount]
	def.ResultCols[finalIdxOfRowCount] = rsOfEachAccountColumns.ResultCols[idxOfRowCount]
	def.ResultCols[finalIdxOfSize] = rsOfEachAccountColumns.ResultCols[idxOfSize]
	def.ResultCols[finalIdxOfComment] = rsOfMoAccountColumns.ResultCols[idxOfComment]
	return def
}

func saveResult(ses *Session, outputBatch []*batch.Batch) error {
	for _, b := range outputBatch {
		if err := saveQueryResult(ses, b); err != nil {
			return err
		}
	}
	if err := saveQueryResultMeta(ses); err != nil {
		return err
	}
	return nil
}

func initOutputRs(rs *MysqlResultSet, rsOfMoAccount *MysqlResultSet, rsOfEachAccount *MysqlResultSet, ctx context.Context) error {
	outputColumns := make([]Column, finalColumnCount)
	var err error
	outputColumns[finalIdxOfAccountName], err = rsOfMoAccount.GetColumn(ctx, idxOfAccountName)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfAdminName], err = rsOfEachAccount.GetColumn(ctx, idxOfAdminName)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfCreated], err = rsOfMoAccount.GetColumn(ctx, idxOfCreated)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfStatus], err = rsOfMoAccount.GetColumn(ctx, idxOfStatus)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfSuspendedTime], err = rsOfMoAccount.GetColumn(ctx, idxOfSuspendedTime)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfDBCount], err = rsOfEachAccount.GetColumn(ctx, idxOfDBCount)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfTableCount], err = rsOfEachAccount.GetColumn(ctx, idxOfTableCount)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfRowCount], err = rsOfEachAccount.GetColumn(ctx, idxOfRowCount)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfSize], err = rsOfEachAccount.GetColumn(ctx, idxOfSize)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfComment], err = rsOfMoAccount.GetColumn(ctx, idxOfComment)
	if err != nil {
		return err
	}
	for _, o := range outputColumns {
		rs.AddColumn(o)
	}
	return nil
}

// getAccountInfo gets account info from mo_account under sys account
func getAccountInfo(ctx context.Context,
	bh *BackgroundHandler,
	sql string,
	returnAccountIds bool) ([]*batch.Batch, [][]int32, error) {
	var err error
	var batchIndex2AccounsIds [][]int32
	var rsOfMoAccount []*batch.Batch

	bh.ClearExecResultBatches()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, nil, err
	}

	rsOfMoAccount = bh.GetExecResultBatches()
	if len(rsOfMoAccount) == 0 {
		return nil, nil, moerr.NewInternalError(ctx, "get data from mo_account failed")
	}
	if returnAccountIds {
		batchCount := len(rsOfMoAccount)
		batchIndex2AccounsIds = make([][]int32, batchCount)
		for i := 0; i < batchCount; i++ {
			vecLen := rsOfMoAccount[i].Vecs[0].Length()
			for row := 0; row < vecLen; row++ {
				batchIndex2AccounsIds[i] = append(batchIndex2AccounsIds[i], vector.GetFixedAt[int32](rsOfMoAccount[i].Vecs[0], row))
			}
		}
	}
	return rsOfMoAccount, batchIndex2AccounsIds, err
}

// getTableStats gets the table statistics for the account
func getTableStats(ctx context.Context, bh *BackgroundHandler, accountId int32) (*batch.Batch, error) {
	var sql string
	var err error
	var rs []*batch.Batch
	sql = getSqlForTableStats(accountId)
	bh.ClearExecResultBatches()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}
	rs = bh.GetExecResultBatches()
	if len(rs) != 1 {
		return nil, moerr.NewInternalError(ctx, "get table stats failed")
	}
	return rs[0], err
}

// mergeOutputResult merges the result set from mo_account and the table status
// into the final output format
func mergeOutputResult(ses *Session, outputBatch *batch.Batch, rsOfMoAccount *batch.Batch, rsOfEachAccount []*batch.Batch) error {
	outputBatch.Vecs[finalIdxOfAccountName] = rsOfMoAccount.Vecs[idxOfAccountName]
	outputBatch.Vecs[finalIdxOfAdminName] = vector.NewVec(*rsOfEachAccount[0].Vecs[idxOfAdminName].GetType())
	outputBatch.Vecs[finalIdxOfCreated] = rsOfMoAccount.Vecs[idxOfCreated]
	outputBatch.Vecs[finalIdxOfStatus] = rsOfMoAccount.Vecs[idxOfStatus]
	outputBatch.Vecs[finalIdxOfSuspendedTime] = rsOfMoAccount.Vecs[idxOfSuspendedTime]
	outputBatch.Vecs[finalIdxOfDBCount] = vector.NewVec(*rsOfEachAccount[0].Vecs[idxOfDBCount].GetType())
	outputBatch.Vecs[finalIdxOfTableCount] = vector.NewVec(*rsOfEachAccount[0].Vecs[idxOfTableCount].GetType())
	outputBatch.Vecs[finalIdxOfRowCount] = vector.NewVec(*rsOfEachAccount[0].Vecs[idxOfRowCount].GetType())
	outputBatch.Vecs[finalIdxOfSize] = vector.NewVec(*rsOfEachAccount[0].Vecs[idxOfSize].GetType())
	outputBatch.Vecs[finalIdxOfComment] = rsOfMoAccount.Vecs[idxOfComment]

	var err error
	mp := ses.GetMemPool()
	for _, bat := range rsOfEachAccount {
		err = outputBatch.Vecs[finalIdxOfAdminName].UnionOne(bat.Vecs[idxOfAdminName], 0, mp)
		if err != nil {
			return err
		}
		err = outputBatch.Vecs[finalIdxOfDBCount].UnionOne(bat.Vecs[idxOfDBCount], 0, mp)
		if err != nil {
			return err
		}
		err = outputBatch.Vecs[finalIdxOfTableCount].UnionOne(bat.Vecs[idxOfTableCount], 0, mp)
		if err != nil {
			return err
		}
		err = outputBatch.Vecs[finalIdxOfRowCount].UnionOne(bat.Vecs[idxOfRowCount], 0, mp)
		if err != nil {
			return err
		}
		err = outputBatch.Vecs[finalIdxOfSize].UnionOne(bat.Vecs[idxOfSize], 0, mp)
		if err != nil {
			return err
		}
	}
	outputBatch.Zs = make([]int64, len(rsOfMoAccount.Zs))
	copy(outputBatch.Zs, rsOfMoAccount.Zs)
	return nil
}
