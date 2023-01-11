package frontend

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
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

func getSqlForTableStats(accountId uint64) string {
	return fmt.Sprintf(getTableStatsFormat, accountId)
}

func doShowAccounts(ctx context.Context, ses *Session, sa *tree.ShowAccounts) error {
	var err error
	var sql string
	var accountIds []uint64
	var rsOfMoAccount *MysqlResultSet
	var rsOfEachAccount []*MysqlResultSet
	var tempRS, outputRS *MysqlResultSet
	outputRS = &MysqlResultSet{}

	bh := ses.GetBackgroundExec(ctx)
	defer bh.Close()

	account := ses.GetTenantInfo()

	err = bh.Exec(ctx, "begin;")
	if err != nil {
		goto handleFailed
	}

	//step1: current account is sys or non-sys ?
	//the result of the statement show accounts is different
	//under the sys or non-sys.

	//step2:
	if account.IsSysTenant() {
		//under sys account
		//step2.1: get all account info from mo_account;

		sql = getSqlForAllAccountInfo(sa.Like)
		rsOfMoAccount, accountIds, err = getAccountInfo(ctx, bh, sql, true)
		if err != nil {
			goto handleFailed
		}

		//step2.2: for all accounts, switch into an account,
		//get the admin_name, table size and table rows.
		for _, id := range accountIds {
			newCtx := context.WithValue(ctx, defines.TenantIDKey{}, uint32(id))
			tempRS, err = getTableStats(newCtx, bh, id)
			if err != nil {
				goto handleFailed
			}
			rsOfEachAccount = append(rsOfEachAccount, tempRS)
		}

		//step3: merge result set from mo_account and table stats from each account
		err = mergeOutputResult(ctx, outputRS, rsOfMoAccount, rsOfEachAccount)
		if err != nil {
			goto handleFailed
		}
		ses.SetMysqlResultSet(outputRS)
	} else {
		if sa.Like != nil {
			err = moerr.NewInternalError(ctx, "only sys account can use LIKE clause")
			goto handleFailed
		}
		//under non-sys account
		//step2.1: switch into the sys account, get the account info
		newCtx := context.WithValue(ctx, defines.TenantIDKey{}, uint32(sysAccountID))
		sql = getSqlForAccountInfo(uint64(account.GetTenantID()))
		rsOfMoAccount, _, err = getAccountInfo(newCtx, bh, sql, false)
		if err != nil {
			goto handleFailed
		}

		//step2.2: get the admin_name, table size and table rows.
		tempRS, err = getTableStats(ctx, bh, uint64(account.GetTenantID()))
		if err != nil {
			goto handleFailed
		}

		err = mergeOutputResult(ctx, outputRS, rsOfMoAccount, []*MysqlResultSet{tempRS})
		if err != nil {
			goto handleFailed
		}
		ses.SetMysqlResultSet(outputRS)
	}

	//step3: make a response packet.
	err = bh.Exec(ctx, "commit;")
	if err != nil {
		goto handleFailed
	}

	return err

handleFailed:
	//ROLLBACK the transaction
	rbErr := bh.Exec(ctx, "rollback;")
	if rbErr != nil {
		return rbErr
	}
	return err
}

// getAccountInfo gets account info from mo_account under sys account
func getAccountInfo(ctx context.Context,
	bh BackgroundExec,
	sql string,
	returnAccountIds bool) (*MysqlResultSet, []uint64, error) {
	var err error
	var accountIds []uint64
	var accountId uint64
	var erArray []ExecResult
	var rsOfMoAccount *MysqlResultSet
	var ok bool

	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, nil, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return nil, nil, err
	}

	if execResultArrayHasData(erArray) {
		if returnAccountIds {
			for i := uint64(0); i < erArray[0].GetRowCount(); i++ {
				//account_id
				accountId, err = erArray[0].GetUint64(ctx, i, 0)
				if err != nil {
					return nil, nil, err
				}

				accountIds = append(accountIds, accountId)
			}
		}

		rsOfMoAccount, ok = erArray[0].(*MysqlResultSet)
		if !ok {
			return nil, nil, moerr.NewInternalError(ctx, "convert result set failed.")
		}
	} else {
		return nil, nil, moerr.NewInternalError(ctx, "get data from mo_account failed")
	}
	return rsOfMoAccount, accountIds, err
}

// getTableStats gets the table statistics for the account
func getTableStats(ctx context.Context, bh BackgroundExec, accountId uint64) (*MysqlResultSet, error) {
	var sql string
	var err error
	var erArray []ExecResult
	var rs *MysqlResultSet
	var ok bool
	sql = getSqlForTableStats(accountId)
	bh.ClearExecResultSet()
	err = bh.Exec(ctx, sql)
	if err != nil {
		return nil, err
	}

	erArray, err = getResultSet(ctx, bh)
	if err != nil {
		return nil, err
	}

	if execResultArrayHasData(erArray) {
		rs, ok = erArray[0].(*MysqlResultSet)
		if !ok {
			err = moerr.NewInternalError(ctx, "convert result set failed")
			return nil, err
		}
	} else {
		err = moerr.NewInternalError(ctx, "get table stats failed")
		return nil, err
	}
	return rs, err
}

// mergeOutputResult merges the result set from mo_account and the table status
// into the final output format
func mergeOutputResult(ctx context.Context, outputRS *MysqlResultSet, rsOfMoAccount *MysqlResultSet, rsOfEachAccount []*MysqlResultSet) error {
	var err error
	outputColumns := make([]Column, finalColumnCount)

	outputColumns[finalIdxOfAccountName], err = rsOfMoAccount.GetColumn(ctx, idxOfAccountName)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfAdminName], err = rsOfEachAccount[0].GetColumn(ctx, idxOfAdminName)
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
	outputColumns[finalIdxOfDBCount], err = rsOfEachAccount[0].GetColumn(ctx, idxOfDBCount)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfTableCount], err = rsOfEachAccount[0].GetColumn(ctx, idxOfTableCount)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfRowCount], err = rsOfEachAccount[0].GetColumn(ctx, idxOfRowCount)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfSize], err = rsOfEachAccount[0].GetColumn(ctx, idxOfSize)
	if err != nil {
		return err
	}
	outputColumns[finalIdxOfComment], err = rsOfMoAccount.GetColumn(ctx, idxOfComment)
	if err != nil {
		return err
	}
	for _, o := range outputColumns {
		outputRS.AddColumn(o)
	}
	for i, rs := range rsOfEachAccount {
		outputRow := make([]interface{}, finalColumnCount)
		outputRow[finalIdxOfAccountName], err = rsOfMoAccount.GetValue(ctx, uint64(i), idxOfAccountName)
		if err != nil {
			return err
		}
		outputRow[finalIdxOfAdminName], err = rs.GetValue(ctx, 0, idxOfAdminName)
		if err != nil {
			return err
		}
		outputRow[finalIdxOfCreated], err = rsOfMoAccount.GetValue(ctx, uint64(i), idxOfCreated)
		if err != nil {
			return err
		}
		outputRow[finalIdxOfStatus], err = rsOfMoAccount.GetValue(ctx, uint64(i), idxOfStatus)
		if err != nil {
			return err
		}
		outputRow[finalIdxOfSuspendedTime], err = rsOfMoAccount.GetValue(ctx, uint64(i), idxOfSuspendedTime)
		if err != nil {
			return err
		}
		outputRow[finalIdxOfDBCount], err = rs.GetValue(ctx, 0, idxOfDBCount)
		if err != nil {
			return err
		}
		outputRow[finalIdxOfTableCount], err = rs.GetValue(ctx, 0, idxOfTableCount)
		if err != nil {
			return err
		}
		outputRow[finalIdxOfRowCount], err = rs.GetValue(ctx, 0, idxOfRowCount)
		if err != nil {
			return err
		}
		outputRow[finalIdxOfSize], err = rs.GetValue(ctx, 0, idxOfSize)
		if err != nil {
			return err
		}
		outputRow[finalIdxOfComment], err = rsOfMoAccount.GetValue(ctx, uint64(i), idxOfComment)
		if err != nil {
			return err
		}
		outputRS.AddRow(outputRow)
	}
	return err
}
