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
	"database/sql"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/google/uuid"

	cdc2 "github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

const (
	insertNewCdcTaskFormat = `insert into mo_catalog.mo_cdc_task values(` +
		`%d,` + //account id
		`"%s",` + //task id
		`"%s",` + //task name
		`"%s",` + //source_uri
		`"%s",` + //source_password
		`"%s",` + //sink_uri
		`"%s",` + //sink_type
		`"%s",` + //sink_password
		`"%s",` + //sink_ssl_ca_path
		`"%s",` + //sink_ssl_cert_path
		`"%s",` + //sink_ssl_key_path
		`"%s",` + //tables
		`"%s",` + //filters
		`"%s",` + //opfilters
		`"%s",` + //source_state
		`"%s",` + //sink_state
		`"%s",` + //start_ts
		`"%s",` + //end_ts
		`"%s",` + //config_file
		`"%s",` + //task_create_time
		`"%s",` + //state
		`%d,` + //checkpoint
		`"%d",` + //checkpoint_str
		`"%s",` + //full_config
		`"%s",` + //incr_config
		`"",` + //reserved0
		`"",` + //reserved1
		`"",` + //reserved2
		`"",` + //reserved3
		`""` + //reserved4
		`)`

	getCdcTaskFormat = `select ` +
		`sink_uri, ` +
		`sink_type, ` +
		`sink_password, ` +
		`tables, ` +
		`filters, ` +
		`start_ts ` +
		`from ` +
		`mo_catalog.mo_cdc_task ` +
		`where ` +
		`account_id = %d and ` +
		`task_id = "%s"`

	getDbIdAndTableIdFormat = "select reldatabase_id,rel_id from mo_catalog.mo_tables where account_id = %d and reldatabase = '%s' and relname = '%s'"

	getTable = "select rel_id from `mo_catalog`.`mo_tables` where account_id = %d and reldatabase ='%s' and relname = '%s'"

	showIndex = "show index from `%s`.`%s`"

	getAccount = "select account_id from `mo_catalog`.`mo_account` where account_name='%s'"
)

func getSqlForNewCdcTask(
	accId uint64,
	taskId uuid.UUID,
	taskName string,
	sourceUri string,
	sourcePwd string,
	sinkUri string,
	sinkTyp string,
	sinkPwd string,
	sinkCaPath string,
	sinkCertPath string,
	sinkKeyPath string,
	tables string,
	filters string,
	opfilters string,
	sourceState string,
	sinkState string,
	startTs string,
	endTs string,
	configFile string,
	taskCreateTime time.Time,
	state string,
	checkpoint uint64,
	fullConfig string,
	incrConfig string,
) string {
	return fmt.Sprintf(insertNewCdcTaskFormat,
		accId,
		taskId,
		taskName,
		sourceUri,
		sourcePwd,
		sinkUri,
		sinkTyp,
		sinkPwd,
		sinkCaPath,
		sinkCertPath,
		sinkKeyPath,
		tables,
		filters,
		opfilters,
		sourceState,
		sinkState,
		startTs,
		endTs,
		configFile,
		taskCreateTime.Format(time.DateTime),
		state,
		checkpoint,
		checkpoint,
		fullConfig,
		incrConfig,
	)
}

func getSqlForRetrievingCdcTask(
	accId uint64,
	taskId uuid.UUID,
) string {
	return fmt.Sprintf(getCdcTaskFormat, accId, taskId)
}

func getSqlForDbIdAndTableId(accId uint64, db, table string) string {
	return fmt.Sprintf(getDbIdAndTableIdFormat, accId, db, table)
}

func getSqlForGetTable(accountId uint64, db, table string) string {
	return fmt.Sprintf(getTable, accountId, db, table)
}

func getSqlForShowIndex(db, table string) string {
	return fmt.Sprintf(showIndex, db, table)
}

func getSqlForCheckAccount(account string) string {
	return fmt.Sprintf(getAccount, account)
}

const (
	AccountLevel      = "account"
	ClusterLevel      = "cluster"
	MysqlSink         = "mysql"
	MatrixoneSink     = "matrixone"
	ConsoleSink       = "console"
	SourceUriPrefix   = "mysql://"
	SinkUriPrefix     = "mysql://"
	ConsolePrefix     = "console://" //only used in testing stage
	EnableConsoleSink = true

	SASCommon = "common"
	SASError  = "error"
)

func handleCreateCdc(ses *Session, execCtx *ExecCtx, create *tree.CreateCDC) error {
	return doCreateCdc(execCtx.reqCtx, ses, create)
}

func doCreateCdc(ctx context.Context, ses *Session, create *tree.CreateCDC) (err error) {
	fmt.Fprintln(os.Stderr, "===>create cdc", create.Tables)
	ts := getGlobalPu().TaskService
	if ts == nil {
		return moerr.NewInternalError(ctx, "no task service is found")
	}

	cdcTaskOptionsMap := make(map[string]string)
	for i := 0; i < len(create.Option); i += 2 {
		cdcTaskOptionsMap[create.Option[i]] = create.Option[i+1]
	}

	cdcLevel := cdcTaskOptionsMap["Level"]
	cdcAccount := cdcTaskOptionsMap["Account"]

	if cdcLevel != AccountLevel {
		return moerr.NewInternalError(ctx, "invalid level. only support account level in 1.3")
	}

	if cdcAccount != ses.GetTenantInfo().GetTenant() {
		return moerr.NewInternalErrorf(ctx, "invalid account. account must be %s", ses.GetTenantInfo().GetTenant())
	}

	////////////
	//!!!NOTE!!!
	//1.3
	//	level: account level
	//	account: must be designated.
	///////////

	//step 1 : handle tables
	jsonTables, tablePts, err := preprocessTables(ctx, ses, cdcLevel, cdcAccount, create.Tables)
	if err != nil {
		return err
	}

	//step 2: handle filters
	//There must be no special characters (',' '.' ':' '`') in the single rule.
	var filters string
	filters = cdcTaskOptionsMap["Rules"]

	fmt.Fprintln(os.Stderr, "===>create cdc rules", filters)

	jsonFilters, filterPts, err := preprocessRules(ctx, filters)
	if err != nil {
		return err
	}

	err = attachAccountForFilters(ctx, ses, cdcLevel, cdcAccount, filterPts)
	if err != nil {
		return err
	}

	//TODO: refine it after 1.3
	//check table be filtered or not
	if filterTable(tablePts, filterPts) == 0 {
		return moerr.NewInternalError(ctx, "all tables has been excluded by filters. create cdc failed.")
	}

	dat := time.Now().UTC()

	creatorAccInfo := ses.GetTenantInfo()
	cdcId, _ := uuid.NewV7()

	//step 4: check uri format and strip password
	jsonSrcUri, _, err := extractUriInfo(ctx, create.SourceUri, SourceUriPrefix)
	if err != nil {
		return err
	}

	sinkType := strings.ToLower(create.SinkType)
	useConsole := false
	if EnableConsoleSink && sinkType == ConsoleSink {
		useConsole = true
	}

	if !useConsole && sinkType != MysqlSink && sinkType != MatrixoneSink {
		return moerr.NewInternalErrorf(ctx, "unsupported sink type: %s", create.SinkType)
	}

	var jsonSinkUri string
	var encodedSinkPwd string
	var sinkUriInfo cdc2.UriInfo
	if !useConsole {
		jsonSinkUri, sinkUriInfo, err = extractUriInfo(ctx, create.SinkUri, SinkUriPrefix)
		if err != nil {
			return err
		}

		encodedSinkPwd, err = sinkUriInfo.GetEncodedPassword()
		if err != nil {
			return err
		}
	}

	//step 5: create daemon task
	insertSql := getSqlForNewCdcTask(
		uint64(creatorAccInfo.GetTenantID()), //the account_id of cdc creator
		cdcId,
		create.TaskName.String(),
		jsonSrcUri, //json bytes
		"",
		jsonSinkUri, //json bytes
		sinkType,
		encodedSinkPwd, //encrypted password
		"",
		"",
		"",
		jsonTables,
		jsonFilters,
		"",
		SASCommon,
		SASCommon,
		"", //1.3 does not support startTs
		"", //1.3 does not support endTs
		cdcTaskOptionsMap["ConfigFile"],
		dat,
		taskservice.CdcRunning,
		0,
		"",
		"",
	)

	details := &task.Details{
		//account info that create cdc
		AccountID: creatorAccInfo.GetTenantID(),
		Account:   creatorAccInfo.GetTenant(),
		Username:  creatorAccInfo.GetUser(),
		Details: &task.Details_CreateCdc{
			CreateCdc: &task.CreateCdcDetails{
				TaskName: create.TaskName.String(),
				TaskId:   cdcId.String(),
				Accounts: []*task.Account{
					{
						Id:   uint64(creatorAccInfo.GetTenantID()),
						Name: cdcAccount,
					},
				},
			},
		},
	}

	fmt.Fprintln(os.Stderr, "====>save cdc task",
		creatorAccInfo.GetTenantID(),
		cdcId,
		create.TaskName,
		create.SourceUri,
		create.SinkUri,
		create.SinkType,
	)

	addCdcTaskCallback := func(ctx context.Context, tx taskservice.SqlExecutor) (ret int, err error) {
		err = checkAccounts(ctx, tx, tablePts, filterPts)
		if err != nil {
			return 0, err
		}

		ret, err = checkTables(ctx, tx, tablePts, filterPts)
		if err != nil {
			return 0, err
		}

		//insert cdc record into the mo_cdc_task
		exec, err := tx.ExecContext(ctx, insertSql)
		if err != nil {
			return 0, err
		}

		cdcTaskRowsAffected, err := exec.RowsAffected()
		if err != nil {
			return 0, err
		}

		if cdcTaskRowsAffected == 0 {
			return 0, nil
		}

		return int(cdcTaskRowsAffected), nil
	}

	if _, err = ts.AddCdcTask(ctx, cdcTaskMetadata(cdcId.String()), details, addCdcTaskCallback); err != nil {
		return err
	}
	return
}

func cdcTaskMetadata(cdcId string) task.TaskMetadata {
	return task.TaskMetadata{
		ID:       cdcId,
		Executor: task.TaskCode_InitCdc,
		Options: task.TaskOptions{
			MaxRetryTimes: defaultConnectorTaskMaxRetryTimes,
			RetryInterval: defaultConnectorTaskRetryInterval,
			DelayDuration: 0,
			Concurrency:   0,
		},
	}
}

// checkAccounts checks the accounts exists or not
func checkAccounts(ctx context.Context, tx taskservice.SqlExecutor, tablePts, filterPts *cdc2.PatternTuples) error {
	//step1 : collect accounts
	accounts := make(map[string]uint64)
	for _, pt := range tablePts.Pts {
		if pt == nil || pt.Source.Account == "" {
			continue
		}
		accounts[pt.Source.Account] = math.MaxUint64
	}

	for _, pt := range filterPts.Pts {
		if pt == nil || pt.Source.Account == "" {
			continue
		}
		accounts[pt.Source.Account] = math.MaxUint64
	}

	//step2 : collect account id
	//after this step, all account has accountid
	res := make(map[string]uint64)
	for acc := range accounts {
		exists, accId, err := checkAccountExists(ctx, tx, acc)
		if err != nil {
			return err
		}
		if !exists {
			return moerr.NewInternalError(ctx, fmt.Sprintf("account %s does not exist", acc))
		}
		res[acc] = accId
	}

	//step3: attach accountId
	for _, pt := range tablePts.Pts {
		if pt == nil || pt.Source.Account == "" {
			continue
		}
		pt.Source.AccountId = res[pt.Source.Account]
	}

	for _, pt := range filterPts.Pts {
		if pt == nil || pt.Source.Account == "" {
			continue
		}
		pt.Source.AccountId = res[pt.Source.Account]
	}

	return nil
}

// checkTables
// checks the table existed or not
// checks the table having the primary key
// filters the table
func checkTables(ctx context.Context, tx taskservice.SqlExecutor, tablePts, filterPts *cdc2.PatternTuples) (int, error) {
	var err error
	var found bool
	var hasPrimaryKey bool
	for _, pt := range tablePts.Pts {
		if pt == nil {
			continue
		}

		//skip tables that is filtered
		if needSkipThisTable(pt.Source.Account, pt.Source.Database, pt.Source.Table, filterPts) {
			continue
		}

		//check tables exists or not and filter the table
		found, err = checkTableExists(ctx, tx, pt.Source.AccountId, pt.Source.Database, pt.Source.Table)
		if err != nil {
			return 0, err
		}
		if !found {
			return 0, moerr.NewInternalErrorf(ctx, "no table %s:%s", pt.Source.Database, pt.Source.Table)
		}

		//check table has primary key
		hasPrimaryKey, err = checkPrimaryKey(ctx, tx, pt.Source.Database, pt.Source.Table)
		if err != nil {
			return 0, err
		}
		if !hasPrimaryKey {
			return 0, moerr.NewInternalErrorf(ctx, "table %s:%s does not have primary key", pt.Source.Database, pt.Source.Table)
		}
	}
	return 0, err
}

// filterTable checks the table filtered or not
// returns the count of tables that not be filtered
func filterTable(tablePts, filterPts *cdc2.PatternTuples) int {
	//check table be filtered or not
	leftCount := 0
	for _, pt := range tablePts.Pts {
		if pt == nil {
			continue
		}

		//skip tables that is filtered
		if needSkipThisTable(pt.Source.Account, pt.Source.Database, pt.Source.Table, filterPts) {
			continue
		}
		leftCount++
	}
	return leftCount
}

func queryTable(
	ctx context.Context,
	tx taskservice.SqlExecutor,
	query string,
	callback func(ctx context.Context, rows *sql.Rows) (bool, error)) (bool, error) {
	var rows *sql.Rows
	var err error
	rows, err = tx.QueryContext(ctx, query)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = rows.Close()
	}()

	var ret bool
	for rows.Next() {
		ret, err = callback(ctx, rows)
		if err != nil {
			return false, err
		}
		if ret {
			return true, nil
		}
	}
	return false, nil
}

func checkAccountExists(ctx context.Context, tx taskservice.SqlExecutor, account string) (bool, uint64, error) {
	checkSql := getSqlForCheckAccount(account)
	var err error
	var ret bool
	var accountId uint64
	ret, err = queryTable(ctx, tx, checkSql, func(ctx context.Context, rows *sql.Rows) (bool, error) {
		accountId = 0
		if err = rows.Scan(&accountId); err != nil {
			return false, err
		}
		return true, nil
	})

	return ret, accountId, err
}

func checkTableExists(ctx context.Context, tx taskservice.SqlExecutor, accountId uint64, db, table string) (bool, error) {
	//select from mo_tables
	checkSql := getSqlForGetTable(accountId, db, table)
	var err error
	var ret bool
	var tableId uint64
	ret, err = queryTable(ctx, tx, checkSql, func(ctx context.Context, rows *sql.Rows) (bool, error) {
		tableId = 0
		if err = rows.Scan(&tableId); err != nil {
			return false, err
		}
		return true, nil
	})

	return ret, err
}

func checkPrimaryKey(ctx context.Context, tx taskservice.SqlExecutor, db, table string) (bool, error) {
	checkSql := getSqlForShowIndex(db, table)
	var ret bool
	var err error
	var tableName string
	var nonUnique int
	var keyName string
	var seqInIndex int
	var columnName string
	var collation string
	var card int
	var subpart string
	var packed string
	var yes string
	var indexType string
	var comment string
	var indexComment string
	var indexParams string
	var visible string
	var expr string

	ret, err = queryTable(ctx, tx, checkSql, func(ctx context.Context, rows *sql.Rows) (bool, error) {
		tableName = ""
		nonUnique = 0
		keyName = ""
		seqInIndex = 0
		columnName = ""
		card = 0
		subpart = ""
		packed = ""
		yes = ""
		indexType = ""
		comment = ""
		indexComment = ""
		indexParams = ""
		visible = ""
		expr = ""
		/*
			Reference To: https://docs.matrixorigin.cn/en/1.2.2/MatrixOne/Reference/SQL-Reference/Other/SHOW-Statements/show-index/#examples
			CREATE TABLE show_01(sname varchar(30),id int);
			mysql> show INDEX FROM show_01;
			+---------+------------+------------+--------------+-------------+-----------+-------------+----------+--------+------+------------+------------------+---------+------------+
			| Table   | Non_unique | Key_name   | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment          | Visible | Expression |
			+---------+------------+------------+--------------+-------------+-----------+-------------+----------+--------+------+------------+------------------+---------+------------+
			| show_01 |          0 | id         |            1 | id          | A         |           0 | NULL     | NULL   | YES  |            |                  | YES     | NULL       |
			| show_01 |          0 | sname      |            1 | sname       | A         |           0 | NULL     | NULL   | YES  |            |                  | YES     | NULL       |
			| show_01 |          0 | __mo_rowid |            1 | __mo_rowid  | A         |           0 | NULL     | NULL   | NO   |            | Physical address | NO      | NULL       |
			+---------+------------+------------+--------------+-------------+-----------+-------------+----------+--------+------+------------+------------------+---------+------------+
			3 rows in set (0.02 sec)
		*/
		if err = rows.Scan(
			&tableName,
			&nonUnique,
			&keyName,
			&seqInIndex,
			&columnName,
			&collation,
			&card,
			&subpart,
			&packed,
			&yes,
			&indexType,
			&comment,
			&indexComment,
			&indexParams,
			&visible,
			&expr,
		); err != nil {
			return false, err
		}
		if strings.ToLower(keyName) == "primary" {
			return true, nil
		}
		return false, nil
	})

	return ret, err
}

// extractTablePair
// extract source:sink pair from the pattern
//
//	There must be no special characters (','  '.'  ':' '`') in account name & database name & table name.
func extractTablePair(ctx context.Context, pattern string) (*cdc2.PatternTuple, error) {
	var err error
	pattern = strings.TrimSpace(pattern)
	//step1 : split table pair by ':' => table0 table1
	//step2 : split table0/1 by '.' => account database table
	//step3 : check table accord with regular expression
	pt := &cdc2.PatternTuple{OriginString: pattern}
	if strings.Contains(pattern, ":") {
		//Format: account.db.table:db:table
		splitRes := strings.Split(pattern, ":")
		if len(splitRes) != 2 {
			return nil, moerr.NewInternalErrorf(ctx, "must be source : sink. invalid format")
		}

		//handle source part
		pt.Source.Account, pt.Source.Database, pt.Source.Table, pt.Source.TableIsRegexp, err = extractTableInfo(ctx, splitRes[0], false)
		if err != nil {
			return nil, err
		}

		//handle sink part
		pt.Sink.Account, pt.Sink.Database, pt.Sink.Table, pt.Sink.TableIsRegexp, err = extractTableInfo(ctx, splitRes[1], false)
		if err != nil {
			return nil, err
		}
		return pt, nil
	}

	//Format: account.db.table
	//handle source part only
	pt.Source.Account, pt.Source.Database, pt.Source.Table, pt.Source.TableIsRegexp, err = extractTableInfo(ctx, pattern, false)
	if err != nil {
		return nil, err
	}
	pt.Sink.Account = pt.Source.Account
	pt.Sink.Database = pt.Source.Database
	pt.Sink.Table = pt.Source.Table
	pt.Sink.TableIsRegexp = pt.Source.TableIsRegexp
	return pt, nil
}

// extractTableInfo
// get account,database,table info from string
//
// account: may be empty
// database: must be concrete name instead of pattern.
// table: must be concrete name or pattern in the source part. must be concrete name in the destination part
// isRegexpTable: table name is regular expression
// !!!NOTE!!!
//
//	There must be no special characters (','  '.'  ':' '`') in account name & database name & table name.
func extractTableInfo(ctx context.Context, input string, mustBeConcreteTable bool) (account string, db string, table string, isRegexpTable bool, err error) {
	parts := strings.Split(strings.TrimSpace(input), ".")
	if len(parts) != 2 && len(parts) != 3 {
		return "", "", "", false, moerr.NewInternalErrorf(ctx, "needs account.database.table or database.table. invalid format.")
	}

	if len(parts) == 2 {
		db, table = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	} else {
		account, db, table = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), strings.TrimSpace(parts[2])

		if !accountNameIsLegal(account) {
			return "", "", "", false, moerr.NewInternalErrorf(ctx, "invalid account name")
		}
	}

	if !dbNameIsLegal(db) {
		return "", "", "", false, moerr.NewInternalErrorf(ctx, "invalid database name")
	}

	if !tableNameIsLegal(table) {
		return "", "", "", false, moerr.NewInternalErrorf(ctx, "invalid table name")
	}

	return
}

// preprocessTables extract tables and serialize them
func preprocessTables(
	ctx context.Context,
	ses *Session,
	level string,
	account string,
	tables string) (string, *cdc2.PatternTuples, error) {
	tablesPts, err := extractTablePairs(ctx, tables)
	if err != nil {
		return "", nil, err
	}

	//step 2: check privilege
	if err = canCreateCdcTask(ctx, ses, level, account, tablesPts); err != nil {
		return "", nil, err
	}

	jsonTablePts, err := cdc2.JsonEncode(tablesPts)
	if err != nil {
		return "", nil, err
	}
	return jsonTablePts, tablesPts, nil
}

/*
extractTablePairs extracts all source:sink pairs from the pattern
There must be no special characters (','  '.'  ':' '`') in account name & database name & table name.
*/
func extractTablePairs(ctx context.Context, pattern string) (*cdc2.PatternTuples, error) {
	pattern = strings.TrimSpace(pattern)
	pts := &cdc2.PatternTuples{}

	tablePairs := strings.Split(pattern, ",")
	if len(tablePairs) == 0 {
		return nil, fmt.Errorf("invalid pattern format")
	}

	//step1 : split pattern by ',' => table pair
	for _, pair := range tablePairs {
		pt, err := extractTablePair(ctx, pair)
		if err != nil {
			return nil, err
		}
		pts.Append(pt)
	}

	return pts, nil
}

func preprocessRules(ctx context.Context, rules string) (string, *cdc2.PatternTuples, error) {
	pts, err := extractRules(ctx, rules)
	if err != nil {
		return "", nil, err
	}

	jsonPts, err := cdc2.JsonEncode(pts)
	if err != nil {
		return "", nil, err
	}
	return jsonPts, pts, nil
}

/*
extractRules extracts filters
pattern maybe empty string. then, it returns empty PatternTuples
There must be no special characters (','  '.'  ':' '`') in account name & database name & table name.
*/
func extractRules(ctx context.Context, pattern string) (*cdc2.PatternTuples, error) {
	pattern = strings.TrimSpace(pattern)
	pts := &cdc2.PatternTuples{}
	if len(pattern) == 0 {
		return pts, nil
	}

	tablePairs := strings.Split(pattern, ",")
	if len(tablePairs) == 0 {
		return nil, fmt.Errorf("invalid pattern format")
	}
	var err error
	//step1 : split pattern by ',' => table pair
	for _, pair := range tablePairs {
		pt := &cdc2.PatternTuple{}
		pt.Source.Account, pt.Source.Database, pt.Source.Table, pt.Source.TableIsRegexp, err = extractTableInfo(ctx, pair, false)
		if err != nil {
			return nil, err
		}
		pts.Append(pt)
	}

	return pts, nil
}

func canCreateCdcTask(ctx context.Context, ses *Session, level string, account string, pts *cdc2.PatternTuples) error {
	if strings.EqualFold(level, ClusterLevel) {
		if !ses.tenant.IsMoAdminRole() {
			return moerr.NewInternalError(ctx, "Only sys account administrator are allowed to create cluster level task")
		}
		for _, pt := range pts.Pts {
			if pt.Source.Account == "" {
				pt.Source.Account = ses.GetTenantName()
			}
			if isBannedDatabase(pt.Source.Database) {
				return moerr.NewInternalError(ctx, "The system database cannot be subscribed to")
			}
		}
	} else if strings.EqualFold(level, AccountLevel) {
		if !ses.tenant.IsMoAdminRole() && ses.GetTenantName() != account {
			return moerr.NewInternalErrorf(ctx, "No privilege to create task on %s", account)
		}
		for _, pt := range pts.Pts {
			if pt.Source.Account == "" {
				pt.Source.Account = account
			}
			if account != pt.Source.Account {
				return moerr.NewInternalErrorf(ctx, "No privilege to create task on table %s", pt.OriginString)
			}
			if isBannedDatabase(pt.Source.Database) {
				return moerr.NewInternalError(ctx, "The system database cannot be subscribed to")
			}
		}
	} else {
		return moerr.NewInternalErrorf(ctx, "Incorrect level %s", level)
	}
	return nil
}

func attachAccountForFilters(ctx context.Context, ses *Session, level string, account string, pts *cdc2.PatternTuples) error {
	if strings.EqualFold(level, ClusterLevel) {
		if !ses.tenant.IsMoAdminRole() {
			return moerr.NewInternalError(ctx, "Only sys account administrator are allowed to create cluster level task")
		}
		for _, pt := range pts.Pts {
			if pt.Source.Account == "" {
				pt.Source.Account = ses.GetTenantName()
			}
		}
	} else if strings.EqualFold(level, AccountLevel) {
		if !ses.tenant.IsMoAdminRole() && ses.GetTenantName() != account {
			return moerr.NewInternalErrorf(ctx, "No privilege to create task on %s", account)
		}
		for _, pt := range pts.Pts {
			if pt.Source.Account == "" {
				pt.Source.Account = account
			}
			if account != pt.Source.Account {
				return moerr.NewInternalErrorf(ctx, "No privilege to create task on table %s", pt.OriginString)
			}
		}
	} else {
		return moerr.NewInternalErrorf(ctx, "Incorrect level %s", level)
	}
	return nil
}

func RegisterCdcExecutor(
	logger *zap.Logger,
	ts taskservice.TaskService,
	ieFactory func() ie.InternalExecutor,
	attachToTask func(context.Context, uint64, taskservice.ActiveRoutine) error,
	cnUUID string,
	fileService fileservice.FileService,
	cnTxnClient client.TxnClient,
	cnEngine engine.Engine,
	cnEngMp *mpool.MPool,
) func(ctx context.Context, task task.Task) error {
	return func(ctx context.Context, T task.Task) error {
		fmt.Fprintln(os.Stderr, "====>", "cdc task executor")
		ctx1, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		tasks, err := ts.QueryDaemonTask(ctx1,
			taskservice.WithTaskIDCond(taskservice.EQ, T.GetID()),
		)
		if err != nil {
			return err
		}
		if len(tasks) != 1 {
			return moerr.NewInternalErrorf(ctx, "invalid tasks count %d", len(tasks))
		}
		details, ok := tasks[0].Details.Details.(*task.Details_CreateCdc)
		if !ok {
			return moerr.NewInternalError(ctx, "invalid details type")
		}

		fmt.Fprintln(os.Stderr, "====>", "cdc task info 1", tasks[0].String())
		taskId := details.CreateCdc.GetTaskId()
		taskName := details.CreateCdc.GetTaskName()

		fmt.Fprintln(os.Stderr, "====>", "cdc task info 2", taskId, taskName)

		cdc := NewCdcTask(
			logger,
			ieFactory(),
			details.CreateCdc,
			cnUUID,
			fileService,
			cnTxnClient,
			cnEngine,
			cnEngMp,
		)
		cdc.activeRoutine = cdc2.NewCdcActiveRoutine()
		if err = attachToTask(ctx, T.GetID(), cdc); err != nil {
			return err
		}
		return cdc.Start(ctx, true)
	}
}

type CdcTask struct {
	logger *zap.Logger
	ie     ie.InternalExecutor

	cnUUID      string
	cnTxnClient client.TxnClient
	cnEngine    engine.Engine
	fileService fileservice.FileService

	cdcTask *task.CreateCdcDetails

	mp         *mpool.MPool
	packerPool *fileservice.Pool[*types.Packer]

	sinkUri cdc2.UriInfo
	tables  cdc2.PatternTuples
	filters cdc2.PatternTuples
	startTs types.TS

	activeRoutine *cdc2.ActiveRoutine
	// sunkWatermarkUpdater update the watermark of the items that has been sunk to downstream
	sunkWatermarkUpdater *cdc2.WatermarkUpdater
}

func NewCdcTask(
	logger *zap.Logger,
	ie ie.InternalExecutor,
	cdcTask *task.CreateCdcDetails,
	cnUUID string,
	fileService fileservice.FileService,
	cnTxnClient client.TxnClient,
	cnEngine engine.Engine,
	cdcMp *mpool.MPool,
) *CdcTask {
	return &CdcTask{
		logger:      logger,
		ie:          ie,
		cdcTask:     cdcTask,
		cnUUID:      cnUUID,
		fileService: fileService,
		cnTxnClient: cnTxnClient,
		cnEngine:    cnEngine,
		mp:          cdcMp,
		packerPool: fileservice.NewPool(
			128,
			func() *types.Packer {
				return types.NewPacker()
			},
			func(packer *types.Packer) {
				packer.Reset()
			},
			func(packer *types.Packer) {
				packer.Close()
			},
		),
	}
}

func (cdc *CdcTask) Start(rootCtx context.Context, firstTime bool) (err error) {
	defer func() {
		if err != nil {
			// if Start failed, there will be some dangle goroutines(watermarkUpdater, reader, sinker...)
			// need to close them to avoid goroutine leak
			close(cdc.activeRoutine.Cancel)
		}
	}()

	fmt.Fprintln(os.Stderr, "====> cdc start")

	ctx := defines.AttachAccountId(rootCtx, uint32(cdc.cdcTask.Accounts[0].GetId()))

	//step1 : get cdc task definition
	err = cdc.retrieveCdcTask(ctx)
	if err != nil {
		return err
	}

	//check table be filtered or not
	if filterTable(&cdc.tables, &cdc.filters) == 0 {
		return moerr.NewInternalError(ctx, "all tables has been excluded by filters. start cdc failed.")
	}

	//step2 : get source tableid
	var info *cdc2.DbTableInfo
	dbTableInfos := make([]*cdc2.DbTableInfo, 0, len(cdc.tables.Pts))
	for _, tuple := range cdc.tables.Pts {
		if needSkipThisTable(tuple.Source.Account, tuple.Source.Database, tuple.Source.Table, &cdc.filters) {
			logutil.Infof("cdc skip table %s:%s by filter", tuple.Source.Database, tuple.Source.Table)
			continue
		}
		//get dbid tableid for the source table
		info, err = cdc.retrieveTable(ctx, tuple.Source.Database, tuple.Source.Table)
		if err != nil {
			return err
		}

		info.SinkAccountName = tuple.Sink.Account
		info.SinkDbName = tuple.Sink.Database
		info.SinkTblName = tuple.Sink.Table

		dbTableInfos = append(dbTableInfos, info)
	}

	// start watermark updater
	cdc.sunkWatermarkUpdater = cdc2.NewWatermarkUpdater(cdc.cdcTask.Accounts[0].GetId(), cdc.cdcTask.TaskId, cdc.ie)

	count, err := cdc.sunkWatermarkUpdater.GetCountFromDb()
	if err != nil {
		return err
	} else if count == 0 {
		for _, info = range dbTableInfos {
			// use startTs as watermark
			if err = cdc.sunkWatermarkUpdater.InsertIntoDb(info.SourceTblId, cdc.startTs); err != nil {
				return err
			}
		}
	}
	go cdc.sunkWatermarkUpdater.Run(cdc.activeRoutine)

	// create exec pipelines
	for _, info = range dbTableInfos {
		if err = cdc.addExecPipelineForTable(info); err != nil {
			return
		}
	}

	if firstTime {
		// hold
		ch := make(chan int, 1)
		<-ch
	}
	return
}

func (cdc *CdcTask) retrieveCdcTask(ctx context.Context) error {
	cdcTaskId, _ := uuid.Parse(cdc.cdcTask.TaskId)
	sql := getSqlForRetrievingCdcTask(cdc.cdcTask.Accounts[0].GetId(), cdcTaskId)
	res := cdc.ie.Query(ctx, sql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		return res.Error()
	}

	if res.RowCount() < 1 {
		return moerr.NewInternalErrorf(ctx, "none cdc task for %d %s", cdc.cdcTask.Accounts[0].GetId(), cdc.cdcTask.TaskId)
	} else if res.RowCount() > 1 {
		return moerr.NewInternalErrorf(ctx, "duplicate cdc task for %d %s", cdc.cdcTask.Accounts[0].GetId(), cdc.cdcTask.TaskId)
	}

	//sink_type
	sinkTyp, err := res.GetString(ctx, 0, 1)
	if err != nil {
		return err
	}

	var sinkPwd string
	if sinkTyp != ConsoleSink {
		//sink uri
		jsonSinkUri, err := res.GetString(ctx, 0, 0)
		if err != nil {
			return err
		}

		err = cdc2.JsonDecode(jsonSinkUri, &cdc.sinkUri)
		if err != nil {
			return err
		}

		//sink_password
		sinkPwd, err = res.GetString(ctx, 0, 2)
		if err != nil {
			return err
		}

		cdc.sinkUri.Password, err = cdc2.AesCFBDecode(ctx, sinkPwd)
		if err != nil {
			return err
		}
	}

	//update sink type after deserialize
	cdc.sinkUri.SinkTyp = sinkTyp

	//tables
	jsonTables, err := res.GetString(ctx, 0, 3)
	if err != nil {
		return err
	}

	err = cdc2.JsonDecode(jsonTables, &cdc.tables)
	if err != nil {
		return err
	}

	//filters
	jsonFilters, err := res.GetString(ctx, 0, 4)
	if err != nil {
		return err
	}

	err = cdc2.JsonDecode(jsonFilters, &cdc.filters)
	if err != nil {
		return err
	}

	// TODO read startTs from db
	cdc.startTs = types.TS{}

	fmt.Fprintln(os.Stderr, "====>", "cdc task row",
		cdc.sinkUri,
		sinkTyp,
		sinkPwd,
		cdc.tables,
		cdc.filters,
	)
	return nil
}

func (cdc *CdcTask) retrieveTable(ctx context.Context, dbName, tblName string) (*cdc2.DbTableInfo, error) {
	var dbId, tblId uint64
	var err error
	sql := getSqlForDbIdAndTableId(cdc.cdcTask.Accounts[0].GetId(), dbName, tblName)
	res := cdc.ie.Query(ctx, sql, ie.SessionOverrideOptions{})
	if res.Error() != nil {
		return nil, res.Error()
	}

	/*
		missing table will be handled in the future.
	*/
	if res.RowCount() < 1 {

		return nil, moerr.NewInternalErrorf(ctx, "no table %s:%s", dbName, tblName)
	} else if res.RowCount() > 1 {
		return nil, moerr.NewInternalErrorf(ctx, "duplicate table %s:%s", dbName, tblName)
	}

	if dbId, err = res.GetUint64(ctx, 0, 0); err != nil {
		return nil, err
	}

	if tblId, err = res.GetUint64(ctx, 0, 1); err != nil {
		return nil, err
	}

	return &cdc2.DbTableInfo{
		SourceDbName:  dbName,
		SourceTblName: tblName,
		SourceDbId:    dbId,
		SourceTblId:   tblId,
	}, err
}

func needSkipThisTable(accountName, dbName, tblName string, filters *cdc2.PatternTuples) bool {
	if len(filters.Pts) == 0 {
		return false
	}
	for _, filter := range filters.Pts {
		if filter == nil {
			continue
		}
		if filter.Source.Account == accountName &&
			filter.Source.Database == dbName &&
			filter.Source.Table == tblName {
			return true
		}
	}
	return false
}

// Resume cdc task from last recorded watermark
func (cdc *CdcTask) Resume() (err error) {
	fmt.Println("=====> it's resume")
	for {
		// closed in Pause, need renew
		cdc.activeRoutine.Cancel = make(chan struct{})
		if err = cdc.Start(context.Background(), false); err == nil {
			return
		}
		_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Resume task %s failed, err: %v\n", cdc.cdcTask.TaskName, err)
		time.Sleep(time.Second)
	}
}

// Restart cdc task from init watermark
func (cdc *CdcTask) Restart() (err error) {
	fmt.Println("=====> it's restart")
	for {
		// closed in Pause, need renew
		cdc.activeRoutine.Cancel = make(chan struct{})
		// delete previous records
		if err = cdc.sunkWatermarkUpdater.DeleteAllFromDb(); err == nil {
			if err = cdc.Start(context.Background(), false); err == nil {
				return
			}
		}
		_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Restart task %s failed, err: %v\n", cdc.cdcTask.TaskName, err)
		time.Sleep(time.Second)
	}
}

// Pause cdc task
func (cdc *CdcTask) Pause() error {
	fmt.Println("=====> it's pause")
	close(cdc.activeRoutine.Cancel)
	cdc.activeRoutine.Cancel = nil
	return nil
}

// Cancel cdc task
func (cdc *CdcTask) Cancel() error {
	fmt.Println("=====> it's cancel")
	if cdc.activeRoutine.Cancel != nil {
		close(cdc.activeRoutine.Cancel)
	}
	return cdc.sunkWatermarkUpdater.DeleteAllFromDb()
}

func handleDropCdc(ses *Session, execCtx *ExecCtx, st *tree.DropCDC) error {
	return updateCdc(execCtx.reqCtx, ses, st)
}

func handlePauseCdc(ses *Session, execCtx *ExecCtx, st *tree.PauseCDC) error {
	return updateCdc(execCtx.reqCtx, ses, st)
}

func handleResumeCdc(ses *Session, execCtx *ExecCtx, st *tree.ResumeCDC) error {
	return updateCdc(execCtx.reqCtx, ses, st)
}

func handleRestartCdc(ses *Session, execCtx *ExecCtx, st *tree.RestartCDC) error {
	return updateCdc(execCtx.reqCtx, ses, st)
}

func updateCdc(ctx context.Context, ses *Session, st tree.Statement) (err error) {
	var targetTaskStatus task.TaskStatus
	ts := getGlobalPu().TaskService
	if ts == nil {
		return moerr.NewInternalError(ctx,
			"task service not ready yet, please try again later.")
	}
	//TODO: clarify the accountid following
	switch stmt := st.(type) {
	case *tree.DropCDC:
		targetTaskStatus = task.TaskStatus_CancelRequested
		if stmt.Option.All {
			_, err = ts.UpdateCdcTask(ctx, targetTaskStatus,
				taskservice.WithAccountID(taskservice.EQ, ses.GetTenantInfo().GetTenantID()),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		} else {
			_, err = ts.UpdateCdcTask(ctx, targetTaskStatus,
				taskservice.WithAccountID(taskservice.EQ, ses.GetTenantInfo().GetTenantID()),
				taskservice.WithTaskName(taskservice.EQ, stmt.Option.TaskName.String()),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		}
	case *tree.PauseCDC:
		targetTaskStatus = task.TaskStatus_PauseRequested
		if stmt.Option.All {
			_, err = ts.UpdateCdcTask(ctx, targetTaskStatus,
				taskservice.WithAccountID(taskservice.EQ, ses.GetTenantInfo().GetTenantID()),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		} else {
			_, err = ts.UpdateCdcTask(ctx, targetTaskStatus,
				taskservice.WithAccountID(taskservice.EQ, ses.GetTenantInfo().GetTenantID()),
				taskservice.WithTaskName(taskservice.EQ, stmt.Option.TaskName.String()),
				taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
			)
		}
	case *tree.RestartCDC:
		targetTaskStatus = task.TaskStatus_RestartRequested
		_, err = ts.UpdateCdcTask(ctx, targetTaskStatus,
			taskservice.WithAccountID(taskservice.EQ, ses.GetTenantInfo().GetTenantID()),
			taskservice.WithTaskName(taskservice.EQ, stmt.TaskName.String()),
			taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
		)
	case *tree.ResumeCDC:
		targetTaskStatus = task.TaskStatus_ResumeRequested
		_, err = ts.UpdateCdcTask(ctx, targetTaskStatus,
			taskservice.WithAccountID(taskservice.EQ, ses.GetTenantInfo().GetTenantID()),
			taskservice.WithTaskName(taskservice.EQ, stmt.TaskName.String()),
			taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()),
		)
	}
	if err != nil {
		return err
	}
	return
}

func (cdc *CdcTask) addExecPipelineForTable(info *cdc2.DbTableInfo) error {
	// reader --call--> sinker ----> remote db
	ctx := defines.AttachAccountId(context.Background(), uint32(cdc.cdcTask.Accounts[0].GetId()))

	// add watermark to updater
	watermark, err := cdc.sunkWatermarkUpdater.GetFromDb(info.SourceTblId)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(os.Stderr, "table %s(%d) init watermark: %s\n",
		info.SourceTblName, info.SourceTblId, watermark.ToString())
	cdc.sunkWatermarkUpdater.UpdateMem(info.SourceTblId, watermark)

	tableDef, err := cdc2.GetTableDef(ctx, cdc.cnEngine, cdc.cnTxnClient, info.SourceTblId)
	if err != nil {
		return err
	}

	// make sinker for table
	sinker, err := cdc2.NewSinker(
		cdc.sinkUri,
		info,
		cdc.sunkWatermarkUpdater,
		tableDef,
		cdc2.DefaultRetryTimes,
		cdc2.DefaultRetryDuration,
	)
	if err != nil {
		return err
	}

	// make reader
	reader := cdc2.NewTableReader(
		cdc.cnTxnClient,
		cdc.cnEngine,
		cdc.mp,
		cdc.packerPool,
		info,
		sinker,
		cdc.sunkWatermarkUpdater,
		tableDef,
		cdc.ResetWatermarkForTable,
	)
	go reader.Run(ctx, cdc.activeRoutine)

	_, _ = fmt.Fprintf(os.Stderr, "ExecPipeline for table %s(%d) has been added\n", info.SourceTblName, info.SourceTblId)
	return nil
}

func (cdc *CdcTask) ResetWatermarkForTable(info *cdc2.DbTableInfo) (err error) {
	tblId := info.SourceTblId
	_, _ = fmt.Fprintf(os.Stderr, "ResetWatermarkForTable: %s(%d)\n", info.SourceTblName, tblId)

	// delete old watermark of table
	cdc.sunkWatermarkUpdater.DeleteFromMem(tblId)
	if err = cdc.sunkWatermarkUpdater.DeleteFromDb(tblId); err != nil {
		return
	}

	// use start_ts as init watermark
	if err = cdc.sunkWatermarkUpdater.InsertIntoDb(tblId, cdc.startTs); err != nil {
		return
	}
	cdc.sunkWatermarkUpdater.UpdateMem(tblId, cdc.startTs)
	return
}
