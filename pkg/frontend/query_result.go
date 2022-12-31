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
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"strings"
)

func openSaveQueryResult(ses *Session) bool {
	// TODO: Graceful judgment
	sql := strings.ToLower(ses.sql)
	if strings.Contains(sql, "meta_scan") || strings.Contains(sql, "result_scan") || ses.tStmt == nil {
		return false
	}
	if ses.tStmt.SqlSourceType == "internal_sql" {
		return false
	}
	if strings.ToLower(ses.GetParameterUnit().SV.SaveQueryResult) == "on" {
		return true
	}
	// TODO: Increase priority
	val, err := ses.GetGlobalVar("save_query_result")
	if err != nil {
		return false
	}
	if v, _ := val.(int8); v > 0 {
		return true
	}
	return false
}

func saveQueryResult(ses *Session, bat *batch.Batch) error {
	fs := ses.GetParameterUnit().FileService
	// write query result
	path := catalog.BuildQueryResultPath(ses.GetTenantInfo().GetTenant(), uuid.UUID(ses.tStmt.StatementID).String(), ses.GetBlockIdx())
	writer, err := objectio.NewObjectWriter(path, fs)
	if err != nil {
		return err
	}
	_, err = writer.Write(bat)
	if err != nil {
		return err
	}
	_, err = writer.WriteEnd(ses.requestCtx)
	if err != nil {
		return err
	}
	return nil
}

func saveQueryResultMeta(ses *Session, bat *batch.Batch) error {
	defer func() {
		ses.ResetBlockIdx()
	}()
	fs := ses.GetParameterUnit().FileService
	// write query result meta
	b, err := ses.rs.Marshal()
	if err != nil {
		return err
	}
	buf := new(strings.Builder)
	prefix := ",\n"
	for i := 1; i <= ses.blockIdx; i++ {
		if i > 1 {
			buf.WriteString(prefix)
		}
		buf.WriteString(catalog.BuildQueryResultPath(ses.GetTenantInfo().GetTenant(), uuid.UUID(ses.tStmt.StatementID).String(), i))
	}

	m := &catalog.Meta{
		QueryId:    ses.tStmt.StatementID,
		Statement:  ses.tStmt.Statement,
		AccountId:  ses.GetTenantInfo().GetTenantID(),
		RoleId:     ses.tStmt.RoleId,
		ResultPath: buf.String(),
		CreateTime: types.CurrentTimestamp(),
		ResultSize: 100, // TODO: implement
		Columns:    string(b),
	}
	metaBat, err := buildQueryResultMetaBatch(m, ses.mp)
	if err != nil {
		return err
	}
	metaPath := catalog.BuildQueryResultMetaPath(ses.GetTenantInfo().GetTenant(), uuid.UUID(ses.tStmt.StatementID).String())
	metaWriter, err := objectio.NewObjectWriter(metaPath, fs)
	if err != nil {
		return err
	}
	_, err = metaWriter.Write(metaBat)
	if err != nil {
		return err
	}
	_, err = metaWriter.WriteEnd(ses.requestCtx)
	if err != nil {
		return err
	}
	return nil
}

func buildQueryResultMetaBatch(m *catalog.Meta, mp *mpool.MPool) (*batch.Batch, error) {
	var err error
	bat := batch.NewWithSize(len(catalog.MetaColTypes))
	bat.SetAttributes(catalog.MetaColNames)
	for i, t := range catalog.MetaColTypes {
		bat.Vecs[i] = vector.New(t)
	}
	if err = bat.Vecs[catalog.QUERY_ID_IDX].Append(types.Uuid(m.QueryId), false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.STATEMENT_IDX].Append([]byte(m.Statement), false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.ACCOUNT_ID_IDX].Append(m.AccountId, false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.ROLE_ID_IDX].Append(m.RoleId, false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.RESULT_PATH_IDX].Append([]byte(m.ResultPath), false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.CREATE_TIME_IDX].Append(m.CreateTime, false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.RESULT_SIZE_IDX].Append(m.ResultSize, false, mp); err != nil {
		return nil, err
	}
	if err = bat.Vecs[catalog.COLUMNS_IDX].Append([]byte(m.Columns), false, mp); err != nil {
		return nil, err
	}
	return bat, nil
}
