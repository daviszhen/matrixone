// Copyright 2022 Matrix Origin
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

package cache

import (
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/tidwall/btree"
)

func NewCatalog() *CatalogCache {
	return &CatalogCache{
		tables: &tableCache{
			data:       btree.NewBTreeG(tableItemLess),
			rowidIndex: btree.NewBTreeG(tableItemRowidLess),
		},
		databases: &databaseCache{
			data:       btree.NewBTreeG(databaseItemLess),
			rowidIndex: btree.NewBTreeG(databaseItemRowidLess),
		},
	}
}

func (cc *CatalogCache) GC(ts timestamp.Timestamp) {
	{ // table cache gc
		var items []*TableItem

		cc.tables.data.Scan(func(item *TableItem) bool {
			if len(items) > GcBuffer {
				return false
			}
			if item.Ts.Less(ts) {
				items = append(items, item)
			}
			return true
		})
		for _, item := range items {
			fmt.Println("gc1", item.deleted, item.DatabaseId, item.Name, item.Id, item.Ts, item.Rowid)
			cc.tables.data.Delete(item)
			if !item.deleted {
				fmt.Println("gc2", item.deleted, item.DatabaseId, item.Name, item.Id, item.Ts, item.Rowid)
				cc.tables.rowidIndex.Delete(item)
			}
		}
	}
	{ // database cache gc
		var items []*DatabaseItem

		cc.databases.data.Scan(func(item *DatabaseItem) bool {
			if len(items) > GcBuffer {
				return false
			}
			if item.Ts.Less(ts) {
				items = append(items, item)
			}
			return true
		})
		for _, item := range items {
			cc.databases.data.Delete(item)
			if !item.deleted {
				cc.databases.rowidIndex.Delete(item)
			}
		}
	}
}

func (cc *CatalogCache) Tables(accountId uint32, databaseId uint64,
	ts timestamp.Timestamp) ([]string, []uint64) {
	var rs []string
	var rids []uint64

	key := &TableItem{
		AccountId:  accountId,
		DatabaseId: databaseId,
	}
	mp := make(map[uint64]uint8)
	cc.tables.data.Ascend(key, func(item *TableItem) bool {
		if item.AccountId != accountId {
			return false
		}
		if item.DatabaseId != databaseId {
			return false
		}
		if item.Ts.Greater(ts) {
			return true
		}
		if _, ok := mp[item.Id]; !ok {
			mp[item.Id] = 0
			if !item.deleted {
				rs = append(rs, item.Name)
				rids = append(rids, item.Id)
			}
		}
		return true
	})
	return rs, rids
}

func (cc *CatalogCache) GetTableById(databaseId, tblId uint64) *TableItem {
	var rel *TableItem

	key := &TableItem{
		DatabaseId: databaseId,
	}
	// If account is much, the performance is very bad.
	cc.tables.data.Ascend(key, func(item *TableItem) bool {
		if item.Id == tblId {
			rel = item
			return false
		}
		return true
	})
	return rel
}

func (cc *CatalogCache) Databases(accountId uint32, ts timestamp.Timestamp) []string {
	var rs []string

	key := &DatabaseItem{
		AccountId: accountId,
	}
	mp := make(map[string]uint8)
	cc.databases.data.Ascend(key, func(item *DatabaseItem) bool {
		if item.AccountId != accountId {
			return false
		}
		if item.Ts.Greater(ts) {
			return true
		}
		if _, ok := mp[item.Name]; !ok {
			mp[item.Name] = 0
			if !item.deleted {
				rs = append(rs, item.Name)
			}
		}
		return true
	})
	return rs
}

func (cc *CatalogCache) GetTable(tbl *TableItem) bool {
	//var find bool
	//var ts timestamp.Timestamp
	/**
	In push mode.
	It is necessary to distinguish the case create table/drop table
	from truncate table.

	CORNER CASE 1:
	begin;
	create table t1(a int);//table id x. catalog.insertTable(table id x)
	insert into t1 values (1);
	drop table t1; //same table id x. catalog.deleteTable(table id x)
	commit;

	CORNER CASE 2:
	create table t1(a int); //table id x.
	begin;
	insert into t1 values (1);
	-- @session:id=1{
	truncate table t1;//insert table id y, then delete table id x. catalog.insertTable(table id y). catalog.deleteTable(table id x)
	-- @session}
	commit;
	*/
	//var tableId uint64
	cc.tables.data.Scan(func(item *TableItem) bool {
		if item.AccountId == tbl.AccountId &&
			item.DatabaseId == tbl.DatabaseId && item.Name == tbl.Name {
			fmt.Println("GetTable u", item.deleted, item.DatabaseId, item.Name, item.Ts, item.Id, item.Rowid)
		}
		return true
	})

	cc.tables.data.Ascend(tbl, func(item *TableItem) bool {
		if item.AccountId == tbl.AccountId &&
			item.DatabaseId == tbl.DatabaseId && item.Name == tbl.Name {
			fmt.Println("GetTable v", item.deleted, item.DatabaseId, item.Name, item.Ts, item.Id, item.Rowid)
		}
		return true
	})

	deleted := make(map[uint64]*TableItem)
	inserted := make(map[uint64]*TableItem)

	//collect all deleted and inserted items
	cc.tables.data.Ascend(tbl, func(item *TableItem) bool {
		if item.AccountId == tbl.AccountId &&
			item.DatabaseId == tbl.DatabaseId && item.Name == tbl.Name {
			if item.deleted {
				deleted[item.Id] = item
			} else {
				inserted[item.Id] = item
			}
		}
		return true
	})

	//remove deleted item
	for rowid, _ := range deleted {
		delete(inserted, rowid)
	}

	if len(inserted) == 0 {
		return false
	}

	if len(inserted) > 1 {
		panic("multiple table ")
	}

	//get item
	for _, item := range inserted {
		tbl.Id = item.Id
		tbl.Defs = item.Defs
		tbl.Kind = item.Kind
		tbl.Comment = item.Comment
		tbl.ViewDef = item.ViewDef
		tbl.TableDef = item.TableDef
		tbl.Constraint = item.Constraint
		tbl.Partitioned = item.Partitioned
		tbl.Partition = item.Partition
		tbl.CreateSql = item.CreateSql
		tbl.PrimaryIdx = item.PrimaryIdx
		tbl.ClusterByIdx = item.ClusterByIdx
		copy(tbl.Rowid[:], item.Rowid[:])
		tbl.Rowids = make([]types.Rowid, len(item.Rowids))
		for i, rowid := range item.Rowids {
			copy(tbl.Rowids[i][:], rowid[:])
		}
	}

	return true

	//cc.tables.data.Ascend(tbl, func(item *TableItem) bool {
	//	//if item.AccountId == tbl.AccountId &&
	//	//	item.DatabaseId == tbl.DatabaseId && item.Name == tbl.Name {
	//	//	fmt.Println("GetTable w", item.deleted, item.DatabaseId, item.Name, item.Ts, item.Id, item.Rowid)
	//	//}
	//	if item.deleted && item.AccountId == tbl.AccountId &&
	//		item.DatabaseId == tbl.DatabaseId && item.Name == tbl.Name {
	//		ts = item.Ts
	//		tableId = item.Id
	//		fmt.Println("GetTable x", item.deleted, item.DatabaseId, item.Name, item.Ts, item.Id, item.Rowid)
	//		return true
	//	}
	//	if !item.deleted && item.AccountId == tbl.AccountId &&
	//		item.DatabaseId == tbl.DatabaseId && item.Name == tbl.Name &&
	//		(ts.IsEmpty() || ts.Equal(item.Ts) && tableId != item.Id) {
	//		fmt.Println("GetTable y", item.deleted, item.DatabaseId, item.Name, item.Ts, item.Id)
	//		find = true
	//		tbl.Id = item.Id
	//		tbl.Defs = item.Defs
	//		tbl.Kind = item.Kind
	//		tbl.Comment = item.Comment
	//		tbl.ViewDef = item.ViewDef
	//		tbl.TableDef = item.TableDef
	//		tbl.Constraint = item.Constraint
	//		tbl.Partitioned = item.Partitioned
	//		tbl.Partition = item.Partition
	//		tbl.CreateSql = item.CreateSql
	//		tbl.PrimaryIdx = item.PrimaryIdx
	//		tbl.ClusterByIdx = item.ClusterByIdx
	//		copy(tbl.Rowid[:], item.Rowid[:])
	//		tbl.Rowids = make([]types.Rowid, len(item.Rowids))
	//		for i, rowid := range item.Rowids {
	//			copy(tbl.Rowids[i][:], rowid[:])
	//		}
	//	}
	//	if find {
	//		return false
	//	}
	//	//the table is deleted already
	//	if !ts.IsEmpty() {
	//		return true
	//	}
	//	return false //........
	//})
	//return find
}

func (cc *CatalogCache) GetDatabase(db *DatabaseItem) bool {
	var find bool

	cc.databases.data.Ascend(db, func(item *DatabaseItem) bool {
		if !item.deleted && item.AccountId == db.AccountId &&
			item.Name == db.Name {
			find = true
			db.Id = item.Id
			db.CreateSql = item.CreateSql
			db.Typ = item.Typ
		}
		return false
	})
	return find
}

func (cc *CatalogCache) DeleteTable(bat *batch.Batch) {
	rowids := vector.MustFixedCol[types.Rowid](bat.GetVector(MO_ROWID_IDX))
	timestamps := vector.MustFixedCol[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))
	for i, rowid := range rowids {
		fmt.Println("deleteTable", rowid, timestamps[i].ToTimestamp())
		if item, ok := cc.tables.rowidIndex.Get(&TableItem{Rowid: rowid}); ok {
			newItem := &TableItem{
				deleted:    true,
				Id:         item.Id,
				Name:       item.Name,
				Rowid:      item.Rowid,
				AccountId:  item.AccountId,
				DatabaseId: item.DatabaseId,
				Ts:         timestamps[i].ToTimestamp(),
			}
			cc.tables.data.Set(newItem)
			fmt.Println("deleteTable", newItem.deleted, "acc", newItem.AccountId, "db", newItem.DatabaseId, newItem.Name, newItem.Id, newItem.Ts, newItem.Rowid)
		}
	}
}

func (cc *CatalogCache) DeleteDatabase(bat *batch.Batch) {
	rowids := vector.MustFixedCol[types.Rowid](bat.GetVector(MO_ROWID_IDX))
	timestamps := vector.MustFixedCol[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))
	for i, rowid := range rowids {
		if item, ok := cc.databases.rowidIndex.Get(&DatabaseItem{Rowid: rowid}); ok {
			newItem := &DatabaseItem{
				deleted:   true,
				Id:        item.Id,
				Name:      item.Name,
				Rowid:     item.Rowid,
				AccountId: item.AccountId,
				Typ:       item.Typ,
				CreateSql: item.CreateSql,
				Ts:        timestamps[i].ToTimestamp(),
			}
			cc.databases.data.Set(newItem)
		}
	}
}

func (cc *CatalogCache) InsertTable(bat *batch.Batch) {
	rowids := vector.MustFixedCol[types.Rowid](bat.GetVector(MO_ROWID_IDX))
	timestamps := vector.MustFixedCol[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))
	accounts := vector.MustFixedCol[uint32](bat.GetVector(catalog.MO_TABLES_ACCOUNT_ID_IDX + MO_OFF))
	names := vector.MustStrCol(bat.GetVector(catalog.MO_TABLES_REL_NAME_IDX + MO_OFF))
	ids := vector.MustFixedCol[uint64](bat.GetVector(catalog.MO_TABLES_REL_ID_IDX + MO_OFF))
	databaseIds := vector.MustFixedCol[uint64](bat.GetVector(catalog.MO_TABLES_RELDATABASE_ID_IDX + MO_OFF))
	kinds := vector.MustStrCol(bat.GetVector(catalog.MO_TABLES_RELKIND_IDX + MO_OFF))
	comments := vector.MustStrCol(bat.GetVector(catalog.MO_TABLES_REL_COMMENT_IDX + MO_OFF))
	createSqls := vector.MustStrCol(bat.GetVector(catalog.MO_TABLES_REL_CREATESQL_IDX + MO_OFF))
	viewDefs := vector.MustStrCol(bat.GetVector(catalog.MO_TABLES_VIEWDEF_IDX + MO_OFF))
	partitioneds := vector.MustFixedCol[int8](bat.GetVector(catalog.MO_TABLES_PARTITIONED_IDX + MO_OFF))
	paritions := vector.MustStrCol(bat.GetVector(catalog.MO_TABLES_PARTITION_INFO_IDX + MO_OFF))
	constraints := vector.MustBytesCol(bat.GetVector(catalog.MO_TABLES_CONSTRAINT_IDX + MO_OFF))

	for i, account := range accounts {
		item := new(TableItem)
		item.Id = ids[i]
		item.Name = names[i]
		item.AccountId = account
		item.DatabaseId = databaseIds[i]
		item.Ts = timestamps[i].ToTimestamp()
		item.Kind = kinds[i]
		item.ViewDef = viewDefs[i]
		item.Constraint = constraints[i]
		item.Comment = comments[i]
		item.Partitioned = partitioneds[i]
		item.Partition = paritions[i]
		item.CreateSql = createSqls[i]
		item.PrimaryIdx = -1
		item.ClusterByIdx = -1
		copy(item.Rowid[:], rowids[i][:])
		cc.tables.data.Set(item)
		cc.tables.rowidIndex.Set(item)
		fmt.Println("insertTable", "acc", item.AccountId, "db", item.DatabaseId, item.Name, item.Id, item.Ts, item.Rowid)
	}

	for i, _ := range accounts {
		cc.tables.data.Scan(func(item *TableItem) bool {
			if item.Name == names[i] {
				fmt.Println("insertTable-Scan ", item.deleted, "acc", item.AccountId, "db", item.DatabaseId, item.Name, item.Id, item.Ts, item.Rowid)
			}
			return true
		})
	}
}

func (cc *CatalogCache) InsertColumns(bat *batch.Batch) {
	var tblKey tableItemKey

	mp := make(map[tableItemKey]columns) // TableItem -> columns
	key := new(TableItem)
	rowids := vector.MustFixedCol[types.Rowid](bat.GetVector(MO_ROWID_IDX))
	// get table key info
	timestamps := vector.MustFixedCol[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))
	accounts := vector.MustFixedCol[uint32](bat.GetVector(catalog.MO_COLUMNS_ACCOUNT_ID_IDX + MO_OFF))
	databaseIds := vector.MustFixedCol[uint64](bat.GetVector(catalog.MO_COLUMNS_ATT_DATABASE_ID_IDX + MO_OFF))
	tableNames := vector.MustStrCol(bat.GetVector(catalog.MO_COLUMNS_ATT_RELNAME_IDX + MO_OFF))
	tableIds := vector.MustFixedCol[uint64](bat.GetVector(catalog.MO_COLUMNS_ATT_RELNAME_ID_IDX + MO_OFF))
	// get columns info
	names := vector.MustStrCol(bat.GetVector(catalog.MO_COLUMNS_ATTNAME_IDX + MO_OFF))
	comments := vector.MustStrCol(bat.GetVector(catalog.MO_COLUMNS_ATT_COMMENT_IDX + MO_OFF))
	isHiddens := vector.MustFixedCol[int8](bat.GetVector(catalog.MO_COLUMNS_ATT_IS_HIDDEN_IDX + MO_OFF))
	isAutos := vector.MustFixedCol[int8](bat.GetVector(catalog.MO_COLUMNS_ATT_IS_AUTO_INCREMENT_IDX + MO_OFF))
	constraintTypes := vector.MustStrCol(bat.GetVector(catalog.MO_COLUMNS_ATT_CONSTRAINT_TYPE_IDX + MO_OFF))
	typs := vector.MustBytesCol(bat.GetVector(catalog.MO_COLUMNS_ATTTYP_IDX + MO_OFF))
	hasDefs := vector.MustFixedCol[int8](bat.GetVector(catalog.MO_COLUMNS_ATTHASDEF_IDX + MO_OFF))
	defaultExprs := vector.MustBytesCol(bat.GetVector(catalog.MO_COLUMNS_ATT_DEFAULT_IDX + MO_OFF))
	hasUpdates := vector.MustFixedCol[int8](bat.GetVector(catalog.MO_COLUMNS_ATT_HAS_UPDATE_IDX + MO_OFF))
	updateExprs := vector.MustBytesCol(bat.GetVector(catalog.MO_COLUMNS_ATT_UPDATE_IDX + MO_OFF))
	nums := vector.MustFixedCol[int32](bat.GetVector(catalog.MO_COLUMNS_ATTNUM_IDX + MO_OFF))
	clusters := vector.MustFixedCol[int8](bat.GetVector(catalog.MO_COLUMNS_ATT_IS_CLUSTERBY + MO_OFF))
	for i, account := range accounts {
		key.AccountId = account
		key.Name = tableNames[i]
		key.DatabaseId = databaseIds[i]
		key.Ts = timestamps[i].ToTimestamp()
		key.Id = tableIds[i]
		tblKey.Name = key.Name
		tblKey.AccountId = key.AccountId
		tblKey.DatabaseId = key.DatabaseId
		tblKey.NodeId = key.Ts.NodeID
		tblKey.LogicalTime = key.Ts.LogicalTime
		tblKey.PhysicalTime = uint64(key.Ts.PhysicalTime)
		tblKey.Id = tableIds[i]
		if _, ok := cc.tables.data.Get(key); ok {
			col := column{
				num:             nums[i],
				name:            names[i],
				comment:         comments[i],
				isHidden:        isHiddens[i],
				isAutoIncrement: isAutos[i],
				hasDef:          hasDefs[i],
				hasUpdate:       hasUpdates[i],
				constraintType:  constraintTypes[i],
				isClusterBy:     clusters[i],
			}
			copy(col.rowid[:], rowids[i][:])
			col.typ = append(col.typ, typs[i]...)
			col.updateExpr = append(col.updateExpr, updateExprs[i]...)
			col.defaultExpr = append(col.defaultExpr, defaultExprs[i]...)
			mp[tblKey] = append(mp[tblKey], col)
		}
	}
	for k, cols := range mp {
		sort.Sort(cols)
		key.Name = k.Name
		key.AccountId = k.AccountId
		key.DatabaseId = k.DatabaseId
		key.Ts = timestamp.Timestamp{
			NodeID:       k.NodeId,
			PhysicalTime: int64(k.PhysicalTime),
			LogicalTime:  k.LogicalTime,
		}
		key.Id = k.Id
		item, _ := cc.tables.data.Get(key)
		defs := make([]engine.TableDef, 0, len(cols))
		defs = append(defs, genTableDefOfComment(item.Comment))
		item.Rowids = make([]types.Rowid, len(cols))
		for i, col := range cols {
			if col.constraintType == catalog.SystemColPKConstraint {
				item.PrimaryIdx = i
			}
			if col.isClusterBy == 1 {
				item.ClusterByIdx = i
			}
			defs = append(defs, genTableDefOfColumn(col))
			copy(item.Rowids[i][:], col.rowid[:])
		}
		item.Defs = defs
		item.TableDef = getTableDef(item.Name, defs)
	}
}

func (cc *CatalogCache) InsertDatabase(bat *batch.Batch) {
	rowids := vector.MustFixedCol[types.Rowid](bat.GetVector(MO_ROWID_IDX))
	timestamps := vector.MustFixedCol[types.TS](bat.GetVector(MO_TIMESTAMP_IDX))
	accounts := vector.MustFixedCol[uint32](bat.GetVector(catalog.MO_DATABASE_ACCOUNT_ID_IDX + MO_OFF))
	names := vector.MustStrCol(bat.GetVector(catalog.MO_DATABASE_DAT_NAME_IDX + MO_OFF))
	ids := vector.MustFixedCol[uint64](bat.GetVector(catalog.MO_DATABASE_DAT_ID_IDX + MO_OFF))
	typs := vector.MustStrCol(bat.GetVector(catalog.MO_DATABASE_DAT_TYPE_IDX + MO_OFF))
	createSqls := vector.MustStrCol(bat.GetVector(catalog.MO_DATABASE_CREATESQL_IDX + MO_OFF))
	for i, account := range accounts {
		item := new(DatabaseItem)
		item.Id = ids[i]
		item.Name = names[i]
		item.AccountId = account
		item.Ts = timestamps[i].ToTimestamp()
		item.Typ = typs[i]
		item.CreateSql = createSqls[i]
		copy(item.Rowid[:], rowids[i][:])
		cc.databases.data.Set(item)
		cc.databases.rowidIndex.Set(item)
	}
}

func genTableDefOfComment(comment string) engine.TableDef {
	return &engine.CommentDef{
		Comment: comment,
	}
}

func genTableDefOfColumn(col column) engine.TableDef {
	var attr engine.Attribute

	attr.Name = col.name
	attr.ID = uint64(col.num)
	attr.Alg = compress.Lz4
	attr.Comment = col.comment
	attr.IsHidden = col.isHidden == 1
	attr.ClusterBy = col.isClusterBy == 1
	attr.AutoIncrement = col.isAutoIncrement == 1
	if err := types.Decode(col.typ, &attr.Type); err != nil {
		panic(err)
	}
	attr.Default = new(plan.Default)
	if col.hasDef == 1 {
		if err := types.Decode(col.defaultExpr, attr.Default); err != nil {
			panic(err)
		}
	}
	if col.hasUpdate == 1 {
		attr.OnUpdate = new(plan.OnUpdate)
		if err := types.Decode(col.updateExpr, attr.OnUpdate); err != nil {
			panic(err)
		}
	}
	if col.constraintType == catalog.SystemColPKConstraint {
		attr.Primary = true
	}
	return &engine.AttributeDef{Attr: attr}
}

// getTableDef only return all cols and their index.
func getTableDef(name string, defs []engine.TableDef) *plan.TableDef {
	var cols []*plan.ColDef

	i := int32(0)
	name2index := make(map[string]int32)
	for _, def := range defs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			name2index[attr.Attr.Name] = i
			cols = append(cols, &plan.ColDef{
				ColId: attr.Attr.ID,
				Name:  attr.Attr.Name,
				Typ: &plan.Type{
					Id:       int32(attr.Attr.Type.Oid),
					Width:    attr.Attr.Type.Width,
					Scale:    attr.Attr.Type.Scale,
					AutoIncr: attr.Attr.AutoIncrement,
				},
				Primary:  attr.Attr.Primary,
				Default:  attr.Attr.Default,
				OnUpdate: attr.Attr.OnUpdate,
				Comment:  attr.Attr.Comment,
				Hidden:   attr.Attr.IsHidden,
			})
			i++
		}
	}
	return &plan.TableDef{
		Name:          name,
		Cols:          cols,
		Name2ColIndex: name2index,
	}
}
