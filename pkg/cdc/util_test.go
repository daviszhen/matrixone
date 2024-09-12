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

package cdc

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_aes(t *testing.T) {
	data := "test ase"
	encData, err := aesCFBEncode([]byte(data), []byte(aesKey))
	assert.NoError(t, err)
	decData, err := aesCFBDecode(context.Background(), encData, []byte(aesKey))
	assert.NoError(t, err)
	assert.Equal(t, data, decData)
}

func TestAesCFBDecode(t *testing.T) {
	type args struct {
		ctx  context.Context
		data string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AesCFBDecode(tt.args.ctx, tt.args.data)
			if !tt.wantErr(t, err, fmt.Sprintf("AesCFBDecode(%v, %v)", tt.args.ctx, tt.args.data)) {
				return
			}
			assert.Equalf(t, tt.want, got, "AesCFBDecode(%v, %v)", tt.args.ctx, tt.args.data)
		})
	}
}

func TestAesCFBEncode(t *testing.T) {
	type args struct {
		data []byte
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AesCFBEncode(tt.args.data)
			if !tt.wantErr(t, err, fmt.Sprintf("AesCFBEncode(%v)", tt.args.data)) {
				return
			}
			assert.Equalf(t, tt.want, got, "AesCFBEncode(%v)", tt.args.data)
		})
	}
}

func TestGetTableDef(t *testing.T) {
	type args struct {
		ctx      context.Context
		txnOp    client.TxnOperator
		cnEngine engine.Engine
		tblId    uint64
	}
	tests := []struct {
		name    string
		args    args
		want    *plan.TableDef
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetTableDef(tt.args.ctx, tt.args.txnOp, tt.args.cnEngine, tt.args.tblId)
			if !tt.wantErr(t, err, fmt.Sprintf("GetTableDef(%v, %v, %v, %v)", tt.args.ctx, tt.args.txnOp, tt.args.cnEngine, tt.args.tblId)) {
				return
			}
			assert.Equalf(t, tt.want, got, "GetTableDef(%v, %v, %v, %v)", tt.args.ctx, tt.args.txnOp, tt.args.cnEngine, tt.args.tblId)
		})
	}
}

func Test_aesCFBDecode(t *testing.T) {
	type args struct {
		ctx    context.Context
		data   string
		aesKey []byte
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := aesCFBDecode(tt.args.ctx, tt.args.data, tt.args.aesKey)
			if !tt.wantErr(t, err, fmt.Sprintf("aesCFBDecode(%v, %v, %v)", tt.args.ctx, tt.args.data, tt.args.aesKey)) {
				return
			}
			assert.Equalf(t, tt.want, got, "aesCFBDecode(%v, %v, %v)", tt.args.ctx, tt.args.data, tt.args.aesKey)
		})
	}
}

func Test_aesCFBEncode(t *testing.T) {
	type args struct {
		data   []byte
		aesKey []byte
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := aesCFBEncode(tt.args.data, tt.args.aesKey)
			if !tt.wantErr(t, err, fmt.Sprintf("aesCFBEncode(%v, %v)", tt.args.data, tt.args.aesKey)) {
				return
			}
			assert.Equalf(t, tt.want, got, "aesCFBEncode(%v, %v)", tt.args.data, tt.args.aesKey)
		})
	}
}

func Test_appendByte(t *testing.T) {
	type args struct {
		buf []byte
		d   byte
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			args: args{buf: []byte{}, d: 'a'},
			want: []byte{'a'},
		},
		{
			args: args{buf: []byte{'a'}, d: 'b'},
			want: []byte{'a', 'b'},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, appendByte(tt.args.buf, tt.args.d), "appendByte(%v, %v)", tt.args.buf, tt.args.d)
		})
	}
}

func Test_appendBytes(t *testing.T) {
	type args struct {
		buf  []byte
		data []byte
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			args: args{buf: []byte{}, data: []byte{}},
			want: []byte{},
		},
		{
			args: args{buf: []byte{}, data: []byte{'a', 'b', 'c'}},
			want: []byte{'a', 'b', 'c'},
		},
		{
			args: args{buf: []byte{'a', 'b'}, data: []byte{'c', 'd'}},
			want: []byte{'a', 'b', 'c', 'd'},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, appendBytes(tt.args.buf, tt.args.data), "appendBytes(%v, %v)", tt.args.buf, tt.args.data)
		})
	}
}

func Test_appendFloat64(t *testing.T) {
	type args struct {
		buf     []byte
		value   float64
		bitSize int
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			args: args{buf: []byte{}, value: 1.1, bitSize: 64},
			want: []byte("1.1"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, appendFloat64(tt.args.buf, tt.args.value, tt.args.bitSize), "appendFloat64(%v, %v, %v)", tt.args.buf, tt.args.value, tt.args.bitSize)
		})
	}
}

func Test_appendInt64(t *testing.T) {
	type args struct {
		buf   []byte
		value int64
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			args: args{buf: []byte{}, value: 1},
			want: []byte{'1'},
		},
		{
			args: args{buf: []byte{1}, value: math.MaxInt64},
			want: []byte{1, '9', '2', '2', '3', '3', '7', '2', '0', '3', '6', '8', '5', '4', '7', '7', '5', '8', '0', '7'},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, appendInt64(tt.args.buf, tt.args.value), "appendInt64(%v, %v)", tt.args.buf, tt.args.value)
		})
	}
}

func Test_appendString(t *testing.T) {
	type args struct {
		buf []byte
		s   string
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			args: args{buf: []byte{}, s: "test"},
			want: []byte{116, 101, 115, 116},
		},
		{
			args: args{buf: []byte{116, 101, 115, 116}, s: "test"},
			want: []byte{116, 101, 115, 116, 116, 101, 115, 116},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, appendString(tt.args.buf, tt.args.s), "appendString(%v, %v)", tt.args.buf, tt.args.s)
		})
	}
}

func Test_appendUint64(t *testing.T) {
	type args struct {
		buf   []byte
		value uint64
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			args: args{buf: []byte{}, value: 1},
			want: []byte("1"),
		},
		{
			args: args{buf: []byte{}, value: math.MaxUint64},
			want: []byte("18446744073709551615"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, appendUint64(tt.args.buf, tt.args.value), "appendUint64(%v, %v)", tt.args.buf, tt.args.value)
		})
	}
}

func Test_convertColIntoSql(t *testing.T) {
	bj, err := bytejson.ParseFromString("{\"a\": 1}")
	require.Nil(t, err)

	date, err := types.ParseDateCast("2023-02-03")
	require.Nil(t, err)

	rowid := types.BuildTestRowid(1234, 5678)
	blockid := types.BuildTestBlockid(1234, 5678)

	ts := types.BuildTS(1234, 5678)

	type args struct {
		ctx     context.Context
		data    any
		typ     *types.Type
		sqlBuff []byte
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr assert.ErrorAssertionFunc
	}{
		{
			args:    args{ctx: context.Background(), data: nil, typ: &types.Type{Oid: types.T_int8}, sqlBuff: []byte{}},
			want:    []byte("NULL"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: bj, typ: &types.Type{Oid: types.T_json}, sqlBuff: []byte{}},
			want:    []byte("'{\"a\": 1}'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: false, typ: &types.Type{Oid: types.T_bool}, sqlBuff: []byte{}},
			want:    []byte("false"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: true, typ: &types.Type{Oid: types.T_bool}, sqlBuff: []byte{}},
			want:    []byte("true"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: uint64(1), typ: &types.Type{Oid: types.T_bit, Width: 5}, sqlBuff: []byte{}},
			want:    []byte{0x27, 0x1, 0x27},
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: int8(1), typ: &types.Type{Oid: types.T_int8}, sqlBuff: []byte{}},
			want:    []byte{'1'},
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: uint8(1), typ: &types.Type{Oid: types.T_uint8}, sqlBuff: []byte{}},
			want:    []byte{'1'},
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: int16(1), typ: &types.Type{Oid: types.T_int16}, sqlBuff: []byte{}},
			want:    []byte{'1'},
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: uint16(1), typ: &types.Type{Oid: types.T_uint16}, sqlBuff: []byte{}},
			want:    []byte{'1'},
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: int32(1), typ: &types.Type{Oid: types.T_int32}, sqlBuff: []byte{}},
			want:    []byte{'1'},
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: uint32(1), typ: &types.Type{Oid: types.T_uint32}, sqlBuff: []byte{}},
			want:    []byte{'1'},
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: int64(1), typ: &types.Type{Oid: types.T_int64}, sqlBuff: []byte{}},
			want:    []byte{'1'},
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: uint64(1), typ: &types.Type{Oid: types.T_uint64}, sqlBuff: []byte{}},
			want:    []byte{'1'},
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: float32(1.1), typ: &types.Type{Oid: types.T_float32}, sqlBuff: []byte{}},
			want:    []byte("1.1"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: 1.1, typ: &types.Type{Oid: types.T_float64}, sqlBuff: []byte{}},
			want:    []byte("1.1"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []byte("test"), typ: &types.Type{Oid: types.T_char}, sqlBuff: []byte{}},
			want:    []byte("'test'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []byte("test"), typ: &types.Type{Oid: types.T_varchar}, sqlBuff: []byte{}},
			want:    []byte("'test'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []byte("test"), typ: &types.Type{Oid: types.T_blob}, sqlBuff: []byte{}},
			want:    []byte("'test'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []byte("test"), typ: &types.Type{Oid: types.T_text}, sqlBuff: []byte{}},
			want:    []byte("'test'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []byte("test"), typ: &types.Type{Oid: types.T_binary}, sqlBuff: []byte{}},
			want:    []byte("'test'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []byte("test"), typ: &types.Type{Oid: types.T_varbinary}, sqlBuff: []byte{}},
			want:    []byte("'test'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []byte("test"), typ: &types.Type{Oid: types.T_datalink}, sqlBuff: []byte{}},
			want:    []byte("'test'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []float32{1.1, 2.2, 3.3}, typ: &types.Type{Oid: types.T_array_float32}, sqlBuff: []byte{}},
			want:    []byte("'[1.100000,2.200000,3.300000]'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []float64{1.1, 2.2, 3.3}, typ: &types.Type{Oid: types.T_array_float64}, sqlBuff: []byte{}},
			want:    []byte("'[1.100000,2.200000,3.300000]'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: []float64{1.1, 2.2, 3.3}, typ: &types.Type{Oid: types.T_array_float64}, sqlBuff: []byte{}},
			want:    []byte("'[1.100000,2.200000,3.300000]'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: date, typ: &types.Type{Oid: types.T_date}, sqlBuff: []byte{}},
			want:    []byte("'2023-02-03'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: "2023-02-03 01:23:45", typ: &types.Type{Oid: types.T_datetime}, sqlBuff: []byte{}},
			want:    []byte("'2023-02-03 01:23:45'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: "01:23:45", typ: &types.Type{Oid: types.T_time}, sqlBuff: []byte{}},
			want:    []byte("'01:23:45'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: "2023-02-03 01:23:45", typ: &types.Type{Oid: types.T_datetime}, sqlBuff: []byte{}},
			want:    []byte("'2023-02-03 01:23:45'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: "1.1", typ: &types.Type{Oid: types.T_decimal64}, sqlBuff: []byte{}},
			want:    []byte("'1.1'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: "1.1", typ: &types.Type{Oid: types.T_decimal128}, sqlBuff: []byte{}},
			want:    []byte("'1.1'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: "1.1", typ: &types.Type{Oid: types.T_uuid}, sqlBuff: []byte{}},
			want:    []byte("'1.1'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: rowid, typ: &types.Type{Oid: types.T_Rowid}, sqlBuff: []byte{}},
			want:    []byte("'d2040000-0000-0000-2e16-000000000000-0-0-5678'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: blockid, typ: &types.Type{Oid: types.T_Blockid}, sqlBuff: []byte{}},
			want:    []byte("'d2040000-0000-0000-2e16-000000000000-0-0'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: ts, typ: &types.Type{Oid: types.T_TS}, sqlBuff: []byte{}},
			want:    []byte("'1234-5678'"),
			wantErr: assert.NoError,
		},
		{
			args:    args{ctx: context.Background(), data: types.Enum(1), typ: &types.Type{Oid: types.T_enum}, sqlBuff: []byte{}},
			want:    []byte("'1'"),
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertColIntoSql(tt.args.ctx, tt.args.data, tt.args.typ, tt.args.sqlBuff)
			if !tt.wantErr(t, err, fmt.Sprintf("convertColIntoSql(%v, %v, %v, %v)", tt.args.ctx, tt.args.data, tt.args.typ, tt.args.sqlBuff)) {
				return
			}
			assert.Equalf(t, tt.want, got, "convertColIntoSql(%v, %v, %v, %v)", tt.args.ctx, tt.args.data, tt.args.typ, tt.args.sqlBuff)
		})
	}
}

func Test_copyBytes(t *testing.T) {
	type args struct {
		src []byte
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		{
			args: args{src: []byte{}},
			want: []byte{},
		},
		{
			args: args{src: []byte{'a', 'b', 'c'}},
			want: []byte{'a', 'b', 'c'},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, copyBytes(tt.args.src), "copyBytes(%v)", tt.args.src)
		})
	}
}

func Test_extractRowFromEveryVector(t *testing.T) {
	type args struct {
		ctx      context.Context
		dataSet  *batch.Batch
		rowIndex int
		row      []any
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, extractRowFromEveryVector(tt.args.ctx, tt.args.dataSet, tt.args.rowIndex, tt.args.row), fmt.Sprintf("extractRowFromEveryVector(%v, %v, %v, %v)", tt.args.ctx, tt.args.dataSet, tt.args.rowIndex, tt.args.row))
		})
	}
}

func Test_extractRowFromVector(t *testing.T) {
	type args struct {
		ctx      context.Context
		vec      *vector.Vector
		i        int
		row      []any
		rowIndex int
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, extractRowFromVector(tt.args.ctx, tt.args.vec, tt.args.i, tt.args.row, tt.args.rowIndex), fmt.Sprintf("extractRowFromVector(%v, %v, %v, %v, %v)", tt.args.ctx, tt.args.vec, tt.args.i, tt.args.row, tt.args.rowIndex))
		})
	}
}

func Test_floatArrayToString(t *testing.T) {
	type args[T interface{ float32 | float64 }] struct {
		arr []T
	}
	type testCase[T interface{ float32 | float64 }] struct {
		name string
		args args[T]
		want string
	}
	tests := []testCase[float32]{
		{
			args: args[float32]{arr: []float32{}},
			want: "'[]'",
		},
		{
			args: args[float32]{arr: []float32{1.1, 2.2, 3.3}},
			want: "'[1.100000,2.200000,3.300000]'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, floatArrayToString(tt.args.arr), "floatArrayToString(%v)", tt.args.arr)
		})
	}
}

func Test_generateSalt(t *testing.T) {
	type args struct {
		n int
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, generateSalt(tt.args.n), "generateSalt(%v)", tt.args.n)
		})
	}
}

func Test_openDbConn(t *testing.T) {
	type args struct {
		user     string
		password string
		ip       string
		port     int
	}
	tests := []struct {
		name    string
		args    args
		wantDb  *sql.DB
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDb, err := openDbConn(tt.args.user, tt.args.password, tt.args.ip, tt.args.port)
			if !tt.wantErr(t, err, fmt.Sprintf("openDbConn(%v, %v, %v, %v)", tt.args.user, tt.args.password, tt.args.ip, tt.args.port)) {
				return
			}
			assert.Equalf(t, tt.wantDb, gotDb, "openDbConn(%v, %v, %v, %v)", tt.args.user, tt.args.password, tt.args.ip, tt.args.port)
		})
	}
}

func Test_tryConn(t *testing.T) {
	type args struct {
		dsn string
	}
	tests := []struct {
		name    string
		args    args
		want    *sql.DB
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tryConn(tt.args.dsn)
			if !tt.wantErr(t, err, fmt.Sprintf("tryConn(%v)", tt.args.dsn)) {
				return
			}
			assert.Equalf(t, tt.want, got, "tryConn(%v)", tt.args.dsn)
		})
	}
}

func TestGetTxnOp(t *testing.T) {
	type args struct {
		ctx         context.Context
		cnEngine    engine.Engine
		cnTxnClient client.TxnClient
		info        string
	}
	tests := []struct {
		name    string
		args    args
		want    client.TxnOperator
		wantErr assert.ErrorAssertionFunc
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetTxnOp(tt.args.ctx, tt.args.cnEngine, tt.args.cnTxnClient, tt.args.info)
			if !tt.wantErr(t, err, fmt.Sprintf("GetTxnOp(%v, %v, %v, %v)", tt.args.ctx, tt.args.cnEngine, tt.args.cnTxnClient, tt.args.info)) {
				return
			}
			assert.Equalf(t, tt.want, got, "GetTxnOp(%v, %v, %v, %v)", tt.args.ctx, tt.args.cnEngine, tt.args.cnTxnClient, tt.args.info)
		})
	}
}

func TestFinishTxnOp(t *testing.T) {
	type args struct {
		ctx      context.Context
		inputErr error
		txnOp    client.TxnOperator
		cnEngine engine.Engine
	}
	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			FinishTxnOp(tt.args.ctx, tt.args.inputErr, tt.args.txnOp, tt.args.cnEngine)
		})
	}
}
