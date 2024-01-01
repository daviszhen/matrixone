// Copyright 2023 Matrix Origin
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
	"fmt"
	"os"
	gotrace "runtime/trace"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"go.uber.org/zap"
)

/*
extract the data from the pipeline.
obj: session
Warning: The pipeline is the multi-thread environment. The getDataFromPipeline will
access the shared data. Be careful when it writes the shared data.
*/
func getDataFromPipeline2(obj interface{}, bat *batch.Batch) (err error) {
	_, task := gotrace.NewTask(context.TODO(), "frontend.WriteDataToClient")
	defer task.End()
	ses := obj.(*Session)
	if bat == nil {
		return nil
	}

	err = ses.formatWriter.Write(ses.GetRequestContext(), bat)
	if err != nil {
		return err
	}
	for _, writer := range ses.extraWriters {
		err = writer.Write(ses.GetRequestContext(), bat)
		if err != nil {
			return err
		}
	}
	return err
}

// extractRowFromEveryVector gets the j row from the every vector and outputs the row
// needCopyBytes : true -- make a copy of the bytes. else not.
// Case 1: needCopyBytes = false.
// For responding the client, we do not make a copy of the bytes. Because the data
// has been written into the tcp conn before the batch.Batch returned to the pipeline.
// Case 2: needCopyBytes = true.
// For the background execution, we need to make a copy of the bytes. Because the data
// has been saved in the session. Later the data will be used but then the batch.Batch has
// been returned to the pipeline and may be reused and changed by the pipeline.
func extractRowFromEveryVector2(ses *Session, dataSet *batch.Batch, j int, row []any, needCopyBytes bool) (err error) {
	var rowIndex = j
	for i, vec := range dataSet.Vecs { //col index
		rowIndexBackup := rowIndex
		if vec.IsConstNull() {
			row[i] = nil
			continue
		}
		if vec.IsConst() {
			rowIndex = 0
		}

		err = extractRowFromVector2(ses, vec, i, row, rowIndex, needCopyBytes)
		if err != nil {
			return err
		}
		rowIndex = rowIndexBackup
	}
	return nil
}

// extractRowFromVector gets the rowIndex row from the i vector
func extractRowFromVector2(ses *Session, vec *vector.Vector, i int, row []interface{}, rowIndex int, needCopyBytes bool) error {
	if vec.IsConstNull() || vec.GetNulls().Contains(uint64(rowIndex)) {
		row[i] = nil
		return nil
	}

	switch vec.GetType().Oid { //get col
	case types.T_json:
		row[i] = types.DecodeJson(copyBytes(vec.GetBytesAt(rowIndex), needCopyBytes))
	case types.T_bool:
		row[i] = vector.GetFixedAt[bool](vec, rowIndex)
	case types.T_int8:
		row[i] = vector.GetFixedAt[int8](vec, rowIndex)
	case types.T_uint8:
		row[i] = vector.GetFixedAt[uint8](vec, rowIndex)
	case types.T_int16:
		row[i] = vector.GetFixedAt[int16](vec, rowIndex)
	case types.T_uint16:
		row[i] = vector.GetFixedAt[uint16](vec, rowIndex)
	case types.T_int32:
		row[i] = vector.GetFixedAt[int32](vec, rowIndex)
	case types.T_uint32:
		row[i] = vector.GetFixedAt[uint32](vec, rowIndex)
	case types.T_int64:
		row[i] = vector.GetFixedAt[int64](vec, rowIndex)
	case types.T_uint64:
		row[i] = vector.GetFixedAt[uint64](vec, rowIndex)
	case types.T_float32:
		row[i] = vector.GetFixedAt[float32](vec, rowIndex)
	case types.T_float64:
		row[i] = vector.GetFixedAt[float64](vec, rowIndex)
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary:
		row[i] = copyBytes(vec.GetBytesAt(rowIndex), needCopyBytes)
	case types.T_array_float32:
		// NOTE: Don't merge it with T_varchar. You will get raw binary in the SQL output
		//+------------------------------+
		//| abs(cast([1,2,3] as vecf32)) |
		//+------------------------------+
		//|   �?   @  @@                  |
		//+------------------------------+
		row[i] = vector.GetArrayAt[float32](vec, rowIndex)
	case types.T_array_float64:
		row[i] = vector.GetArrayAt[float64](vec, rowIndex)
	case types.T_date:
		row[i] = vector.GetFixedAt[types.Date](vec, rowIndex)
	case types.T_datetime:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAt[types.Datetime](vec, rowIndex).String2(scale)
	case types.T_time:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAt[types.Time](vec, rowIndex).String2(scale)
	case types.T_timestamp:
		scale := vec.GetType().Scale
		timeZone := ses.GetTimeZone()
		row[i] = vector.GetFixedAt[types.Timestamp](vec, rowIndex).String2(timeZone, scale)
	case types.T_decimal64:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAt[types.Decimal64](vec, rowIndex).Format(scale)
	case types.T_decimal128:
		scale := vec.GetType().Scale
		row[i] = vector.GetFixedAt[types.Decimal128](vec, rowIndex).Format(scale)
	case types.T_uuid:
		row[i] = vector.GetFixedAt[types.Uuid](vec, rowIndex).ToString()
	case types.T_Rowid:
		row[i] = vector.GetFixedAt[types.Rowid](vec, rowIndex)
	case types.T_Blockid:
		row[i] = vector.GetFixedAt[types.Blockid](vec, rowIndex)
	case types.T_TS:
		row[i] = vector.GetFixedAt[types.TS](vec, rowIndex)
	case types.T_enum:
		row[i] = copyBytes(vec.GetBytesAt(rowIndex), needCopyBytes)
	default:
		logError(ses, ses.GetDebugString(),
			"Failed to extract row from vector, unsupported type",
			zap.Int("typeID", int(vec.GetType().Oid)))
		return moerr.NewInternalError(ses.GetRequestContext(), "extractRowFromVector : unsupported type %d", vec.GetType().Oid)
	}
	return nil
}

func (chunkw *ChunksWriter) Open(context.Context, ...WriterOpt) error { return nil }
func (chunkw *ChunksWriter) Write(ctx context.Context, chunks Chunks) (err error) {
	//column -> rows
	n := chunks.Vecs[0].Length()
	fmt.Fprintf(os.Stderr, "row count %d\n", n)
	for j := 0; j < n; j++ { //row index
		//fill row
		err = extractRowFromEveryVector2(chunkw.ses, chunks, j, chunkw.row, false)
		if err != nil {
			return err
		}
		//write row
		err = chunkw.writeRow(ctx, chunkw.row)
		if err != nil {
			return err
		}
	}
	return err
}
func (chunkw *ChunksWriter) WriteBytes(context.Context, []byte) error { return nil }
func (chunkw *ChunksWriter) Flush(context.Context) error {
	return nil
}
func (chunkw *ChunksWriter) Close(context.Context) error {
	chunkw.ses = nil
	chunkw.row = nil
	chunkw.writeRow = nil
	return nil
}

func (format *MysqlFormatWriter) Open(ctx context.Context, opts ...WriterOpt) error {
	format.strconvBuffer = make([]byte, 0, 16*1024)
	format.lenEncBuffer = make([]byte, 0, 10)
	format.binaryNullBuffer = make([]byte, 0, 512)
	if format.ChunksWriter == nil {
		format.ChunksWriter = &ChunksWriter{
			row: make([]any, len(format.colDef)),
		}
	}
	format.ChunksWriter.writeRow = format.writeRow
	format.ChunksWriter.Open(ctx, opts...)
	//init row
	format.textRow = &ResultSetRowText{
		colDef:        format.colDef,
		lenEncBuffer:  format.lenEncBuffer,
		strconvBuffer: format.strconvBuffer,
	}
	format.binRow = &ResultSetRowBinary{
		colDef:           format.colDef,
		lenEncBuffer:     format.lenEncBuffer,
		strconvBuffer:    format.strconvBuffer,
		binaryNullBuffer: format.binaryNullBuffer,
	}

	return nil
}

func (format *MysqlFormatWriter) writeRow(ctx context.Context, row []any) (err error) {
	if format.isBin {
		format.binRow.colData = row
		format.binRow.colDef = format.colDef
		format.binRow.lenEncBuffer = format.lenEncBuffer
		format.binRow.strconvBuffer = format.strconvBuffer
		format.binRow.binaryNullBuffer = format.binaryNullBuffer
		format.binRow.Open(ctx)
		defer format.binRow.Close(ctx)
		err = format.endPoint.SendPacket(ctx, format.binRow, true)
		if err != nil {
			return err
		}
	} else {
		format.textRow.colData = row
		format.textRow.colDef = format.colDef
		format.textRow.lenEncBuffer = format.lenEncBuffer
		format.textRow.strconvBuffer = format.strconvBuffer
		format.textRow.Open(ctx)
		defer format.textRow.Close(ctx)
		err = format.endPoint.SendPacket(ctx, format.textRow, true)
		if err != nil {
			return err
		}
	}
	return err
}

func (format *MysqlFormatWriter) Write(ctx context.Context, chunks Chunks) (err error) {
	return format.ChunksWriter.Write(ctx, chunks)
}
func (format *MysqlFormatWriter) WriteBytes(context.Context, []byte) error {
	return nil
}
func (format *MysqlFormatWriter) Flush(context.Context) error {
	return nil
}
func (format *MysqlFormatWriter) Close(ctx context.Context) error {
	format.ChunksWriter.Close(ctx)
	format.textRow = nil
	format.binRow = nil
	format.colDef = nil
	format.lenEncBuffer = nil
	format.strconvBuffer = nil
	format.binaryNullBuffer = nil
	return nil
}
