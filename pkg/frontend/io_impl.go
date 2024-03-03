// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
    "context"
    "github.com/matrixorigin/matrixone/pkg/common/moerr"
    "github.com/matrixorigin/matrixone/pkg/container/types"
    "github.com/matrixorigin/matrixone/pkg/defines"
    "math"
    "strings"
)

func (buffer *MysqlPayloadWriteBuffer) Open(ctx context.Context, opts *WriteBufferOptions) error {
    if opts != nil && opts.proto != nil {
        buffer.proto = opts.proto
    }
    return buffer.proto.openRow(nil)
}

func (buffer *MysqlPayloadWriteBuffer) Write(ctx context.Context, data []byte, options *WriteBufferOptions) error {
    return buffer.proto.fillPacket(data...)
}

func (buffer *MysqlPayloadWriteBuffer) Flush(ctx context.Context) error {
    return buffer.proto.flushOutBuffer()
}

func (buffer *MysqlPayloadWriteBuffer) Close(ctx context.Context) error {
    return buffer.proto.closeRow(nil)
}

func (text *ResultSetRowText) Open(ctx context.Context, options *MysqlWritePacketOptions) error {
    return nil
}

func (text *ResultSetRowText) Write(ctx context.Context, out WriteBuffer, options *MysqlWritePacketOptions) error {
    var err error
    if len(text.colDef) != len(text.colData) {
        return moerr.NewInternalError(ctx, "column count mismatch")
    }
    err = out.Open(ctx, nil)
    if err != nil {
        return err
    }

    err = text.writeImpl(ctx, out, options)
    if err != nil {
        return err
    }

    //output into outbuf
    err = out.Close(ctx)
    if err != nil {
        return err
    }
    return err
}

func (text *ResultSetRowText) writeImpl(ctx context.Context, out WriteBuffer, options *MysqlWritePacketOptions) (err error) {
    for i := uint64(0); i < uint64(len(text.colDef)); i++ {
        mysqlColumn := text.colDef[i]
        row := text.colData[i]

        if row == nil {
            //NULL is sent as 0xfb
            err = appendUint8(ctx, out, 0xFB)
            if err != nil {
                return err
            }
            continue
        }

        switch mysqlColumn.ColumnType() {
        case defines.MYSQL_TYPE_JSON:
            if value, err2 := getString(ctx, row); err2 != nil {
                return err2
            } else {
                err = appendStringLenEnc(ctx, out, text.lenEncBuffer, value)
                if err != nil {
                    return err
                }
            }
        case defines.MYSQL_TYPE_BOOL:
            if value, err2 := getString(ctx, row); err2 != nil {
                return err2
            } else {
                err = appendStringLenEnc(ctx, out, text.lenEncBuffer, value)
                if err != nil {
                    return err
                }
            }
        case defines.MYSQL_TYPE_DECIMAL:
            if value, err2 := getString(ctx, row); err2 != nil {
                return err2
            } else {
                err = appendStringLenEnc(ctx, out, text.lenEncBuffer, value)
                if err != nil {
                    return err
                }
            }
        case defines.MYSQL_TYPE_UUID:
            if value, err2 := getString(ctx, row); err2 != nil {
                return err2
            } else {
                err = appendStringLenEnc(ctx, out, text.lenEncBuffer, value)
                if err != nil {
                    return err
                }
            }
        case defines.MYSQL_TYPE_TINY, defines.MYSQL_TYPE_SHORT, defines.MYSQL_TYPE_INT24, defines.MYSQL_TYPE_LONG, defines.MYSQL_TYPE_YEAR:
            if value, err2 := getInt64(ctx, row); err2 != nil {
                return err2
            } else {
                if mysqlColumn.ColumnType() == defines.MYSQL_TYPE_YEAR {
                    if value == 0 {
                        err = appendStringLenEnc(ctx, out, text.lenEncBuffer, "0000")
                        if err != nil {
                            return err
                        }
                    } else {
                        err = appendStringLenEncOfInt64(ctx, out, text.lenEncBuffer, text.strconvBuffer, value)
                        if err != nil {
                            return err
                        }
                    }
                } else {
                    err = appendStringLenEncOfInt64(ctx, out, text.lenEncBuffer, text.strconvBuffer, value)
                    if err != nil {
                        return err
                    }
                }
            }
        case defines.MYSQL_TYPE_FLOAT:
            switch v := row.(type) {
            case float32:
                err = appendStringLenEncOfFloat64(ctx, out, text.lenEncBuffer, text.strconvBuffer, float64(v), 32)
                if err != nil {
                    return err
                }
            case float64:
                err = appendStringLenEncOfFloat64(ctx, out, text.lenEncBuffer, text.strconvBuffer, v, 32)
                if err != nil {
                    return err
                }
            default:
            }

        case defines.MYSQL_TYPE_DOUBLE:
            switch v := row.(type) {
            case float32:
                err = appendStringLenEncOfFloat64(ctx, out, text.lenEncBuffer, text.strconvBuffer, float64(v), 64)
                if err != nil {
                    return err
                }
            case float64:
                err = appendStringLenEncOfFloat64(ctx, out, text.lenEncBuffer, text.strconvBuffer, v, 64)
                if err != nil {
                    return err
                }
            default:
            }
        case defines.MYSQL_TYPE_LONGLONG:
            if uint32(mysqlColumn.Flag())&defines.UNSIGNED_FLAG != 0 {
                if value, err2 := getUint64(ctx, row); err2 != nil {
                    return err2
                } else {
                    err = appendStringLenEncOfUint64(ctx, out, text.lenEncBuffer, text.strconvBuffer, value)
                    if err != nil {
                        return err
                    }
                }
            } else {
                if value, err2 := getInt64(ctx, row); err2 != nil {
                    return err2
                } else {
                    err = appendStringLenEncOfInt64(ctx, out, text.lenEncBuffer, text.strconvBuffer, value)
                    if err != nil {
                        return err
                    }
                }
            }
        // Binary/varbinary will be sent out as varchar type.
        case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_STRING,
            defines.MYSQL_TYPE_BLOB, defines.MYSQL_TYPE_TEXT:
            if value, err2 := getString(ctx, row); err2 != nil {
                return err2
            } else {
                err = appendStringLenEnc(ctx, out, text.lenEncBuffer, value)
                if err != nil {
                    return err
                }
            }
        case defines.MYSQL_TYPE_DATE:
            err = appendStringLenEnc(ctx, out, text.lenEncBuffer, row.(types.Date).String())
            if err != nil {
                return err
            }
        case defines.MYSQL_TYPE_DATETIME:
            if value, err2 := getString(ctx, row); err2 != nil {
                return err2
            } else {
                err = appendStringLenEnc(ctx, out, text.lenEncBuffer, value)
                if err != nil {
                    return err
                }
            }
        case defines.MYSQL_TYPE_TIME:
            if value, err2 := getString(ctx, row); err2 != nil {
                return err2
            } else {
                err = appendStringLenEnc(ctx, out, text.lenEncBuffer, value)
                if err != nil {
                    return err
                }
            }
        case defines.MYSQL_TYPE_TIMESTAMP:
            if value, err2 := getString(ctx, row); err2 != nil {
                return err2
            } else {
                err = appendStringLenEnc(ctx, out, text.lenEncBuffer, value)
                if err != nil {
                    return err
                }
            }
        case defines.MYSQL_TYPE_ENUM:
            if value, err2 := getString(ctx, row); err2 != nil {
                return err2
            } else {
                err = appendStringLenEnc(ctx, out, text.lenEncBuffer, value)
                if err != nil {
                    return err
                }
            }
        default:
            return moerr.NewInternalError(ctx, "unsupported column type %d ", mysqlColumn.ColumnType())
        }
    }
    return
}

func (text *ResultSetRowText) Close(ctx context.Context) error {
    text.colData = nil
    text.colDef = nil
    text.lenEncBuffer = nil
    text.strconvBuffer = nil
    return nil
}

func (bin *ResultSetRowBinary) Open(ctx context.Context, options *MysqlWritePacketOptions) error {
    return nil
}

func (bin *ResultSetRowBinary) Write(ctx context.Context, out WriteBuffer, options *MysqlWritePacketOptions) error {
    var err error
    if len(bin.colDef) != len(bin.colData) {
        return moerr.NewInternalError(ctx, "column count mismatch")
    }
    err = out.Open(ctx, nil)
    if err != nil {
        return err
    }

    err = bin.writeImpl(ctx, out, options)
    if err != nil {
        return err
    }

    //output into outbuf
    err = out.Close(ctx)
    if err != nil {
        return err
    }
    return err
}

func (bin *ResultSetRowBinary) writeImpl(ctx context.Context, out WriteBuffer, options *MysqlWritePacketOptions) error {
    var err error
    err = appendUint8(ctx, out, defines.OKHeader) // append OkHeader
    if err != nil {
        return err
    }

    // get null buffer
    buffer := bin.binaryNullBuffer[:0]
    columnsLength := uint64(len(bin.colDef))
    numBytes4Null := (columnsLength + 7 + 2) / 8
    for i := uint64(0); i < numBytes4Null; i++ {
        buffer = append(buffer, 0)
    }
    for i := uint64(0); i < columnsLength; i++ {
        if bin.colData[i] == nil {
            bytePos := (i + 2) / 8
            bitPos := byte((i + 2) % 8)
            idx := int(bytePos)
            buffer[idx] |= 1 << bitPos
            continue
        }
    }
    err = appendBuffer(ctx, out, buffer)
    if err != nil {
        return err
    }

    for i := uint64(0); i < columnsLength; i++ {
        if bin.colData[i] == nil {
            continue
        }

        mysqlColumn := bin.colDef[i]
        row := bin.colData[i]
        switch mysqlColumn.ColumnType() {
        case defines.MYSQL_TYPE_BOOL:
            if value, err := getString(ctx, row); err != nil {
                return err
            } else {
                err = appendStringLenEnc(ctx, out, bin.lenEncBuffer, value)
                if err != nil {
                    return err
                }
            }
        case defines.MYSQL_TYPE_TINY:
            if value, err := getInt64(ctx, row); err != nil {
                return err
            } else {
                err = appendUint8(ctx, out, uint8(value))
                if err != nil {
                    return err
                }
            }
        case defines.MYSQL_TYPE_SHORT, defines.MYSQL_TYPE_YEAR:
            if value, err := getInt64(ctx, row); err != nil {
                return err
            } else {
                err = appendUint16(ctx, out, bin.lenEncBuffer, uint16(value))
                if err != nil {
                    return err
                }
            }
        case defines.MYSQL_TYPE_INT24, defines.MYSQL_TYPE_LONG:
            if value, err := getInt64(ctx, row); err != nil {
                return err
            } else {
                err = appendUint32(ctx, out, bin.lenEncBuffer, uint32(value))
                if err != nil {
                    return err
                }
            }
        case defines.MYSQL_TYPE_LONGLONG:
            if value, err := getUint64(ctx, row); err != nil {
                return err
            } else {
                err = appendUint64(ctx, out, bin.lenEncBuffer, value)
                if err != nil {
                    return err
                }
            }
        case defines.MYSQL_TYPE_FLOAT:
            switch v := row.(type) {
            case float32:
                err = appendUint32(ctx, out, bin.lenEncBuffer, math.Float32bits(v))
                if err != nil {
                    return err
                }
            case float64:
                err = appendUint32(ctx, out, bin.lenEncBuffer, math.Float32bits(float32(v)))
                if err != nil {
                    return err
                }
            default:
            }

        case defines.MYSQL_TYPE_DOUBLE:
            switch v := row.(type) {
            case float32:
                err = appendUint64(ctx, out, bin.lenEncBuffer, math.Float64bits(float64(v)))
                if err != nil {
                    return err
                }
            case float64:
                err = appendUint64(ctx, out, bin.lenEncBuffer, math.Float64bits(v))
                if err != nil {
                    return err
                }
            default:
            }

        // Binary/varbinary will be sent out as varchar type.
        case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING, defines.MYSQL_TYPE_STRING,
            defines.MYSQL_TYPE_BLOB, defines.MYSQL_TYPE_TEXT, defines.MYSQL_TYPE_JSON:
            if value, err := getString(ctx, row); err != nil {
                return err
            } else {
                err = appendStringLenEnc(ctx, out, bin.lenEncBuffer, value)
                if err != nil {
                    return err
                }
            }
        // TODO: some type, we use string now. someday need fix it
        case defines.MYSQL_TYPE_DECIMAL:
            if value, err := getString(ctx, row); err != nil {
                return err
            } else {
                err = appendStringLenEnc(ctx, out, bin.lenEncBuffer, value)
                if err != nil {
                    return err
                }
            }
        case defines.MYSQL_TYPE_UUID:
            if value, err := getString(ctx, row); err != nil {
                return err
            } else {
                err = appendStringLenEnc(ctx, out, bin.lenEncBuffer, value)
                if err != nil {
                    return err
                }
            }
        case defines.MYSQL_TYPE_DATE:
            err = appendDate(ctx, out, bin.lenEncBuffer, row.(types.Date))
            if err != nil {
                return err
            }
        case defines.MYSQL_TYPE_TIME:
            if value, err := getString(ctx, row); err != nil {
                return err
            } else {
                var t types.Time
                var err error
                idx := strings.Index(value, ".")
                if idx == -1 {
                    t, err = types.ParseTime(value, 0)
                } else {
                    t, err = types.ParseTime(value, int32(len(value)-idx-1))
                }
                if err != nil {
                    err = appendStringLenEnc(ctx, out, bin.lenEncBuffer, value)
                    if err != nil {
                        return err
                    }
                } else {
                    err = appendTime(ctx, out, bin.lenEncBuffer, t)
                    if err != nil {
                        return err
                    }
                }
            }
        case defines.MYSQL_TYPE_DATETIME, defines.MYSQL_TYPE_TIMESTAMP:
            if value, err := getString(ctx, row); err != nil {
                return err
            } else {
                var dt types.Datetime
                var err error
                idx := strings.Index(value, ".")
                if idx == -1 {
                    dt, err = types.ParseDatetime(value, 0)
                } else {
                    dt, err = types.ParseDatetime(value, int32(len(value)-idx-1))
                }
                if err != nil {
                    err = appendStringLenEnc(ctx, out, bin.lenEncBuffer, value)
                    if err != nil {
                        return err
                    }
                } else {
                    err = appendDatetime(ctx, out, bin.lenEncBuffer, dt)
                    if err != nil {
                        return err
                    }
                }
            }
        // case defines.MYSQL_TYPE_TIMESTAMP:
        // 	if value, err := mrs.GetString(rowIdx, i); err != nil {
        // 		return nil, err
        // 	} else {
        // 		data = mp.appendStringLenEnc(data, value)
        // 	}
        default:
            return moerr.NewInternalError(ctx, "type is not supported in binary text result row")
        }
    }
    return err
}

func (bin *ResultSetRowBinary) Close(ctx context.Context) error {
    bin.colData = nil
    bin.colDef = nil
    bin.lenEncBuffer = nil
    bin.strconvBuffer = nil
    bin.binaryNullBuffer = nil
    return nil
}

func (chunkw *ChunksWriter) Open(context.Context, *WriterOptions) error { return nil }
func (chunkw *ChunksWriter) Write(ctx context.Context, chunks Chunks, opts *WriterOptions) (err error) {
    chunkw.row = make([]any, len(chunks.Vecs))
    defer func() {
        chunkw.row = nil
    }()
    //column -> rows
    n := chunks.Vecs[0].Length()
    for j := 0; j < n; j++ { //row index
        //fill row
        err = extractRowFromEveryVector2(opts.ses, chunks, j, chunkw.row, chunkw.needCopyBytes)
        if err != nil {
            return err
        }
        //write row
        err = opts.writeRow(ctx, chunkw.row, opts)
        if err != nil {
            return err
        }
    }
    return err
}
func (chunkw *ChunksWriter) WriteBytes(context.Context, []byte, *WriterOptions) error { return nil }
func (chunkw *ChunksWriter) Flush(context.Context) error {
    return nil
}
func (chunkw *ChunksWriter) Close(context.Context) error {
    chunkw.row = nil
    return nil
}

func (format *MysqlFormatWriter) Open(ctx context.Context, opts *WriterOptions) error {
    format.strconvBuffer = make([]byte, 0, 16*1024)
    format.lenEncBuffer = make([]byte, 0, 10)
    format.binaryNullBuffer = make([]byte, 0, 512)
    format.packetIO = opts.packetIO
    format.ChunksWriter = &ChunksWriter{}
    _ = format.ChunksWriter.Open(ctx, opts)
    //init row
    format.textRow = &ResultSetRowText{
        lenEncBuffer:  format.lenEncBuffer,
        strconvBuffer: format.strconvBuffer,
    }
    format.binRow = &ResultSetRowBinary{
        lenEncBuffer:     format.lenEncBuffer,
        strconvBuffer:    format.strconvBuffer,
        binaryNullBuffer: format.binaryNullBuffer,
    }

    return nil
}

func (format *MysqlFormatWriter) writeRow(ctx context.Context, row []any, opts *WriterOptions) (err error) {
    if opts.isBinary {
        format.binRow.colData = row
        format.binRow.colDef = opts.colDef
        format.binRow.lenEncBuffer = format.lenEncBuffer
        format.binRow.strconvBuffer = format.strconvBuffer
        format.binRow.binaryNullBuffer = format.binaryNullBuffer
        err = format.packetIO.SendPacket(ctx, format.binRow, format.opts)
        if err != nil {
            return err
        }
    } else {
        format.textRow.colData = row
        format.textRow.colDef = opts.colDef
        format.textRow.lenEncBuffer = format.lenEncBuffer
        format.textRow.strconvBuffer = format.strconvBuffer
        err = format.packetIO.SendPacket(ctx, format.textRow, format.opts)
        if err != nil {
            return err
        }
    }
    return err
}

func (format *MysqlFormatWriter) Write(ctx context.Context, chunks Chunks, opts *WriterOptions) (err error) {
    opts.writeRow = format.writeRow
    return format.ChunksWriter.Write(ctx, chunks, opts)
}
func (format *MysqlFormatWriter) WriteBytes(context.Context, []byte, *WriterOptions) error {
    return nil
}
func (format *MysqlFormatWriter) Flush(context.Context) error {
    return nil
}
func (format *MysqlFormatWriter) Close(ctx context.Context) error {
    _ = format.ChunksWriter.Close(ctx)
    _ = format.textRow.Close(ctx)
    _ = format.binRow.Close(ctx)
    format.textRow = nil
    format.binRow = nil
    format.lenEncBuffer = nil
    format.strconvBuffer = nil
    format.binaryNullBuffer = nil
    return nil
}

/*
SendPacket sends the packet to the mysql client.
*/
func (io *PacketIO) SendPacket(ctx context.Context, packet MysqlWritePacket, opts *MysqlWritePacketOptions) error {
    err := packet.Write(ctx, io.out, opts)
    if err != nil {
        return err
    }
    if opts != nil && opts.flush {
        err = io.out.Flush(ctx)
        if err != nil {
            return err
        }
    }
    return err
}

func (table *TableStatusWriter) Open(ctx context.Context, opts *WriterOptions) error {
    table.ses = opts.ses
    table.ChunksWriter = &ChunksWriter{}
    _ = table.ChunksWriter.Open(ctx, opts)
    return nil
}

func (table *TableStatusWriter) writeRow(ctx context.Context, row []any, opts *WriterOptions) (err error) {
    row2 := make([]interface{}, len(row))
    copy(row2, row)
    table.ses.AppendData(row2)
    return nil
}

func (table *TableStatusWriter) Write(ctx context.Context, chunks Chunks, opts *WriterOptions) error {
    opts.writeRow = table.writeRow
    return table.ChunksWriter.Write(ctx, chunks, opts)
}
func (table *TableStatusWriter) WriteBytes(context.Context, []byte, *WriterOptions) error {
    return nil
}
func (table *TableStatusWriter) Flush(context.Context) error {
    return nil
}
func (table *TableStatusWriter) Close(context.Context) error {
    return nil
}
