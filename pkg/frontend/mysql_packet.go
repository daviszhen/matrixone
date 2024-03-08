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
    "bytes"
    "context"
    "fmt"
    "github.com/matrixorigin/matrixone/pkg/common/moerr"
    "github.com/matrixorigin/matrixone/pkg/container/bytejson"
    "github.com/matrixorigin/matrixone/pkg/container/types"
    "github.com/matrixorigin/matrixone/pkg/defines"
    "math"
    "strconv"
)

var (
    gio = NewIOPackage(true)
)

// write a string with fixed length into the buffer at the position
// return pos + string.length
func writeStringFix(data []byte, pos int, value string, length int) int {
    pos += copy(data[pos:], value[0:length])
    return pos
}

// write a string into the buffer at the position, then appended with 0
// return pos + string.length + 1
func writeStringNUL(data []byte, pos int, value string) int {
    pos = writeStringFix(data, pos, value, len(value))
    data[pos] = 0
    return pos + 1
}

// write the count of bytes into the buffer at the position
// return position + the number of bytes
func writeCountOfBytes(data []byte, pos int, value []byte) int {
    pos += copy(data[pos:], value)
    return pos
}

// write the count of zeros into the buffer at the position
// return pos + count
func writeZeros(data []byte, pos int, count int) int {
    for i := 0; i < count; i++ {
        data[pos+i] = 0
    }
    return pos + count
}

// write an int with length encoded into the buffer at the position
// return position + the count of bytes for length encoded (1 or 3 or 4 or 9)
func writeIntLenEnc(data []byte, pos int, value uint64) int {
    switch {
    case value < 251:
        data[pos] = byte(value)
        return pos + 1
    case value < (1 << 16):
        data[pos] = 0xfc
        data[pos+1] = byte(value)
        data[pos+2] = byte(value >> 8)
        return pos + 3
    case value < (1 << 24):
        data[pos] = 0xfd
        data[pos+1] = byte(value)
        data[pos+2] = byte(value >> 8)
        data[pos+3] = byte(value >> 16)
        return pos + 4
    default:
        data[pos] = 0xfe
        data[pos+1] = byte(value)
        data[pos+2] = byte(value >> 8)
        data[pos+3] = byte(value >> 16)
        data[pos+4] = byte(value >> 24)
        data[pos+5] = byte(value >> 32)
        data[pos+6] = byte(value >> 40)
        data[pos+7] = byte(value >> 48)
        data[pos+8] = byte(value >> 56)
        return pos + 9
    }
}

// write a string with length encoded into the buffer at the position
// return position + the count of bytes for length encoded (1 or 3 or 4 or 9) + length of the string;
func writeStringLenEnc(data []byte, pos int, value string) int {
    pos = writeIntLenEnc(data, pos, uint64(len(value)))
    return writeStringFix(data, pos, value, len(value))
}

// read a string appended with zero from the buffer at the position
// return string ; position + length of the string + 1; true - succeeded or false - failed
func readStringNUL(data []byte, pos int) (string, int, bool) {
    zeroPos := bytes.IndexByte(data[pos:], 0)
    if zeroPos == -1 {
        return "", 0, false
    }
    return string(data[pos : pos+zeroPos]), pos + zeroPos + 1, true
}

// read an int with length encoded from the buffer at the position
// return the int ; position + the count of bytes for length encoded (1 or 3 or 4 or 9)
func readIntLenEnc(data []byte, pos int) (uint64, int, bool) {
    if pos >= len(data) {
        return 0, 0, false
    }
    switch data[pos] {
    case 0xfb:
        //zero, one byte
        return 0, pos + 1, true
    case 0xfc:
        // int in two bytes
        if pos+2 >= len(data) {
            return 0, 0, false
        }
        value := uint64(data[pos+1]) |
            uint64(data[pos+2])<<8
        return value, pos + 3, true
    case 0xfd:
        // int in three bytes
        if pos+3 >= len(data) {
            return 0, 0, false
        }
        value := uint64(data[pos+1]) |
            uint64(data[pos+2])<<8 |
            uint64(data[pos+3])<<16
        return value, pos + 4, true
    case 0xfe:
        // int in eight bytes
        if pos+8 >= len(data) {
            return 0, 0, false
        }
        value := uint64(data[pos+1]) |
            uint64(data[pos+2])<<8 |
            uint64(data[pos+3])<<16 |
            uint64(data[pos+4])<<24 |
            uint64(data[pos+5])<<32 |
            uint64(data[pos+6])<<40 |
            uint64(data[pos+7])<<48 |
            uint64(data[pos+8])<<56
        return value, pos + 9, true
    }
    // 0-250
    return uint64(data[pos]), pos + 1, true
}

// read the count of bytes from the buffer at the position
// return bytes slice ; position + count ; true - succeeded or false - failed
func readCountOfBytes(data []byte, pos int, count int) ([]byte, int, bool) {
    if pos+count-1 >= len(data) {
        return nil, 0, false
    }
    return data[pos : pos+count], pos + count, true
}

// read a string with length encoded from the buffer at the position
// return string ; position + the count of bytes for length encoded (1 or 3 or 4 or 9) + length of the string; true - succeeded or false - failed
func readStringLenEnc(data []byte, pos int) (string, int, bool) {
    var value uint64
    var ok bool
    value, pos, ok = readIntLenEnc(data, pos)
    if !ok {
        return "", 0, false
    }
    sLength := int(value)
    if pos+sLength-1 >= len(data) {
        return "", 0, false
    }
    return string(data[pos : pos+sLength]), pos + sLength, true
}

func readDate(data []byte, pos int) (int, string) {
    year, pos, _ := gio.ReadUint16(data, pos)
    month := data[pos]
    pos++
    day := data[pos]
    pos++
    return pos, fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

func readTime(data []byte, pos int, len uint8) (int, string) {
    var retStr string
    negate := data[pos]
    pos++
    if negate == 1 {
        retStr += "-"
    }
    day, pos, _ := gio.ReadUint32(data, pos)
    if day > 0 {
        retStr += fmt.Sprintf("%dd ", day)
    }
    hour := data[pos]
    pos++
    minute := data[pos]
    pos++
    second := data[pos]
    pos++

    if len == 12 {
        ms, _, _ := gio.ReadUint32(data, pos)
        retStr += fmt.Sprintf("%02d:%02d:%02d.%06d", hour, minute, second, ms)
    } else {
        retStr += fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)
    }

    return pos, retStr
}

func readDateTime(data []byte, pos int) (int, string) {
    pos, date := readDate(data, pos)
    hour := data[pos]
    pos++
    minute := data[pos]
    pos++
    second := data[pos]
    pos++
    return pos, fmt.Sprintf("%s %02d:%02d:%02d", date, hour, minute, second)
}

func readTimestamp(data []byte, pos int) (int, string) {
    pos, dateTime := readDateTime(data, pos)
    microSecond, pos, _ := gio.ReadUint32(data, pos)
    return pos, fmt.Sprintf("%s.%06d", dateTime, microSecond)
}

func appendUint8(ctx context.Context, out WriteBuffer, e uint8) error {
    return out.Write(ctx, []byte{e}, nil)
}

func appendBuffer(ctx context.Context, out WriteBuffer, buf []byte) error {
    return out.Write(ctx, buf, nil)
}

func appendUint16(ctx context.Context, out WriteBuffer, lenEncBuffer []byte, e uint16) error {
    buf := lenEncBuffer[:2]
    pos := gio.WriteUint16(buf, 0, e)
    return appendBuffer(ctx, out, buf[:pos])
}

func appendUint32(ctx context.Context, out WriteBuffer, lenEncBuffer []byte, e uint32) error {
    buf := lenEncBuffer[:4]
    pos := gio.WriteUint32(buf, 0, e)
    return appendBuffer(ctx, out, buf[:pos])
}

func appendUint64(ctx context.Context, out WriteBuffer, lenEncBuffer []byte, e uint64) error {
    buf := lenEncBuffer[:8]
    pos := gio.WriteUint64(buf, 0, e)
    return appendBuffer(ctx, out, buf[:pos])
}

func appendDatetime(ctx context.Context, out WriteBuffer, lenEncBuffer []byte, dt types.Datetime) (err error) {
    if dt.MicroSec() != 0 {
        err = appendUint8(ctx, out, 11)
        if err != nil {
            return err
        }
        err = appendUint16(ctx, out, lenEncBuffer, uint16(dt.Year()))
        if err != nil {
            return err
        }
        err = appendBuffer(ctx, out, []byte{dt.Month(), dt.Day(), byte(dt.Hour()), byte(dt.Minute()), byte(dt.Sec())})
        if err != nil {
            return err
        }
        err = appendUint32(ctx, out, lenEncBuffer, uint32(dt.MicroSec()))
        if err != nil {
            return err
        }
    } else if dt.Hour() != 0 || dt.Minute() != 0 || dt.Sec() != 0 {
        err = appendUint8(ctx, out, 7)
        if err != nil {
            return err
        }
        err = appendUint16(ctx, out, lenEncBuffer, uint16(dt.Year()))
        if err != nil {
            return err
        }
        err = appendBuffer(ctx, out, []byte{dt.Month(), dt.Day(), byte(dt.Hour()), byte(dt.Minute()), byte(dt.Sec())})
        if err != nil {
            return err
        }
    } else {
        err = appendUint8(ctx, out, 4)
        if err != nil {
            return err
        }
        err = appendUint16(ctx, out, lenEncBuffer, uint16(dt.Year()))
        if err != nil {
            return err
        }
        err = appendBuffer(ctx, out, []byte{dt.Month(), dt.Day()})
        if err != nil {
            return err
        }
    }
    return err
}

func appendTime(ctx context.Context, out WriteBuffer, lenEncBuffer []byte, t types.Time) (err error) {
    if int64(t) == 0 {
        err = appendUint8(ctx, out, 0)
        if err != nil {
            return err
        }
    } else {
        hour, minute, sec, msec, isNeg := t.ClockFormat()
        day := uint32(hour / 24)
        hour = hour % 24
        if msec != 0 {
            err = appendUint8(ctx, out, 12)
            if err != nil {
                return err
            }
            if isNeg {
                err = appendUint8(ctx, out, byte(1))
                if err != nil {
                    return err
                }
            } else {
                err = appendUint8(ctx, out, byte(0))
                if err != nil {
                    return err
                }
            }
            err = appendUint32(ctx, out, lenEncBuffer, day)
            if err != nil {
                return err
            }
            err = appendBuffer(ctx, out, []byte{uint8(hour), minute, sec})
            if err != nil {
                return err
            }
            err = appendUint64(ctx, out, lenEncBuffer, msec)
            if err != nil {
                return err
            }
        } else {
            err = appendUint8(ctx, out, 8)
            if err != nil {
                return err
            }
            if isNeg {
                err = appendUint8(ctx, out, byte(1))
                if err != nil {
                    return err
                }
            } else {
                err = appendUint8(ctx, out, byte(0))
                if err != nil {
                    return err
                }
            }
            err = appendUint32(ctx, out, lenEncBuffer, day)
            if err != nil {
                return err
            }
            err = appendBuffer(ctx, out, []byte{uint8(hour), minute, sec})
            if err != nil {
                return err
            }
        }
    }
    return err
}

func appendDate(ctx context.Context, out WriteBuffer, lenEncBuffer []byte, value types.Date) (err error) {
    if int32(value) == 0 {
        err = appendUint8(ctx, out, 0)
    } else {
        err = appendUint8(ctx, out, 4)
        if err != nil {
            return err
        }

        err = appendUint16(ctx, out, lenEncBuffer, value.Year())
        if err != nil {
            return err
        }
        err = appendBuffer(ctx, out, []byte{value.Month(), value.Day()})
        if err != nil {
            return err
        }
    }
    return err
}

// append an int with length encoded to the buffer
// return the buffer
func appendIntLenEnc(ctx context.Context, out WriteBuffer, lenEncBuffer []byte, value uint64) error {
    lenEncBuffer = lenEncBuffer[:9]
    pos := writeIntLenEnc(lenEncBuffer, 0, value)
    return out.Write(ctx, lenEncBuffer[:pos], nil)
}

// append a string with fixed length to the buffer
// return the buffer
func appendStringFix(ctx context.Context, out WriteBuffer, value string, length int) error {
    return out.Write(ctx, []byte(value[:length]), nil)
}

// append a string with length encoded to the buffer
// return the buffer
func appendStringLenEnc(ctx context.Context, out WriteBuffer, lenEncBuffer []byte, value string) error {
    err := appendIntLenEnc(ctx, out, lenEncBuffer, uint64(len(value)))
    if err != nil {
        return err
    }
    return appendStringFix(ctx, out, value, len(value))
}

// append the count of bytes to the buffer
// return the buffer
func appendCountOfBytes(ctx context.Context, out WriteBuffer, value []byte) error {
    return out.Write(ctx, value, nil)
}

// append bytes with length encoded to the buffer
// return the buffer
func appendCountOfBytesLenEnc(ctx context.Context, out WriteBuffer, lenEncBuffer []byte, value []byte) error {
    err := appendIntLenEnc(ctx, out, lenEncBuffer, uint64(len(value)))
    if err != nil {
        return err
    }
    return appendCountOfBytes(ctx, out, value)
}

// append an int64 value converted to string with length encoded to the buffer
// return the buffer
func appendStringLenEncOfInt64(ctx context.Context, out WriteBuffer, lenEncBuffer []byte, strconvBuffer []byte, value int64) error {
    strconvBuffer = strconvBuffer[:0]
    strconvBuffer = strconv.AppendInt(strconvBuffer, value, 10)
    return appendCountOfBytesLenEnc(ctx, out, lenEncBuffer, strconvBuffer)
}

// append an uint64 value converted to string with length encoded to the buffer
// return the buffer
func appendStringLenEncOfUint64(ctx context.Context, out WriteBuffer, lenEncBuffer []byte, strconvBuffer []byte, value uint64) error {
    strconvBuffer = strconvBuffer[:0]
    strconvBuffer = strconv.AppendUint(strconvBuffer, value, 10)
    return appendCountOfBytesLenEnc(ctx, out, lenEncBuffer, strconvBuffer)
}

// append an float32 value converted to string with length encoded to the buffer
// return the buffer
func appendStringLenEncOfFloat64(ctx context.Context, out WriteBuffer, lenEncBuffer []byte, strconvBuffer []byte, value float64, bitSize int) error {
    strconvBuffer = strconvBuffer[:0]
    if !math.IsInf(value, 0) {
        strconvBuffer = strconv.AppendFloat(strconvBuffer, value, 'f', -1, bitSize)
    } else {
        if math.IsInf(value, 1) {
            strconvBuffer = append(strconvBuffer, []byte("+Infinity")...)
        } else {
            strconvBuffer = append(strconvBuffer, []byte("-Infinity")...)
        }
    }
    return appendCountOfBytesLenEnc(ctx, out, lenEncBuffer, strconvBuffer)
}

// convert the value into string
func getString(ctx context.Context, value any) (string, error) {
    switch v := value.(type) {
    case bool:
        if v {
            return "true", nil
        } else {
            return "false", nil
        }
    case uint8:
        return strconv.FormatUint(uint64(v), 10), nil
    case uint16:
        return strconv.FormatUint(uint64(v), 10), nil
    case uint32:
        return strconv.FormatUint(uint64(v), 10), nil
    case uint64:
        return strconv.FormatUint(uint64(v), 10), nil
    case int8:
        return strconv.FormatInt(int64(v), 10), nil
    case int16:
        return strconv.FormatInt(int64(v), 10), nil
    case int32:
        return strconv.FormatInt(int64(v), 10), nil
    case int64:
        return strconv.FormatInt(int64(v), 10), nil
    case float32:
        return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
    case float64:
        return strconv.FormatFloat(v, 'f', -1, 64), nil
    case string:
        return v, nil
    case []byte:
        return string(v), nil
    case []float32:
        return types.ArrayToString[float32](v), nil
    case []float64:
        return types.ArrayToString[float64](v), nil
    case int:
        return strconv.FormatInt(int64(v), 10), nil
    case uint:
        return strconv.FormatUint(uint64(v), 10), nil
    case types.Time:
        return v.String(), nil
    case types.Datetime:
        return v.String(), nil
    case bytejson.ByteJson:
        return v.String(), nil
    case types.Uuid:
        return v.ToString(), nil
    case types.Blockid:
        return v.String(), nil
    case types.TS:
        return v.ToString(), nil
    case types.Enum:
        return strconv.FormatUint(uint64(v), 10), nil
    default:
        return "", moerr.NewInternalError(ctx, "unsupported type %d ", v)
    }
}

// convert the value into int64
func getInt64(ctx context.Context, value any) (int64, error) {
    switch v := value.(type) {
    case bool:
        if v {
            return 1, nil
        } else {
            return 0, nil
        }
    case uint8:
        return int64(v), nil
    case uint16:
        return int64(v), nil
    case uint32:
        return int64(v), nil
    case uint64:
        return int64(v), nil
    case int8:
        return int64(v), nil
    case int16:
        return int64(v), nil
    case int32:
        return int64(v), nil
    case int64:
        return int64(v), nil
    case float32:
        return int64(v), nil
    case float64:
        return int64(v), nil
    case string:
        return strconv.ParseInt(v, 10, 64)
    case []byte:
        return strconv.ParseInt(string(v), 10, 64)
    case int:
        return int64(v), nil
    case uint:
        return int64(v), nil
    default:
        return 0, moerr.NewInternalError(ctx, "unsupported type %d ", v)
    }
}

// convert the value into uint64
func getUint64(ctx context.Context, value any) (uint64, error) {
    switch v := value.(type) {
    case bool:
        if v {
            return 1, nil
        } else {
            return 0, nil
        }
    case uint8:
        return uint64(v), nil
    case uint16:
        return uint64(v), nil
    case uint32:
        return uint64(v), nil
    case uint64:
        return uint64(v), nil
    case int8:
        return uint64(v), nil
    case int16:
        return uint64(v), nil
    case int32:
        return uint64(v), nil
    case int64:
        return uint64(v), nil
    case float32:
        return uint64(v), nil
    case float64:
        return uint64(v), nil
    case string:
        return strconv.ParseUint(v, 10, 64)
    case []byte:
        return strconv.ParseUint(string(v), 10, 64)
    case int:
        return uint64(v), nil
    case uint:
        return uint64(v), nil
    default:
        return 0, moerr.NewInternalError(ctx, "unsupported type %d ", v)
    }
}

// make the column information with the format of column definition41
func makeColumnDefinition41Payload(column *MysqlColumn, cmd int) []byte {
    space := 8*9 + //lenenc bytes of 8 fields
        21 + //fixed-length fields
        3 + // catalog "def"
        len(column.Schema()) +
        len(column.Table()) +
        len(column.OrgTable()) +
        len(column.Name()) +
        len(column.OrgName()) +
        len(column.DefaultValue()) +
        100 // for safe

    data := make([]byte, space)
    pos := 0

    //lenenc_str     catalog(always "def")
    pos = writeStringLenEnc(data, pos, "def")

    //lenenc_str     schema
    pos = writeStringLenEnc(data, pos, column.Schema())

    //lenenc_str     table
    pos = writeStringLenEnc(data, pos, column.Table())

    //lenenc_str     org_table
    pos = writeStringLenEnc(data, pos, column.OrgTable())

    //lenenc_str     name
    pos = writeStringLenEnc(data, pos, column.Name())

    //lenenc_str     org_name
    pos = writeStringLenEnc(data, pos, column.OrgName())

    //lenenc_int     length of fixed-length fields [0c]
    pos = gio.WriteUint8(data, pos, 0x0c)

    if column.ColumnType() == defines.MYSQL_TYPE_BOOL {
        //int<2>              character set
        pos = gio.WriteUint16(data, pos, charsetVarchar)
        //int<4>              column length
        pos = gio.WriteUint32(data, pos, boolColumnLength)
        //int<1>              type
        pos = gio.WriteUint8(data, pos, uint8(defines.MYSQL_TYPE_VARCHAR))
    } else {
        //int<2>              character set
        pos = gio.WriteUint16(data, pos, column.Charset())
        //int<4>              column length
        pos = gio.WriteUint32(data, pos, column.Length())
        //int<1>              type
        pos = gio.WriteUint8(data, pos, uint8(column.ColumnType()))
    }

    //int<2>              flags
    pos = gio.WriteUint16(data, pos, column.Flag())

    //int<1>              decimals
    pos = gio.WriteUint8(data, pos, column.Decimal())

    //int<2>              filler [00] [00]
    pos = gio.WriteUint16(data, pos, 0)

    if CommandType(cmd) == COM_FIELD_LIST {
        pos = writeIntLenEnc(data, pos, uint64(len(column.DefaultValue())))
        pos = writeCountOfBytes(data, pos, column.DefaultValue())
    }

    return data[:pos]
}

func makeEOFPayload(data []byte, warnings, status uint16, capability uint32) []byte {
    pos := 0
    pos = gio.WriteUint8(data, pos, defines.EOFHeader)
    if capability&CLIENT_PROTOCOL_41 != 0 {
        pos = gio.WriteUint16(data, pos, warnings)
        pos = gio.WriteUint16(data, pos, status)
    }
    return data[:pos]
}