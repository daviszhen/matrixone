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
	"bytes"
	"context"
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
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

func appendUint8(ctx context.Context, out WriteBuffer, e uint8) error {
	return out.Write(ctx, []byte{e})
}

func appendBuffer(ctx context.Context, out WriteBuffer, buf []byte) error {
	return out.Write(ctx, buf)
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
	return out.Write(ctx, lenEncBuffer[:pos])
}

// append a string with fixed length to the buffer
// return the buffer
func appendStringFix(ctx context.Context, out WriteBuffer, value string, length int) error {
	return out.Write(ctx, []byte(value[:length]))
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
	return out.Write(ctx, value)
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

func updateOpts(opts ...MysqlWritePacketOpt) *MysqlWritePacketOptions {
	newopts := &MysqlWritePacketOptions{}
	for _, opt := range opts {
		opt(newopts)
	}
	return newopts
}

// the server makes a handshake v10 packet
// return handshake packet
func (hand *Handshake) makeHandshakeV10Payload() []byte {
	var data = make([]byte, HeaderOffset+256)
	var pos = HeaderOffset
	//int<1> protocol version
	pos = gio.WriteUint8(data, pos, clientProtocolVersion)

	pos = writeStringNUL(data, pos, hand.serverVersionPrefix+serverVersion.Load().(string))

	//int<4> connection id
	pos = gio.WriteUint32(data, pos, hand.connectionID)

	//string[8] auth-plugin-data-part-1
	pos = writeCountOfBytes(data, pos, hand.salt[0:8])

	//int<1> filler 0
	pos = gio.WriteUint8(data, pos, 0)

	//int<2>              capabilities flags (lower 2 bytes)
	pos = gio.WriteUint16(data, pos, uint16(hand.capability&0xFFFF))

	//int<1>              character set
	pos = gio.WriteUint8(data, pos, utf8mb4BinCollationID)

	//int<2>              status flags
	pos = gio.WriteUint16(data, pos, DefaultClientConnStatus)

	//int<2>              capabilities flags (upper 2 bytes)
	pos = gio.WriteUint16(data, pos, uint16((DefaultCapability>>16)&0xFFFF))

	if (DefaultCapability & CLIENT_PLUGIN_AUTH) != 0 {
		//int<1>              length of auth-plugin-data
		//set 21 always
		pos = gio.WriteUint8(data, pos, uint8(len(hand.salt)+1))
	} else {
		//int<1>              [00]
		//set 0 always
		pos = gio.WriteUint8(data, pos, 0)
	}

	//string[10]     reserved (all [00])
	pos = writeZeros(data, pos, 10)

	if (DefaultCapability & CLIENT_SECURE_CONNECTION) != 0 {
		//string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
		pos = writeCountOfBytes(data, pos, hand.salt[8:])
		pos = gio.WriteUint8(data, pos, 0)
	}

	if (DefaultCapability & CLIENT_PLUGIN_AUTH) != 0 {
		//string[NUL]    auth-plugin name
		pos = writeStringNUL(data, pos, AuthNativePassword)
	}

	return data[:pos]
}

func (hand *Handshake) Open(ctx context.Context, opts ...MysqlWritePacketOpt) error {
	newopts := updateOpts(opts...)

	hand.serverVersionPrefix = newopts.serverVersionPrefix
	hand.salt = newopts.salt
	hand.connectionID = newopts.connectionID
	hand.capability = newopts.capability
	hand.data = hand.makeHandshakeV10Payload()
	return nil
}

// Write implements MysqlWritePacket.
func (hand *Handshake) Write(ctx context.Context, out WriteBuffer) error {
	return out.Write(ctx, hand.data)
}

// Close implements MysqlWritePacket.
func (hand *Handshake) Close(context.Context) error {
	hand.data = nil
	hand.salt = nil
	return nil
}

func (handrsp *HandshakeResponse) Open(context.Context, ...MysqlReadPacketOpt) error {
	return nil
}

// the server analyses handshake response41 info from the client
// return true - analysed successfully / false - failed ; response41 ; error
func (handrsp *HandshakeResponse) analyseHandshakeResponse41(ctx context.Context, data []byte) (bool, response41, error) {
	var pos = 0
	var ok bool
	var info response41

	//int<4>             capabilities flags of the client, CLIENT_PROTOCOL_41 always set
	info.capabilities, pos, ok = gio.ReadUint32(data, pos)
	if !ok {
		return false, info, moerr.NewInternalError(ctx, "get capabilities failed")
	}

	if (info.capabilities & CLIENT_PROTOCOL_41) == 0 {
		return false, info, moerr.NewInternalError(ctx, "capabilities does not have protocol 41")
	}

	//int<4>             max-packet size
	//max size of a command packet that the client wants to send to the server
	info.maxPacketSize, pos, ok = gio.ReadUint32(data, pos)
	if !ok {
		return false, info, moerr.NewInternalError(ctx, "get max packet size failed")
	}

	//int<1>             character set
	//connection's default character set
	info.collationID, pos, ok = gio.ReadUint8(data, pos)
	if !ok {
		return false, info, moerr.NewInternalError(ctx, "get character set failed")
	}

	if pos+22 >= len(data) {
		return false, info, moerr.NewInternalError(ctx, "skip reserved failed")
	}
	//string[23]         reserved (all [0])
	//just skip it
	pos += 23

	// if client reply for upgradeTls, then data will contains header only.
	if pos == len(data) && (info.capabilities&CLIENT_SSL) != 0 {
		info.isAskForTlsHeader = true
		return true, info, nil
	}

	//string[NUL]        username
	info.username, pos, ok = readStringNUL(data, pos)
	if !ok {
		return false, info, moerr.NewInternalError(ctx, "get username failed")
	}

	/*
		if capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA {
			lenenc-int         length of auth-response
			string[n]          auth-response
		} else if capabilities & CLIENT_SECURE_CONNECTION {
			int<1>             length of auth-response
			string[n]           auth-response
		} else {
			string[NUL]        auth-response
		}
	*/
	if (info.capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0 {
		var l uint64
		l, pos, ok = readIntLenEnc(data, pos)
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get length of auth-response failed")
		}
		info.authResponse, pos, ok = readCountOfBytes(data, pos, int(l))
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get auth-response failed")
		}
	} else if (info.capabilities & CLIENT_SECURE_CONNECTION) != 0 {
		var l uint8
		l, pos, ok = gio.ReadUint8(data, pos)
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get length of auth-response failed")
		}
		info.authResponse, pos, ok = readCountOfBytes(data, pos, int(l))
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get auth-response failed")
		}
	} else {
		var auth string
		auth, pos, ok = readStringNUL(data, pos)
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get auth-response failed")
		}
		info.authResponse = []byte(auth)
	}

	if (info.capabilities & CLIENT_CONNECT_WITH_DB) != 0 {
		info.database, pos, ok = readStringNUL(data, pos)
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get database failed")
		}
	}

	if (info.capabilities & CLIENT_PLUGIN_AUTH) != 0 {
		info.clientPluginName, pos, ok = readStringNUL(data, pos)
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get auth plugin name failed")
		}

		//to switch authenticate method
		if info.clientPluginName != AuthNativePassword {
			info.needChangAuthMethod = true
			// var err error
			// if info.authResponse, err = mp.negotiateAuthenticationMethod(ctx); err != nil {
			// 	return false, info, moerr.NewInternalError(ctx, "negotiate authentication method failed. error:%v", err)
			// }
			// info.clientPluginName = AuthNativePassword
		}
	}

	// client connection attributes
	info.connectAttrs = make(map[string]string)
	if info.capabilities&CLIENT_CONNECT_ATTRS != 0 {
		var l uint64
		var ok bool
		l, pos, ok = readIntLenEnc(data, pos)
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get length of client-connect-attrs failed")
		}
		endPos := pos + int(l)
		var key, value string
		for pos < endPos {
			key, pos, ok = readStringLenEnc(data, pos)
			if !ok {
				return false, info, moerr.NewInternalError(ctx, "get connect-attrs key failed")
			}
			value, pos, ok = readStringLenEnc(data, pos)
			if !ok {
				return false, info, moerr.NewInternalError(ctx, "get connect-attrs value failed")
			}
			info.connectAttrs[key] = value
		}
	}

	return true, info, nil
}

// the server analyses handshake response320 info from the old client
// return true - analysed successfully / false - failed ; response320 ; error
func (handrsp *HandshakeResponse) analyseHandshakeResponse320(ctx context.Context, data []byte) (bool, response320, error) {
	var pos = 0
	var ok bool
	var info response320
	var capa uint16

	//int<2>             capabilities flags, CLIENT_PROTOCOL_41 never set
	capa, pos, ok = gio.ReadUint16(data, pos)
	if !ok {
		return false, info, moerr.NewInternalError(ctx, "get capabilities failed")
	}
	info.capabilities = uint32(capa)

	if pos+2 >= len(data) {
		return false, info, moerr.NewInternalError(ctx, "get max-packet-size failed")
	}

	//int<3>             max-packet size
	//max size of a command packet that the client wants to send to the server
	info.maxPacketSize = uint32(data[pos]) | uint32(data[pos+1])<<8 | uint32(data[pos+2])<<16
	pos += 3

	// if client reply for upgradeTls, then data will contains header only.
	if pos == len(data) && (info.capabilities&CLIENT_SSL) != 0 {
		info.isAskForTlsHeader = true
		return true, info, nil
	}

	//string[NUL]        username
	info.username, pos, ok = readStringNUL(data, pos)
	if !ok {
		return false, info, moerr.NewInternalError(ctx, "get username failed")
	}

	if (info.capabilities & CLIENT_CONNECT_WITH_DB) != 0 {
		var auth string
		auth, pos, ok = readStringNUL(data, pos)
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get auth-response failed")
		}
		info.authResponse = []byte(auth)

		info.database, _, ok = readStringNUL(data, pos)
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get database failed")
		}
	} else {
		info.authResponse, _, ok = readCountOfBytes(data, pos, len(data)-pos)
		if !ok {
			return false, info, moerr.NewInternalError(ctx, "get auth-response failed")
		}
	}

	return true, info, nil
}

func (handrsp *HandshakeResponse) Read(ctx context.Context, payload []byte) error {
	var err error
	if len(payload) < 2 {
		return moerr.NewInternalError(ctx, "received a broken response packet")
	}

	if capabilities, _, ok := gio.ReadUint16(payload, 0); !ok {
		return moerr.NewInternalError(ctx, "read capabilities from response packet failed")
	} else if uint32(capabilities)&CLIENT_PROTOCOL_41 != 0 {
		var resp41 response41
		var ok2 bool
		// logDebugf(mp.getDebugStringUnsafe(), "analyse handshake response")
		if ok2, resp41, err = handrsp.analyseHandshakeResponse41(ctx, payload); !ok2 {
			return err
		}

		// client ask server to upgradeTls
		if resp41.isAskForTlsHeader {
			handrsp.isAskForTlsHeader = true
			return nil
		}

		handrsp.authResponse = resp41.authResponse
		handrsp.capability = handrsp.capability & resp41.capabilities

		if nameAndCharset, ok3 := collationID2CharsetAndName[int(resp41.collationID)]; !ok3 {
			return moerr.NewInternalError(ctx, "get collationName and charset failed")
		} else {
			handrsp.collationID = int(resp41.collationID)
			handrsp.collationName = nameAndCharset.collationName
			handrsp.charset = nameAndCharset.charset
		}

		handrsp.maxClientPacketSize = resp41.maxPacketSize
		handrsp.username = resp41.username
		handrsp.database = resp41.database
		handrsp.connectAttrs = resp41.connectAttrs
		handrsp.needChangAuthMethod = resp41.needChangAuthMethod
	} else {
		var resp320 response320
		var ok2 bool
		if ok2, resp320, err = handrsp.analyseHandshakeResponse320(ctx, payload); !ok2 {
			return err
		}

		// client ask server to upgradeTls
		if resp320.isAskForTlsHeader {
			handrsp.isAskForTlsHeader = true
			return nil
		}

		handrsp.authResponse = resp320.authResponse
		handrsp.capability = handrsp.capability & resp320.capabilities
		handrsp.collationID = int(Utf8mb4CollationID)
		handrsp.collationName = "utf8mb4_general_ci"
		handrsp.charset = "utf8mb4"

		handrsp.maxClientPacketSize = resp320.maxPacketSize
		handrsp.username = resp320.username
		handrsp.database = resp320.database
	}
	return err
}

func (handrsp *HandshakeResponse) Close(context.Context) error {
	return nil
}

// make a OK packet
func (ok *OKPacket) makeOKPayload(affectedRows, lastInsertId uint64, statusFlags, warnings uint16, message string) []byte {
	data := make([]byte, HeaderOffset+128+len(message)+10)
	var pos = HeaderOffset
	pos = gio.WriteUint8(data, pos, defines.OKHeader)
	pos = writeIntLenEnc(data, pos, affectedRows)
	pos = writeIntLenEnc(data, pos, lastInsertId)
	if (ok.capability & CLIENT_PROTOCOL_41) != 0 {
		pos = gio.WriteUint16(data, pos, statusFlags)
		pos = gio.WriteUint16(data, pos, warnings)
	} else if (ok.capability & CLIENT_TRANSACTIONS) != 0 {
		pos = gio.WriteUint16(data, pos, statusFlags)
	}

	if ok.capability&CLIENT_SESSION_TRACK != 0 {
		//TODO:implement it
	} else {
		//string<lenenc> instead of string<EOF> in the manual of mysql
		pos = writeStringLenEnc(data, pos, message)
		return data[:pos]
	}
	return data[:pos]
}
func (ok *OKPacket) Open(ctx context.Context, opts ...MysqlWritePacketOpt) error {
	newopts := updateOpts(opts...)

	ok.capability = newopts.capability
	ok.data = ok.makeOKPayload(newopts.affectedRows,
		newopts.lastInsertId,
		newopts.statusFlags,
		newopts.warnings,
		newopts.message)
	return nil
}
func (ok *OKPacket) Write(ctx context.Context, out WriteBuffer) error {
	return out.Write(ctx, ok.data)
}
func (ok *OKPacket) Close(context.Context) error {
	ok.data = nil
	return nil
}

func (okeof *OKPacketWithEOF) makeOKPayloadWithEof(affectedRows, lastInsertId uint64, statusFlags, warnings uint16, message string) []byte {
	data := make([]byte, HeaderOffset+128+len(message)+10)
	var pos = HeaderOffset
	pos = gio.WriteUint8(data, pos, defines.EOFHeader)
	pos = writeIntLenEnc(data, pos, affectedRows)
	pos = writeIntLenEnc(data, pos, lastInsertId)
	if (okeof.capability & CLIENT_PROTOCOL_41) != 0 {
		pos = gio.WriteUint16(data, pos, statusFlags)
		pos = gio.WriteUint16(data, pos, warnings)
	} else if (okeof.capability & CLIENT_TRANSACTIONS) != 0 {
		pos = gio.WriteUint16(data, pos, statusFlags)
	}

	if okeof.capability&CLIENT_SESSION_TRACK != 0 {
		//TODO:implement it
	} else {
		//string<lenenc> instead of string<EOF> in the manual of mysql
		pos = writeStringLenEnc(data, pos, message)
		return data[:pos]
	}
	return data[:pos]
}

func (okeof *OKPacketWithEOF) Open(ctx context.Context, opts ...MysqlWritePacketOpt) error {
	newopts := updateOpts(opts...)

	okeof.capability = newopts.capability
	okeof.data = okeof.makeOKPayloadWithEof(newopts.affectedRows,
		newopts.lastInsertId,
		newopts.statusFlags,
		newopts.warnings,
		newopts.message)
	return nil
}
func (okeof *OKPacketWithEOF) Write(ctx context.Context, out WriteBuffer) error {
	return out.Write(ctx, okeof.data)
}
func (okeof *OKPacketWithEOF) Close(context.Context) error {
	okeof.data = nil
	return nil
}

func (eof *EOFPacket) makeEOFPayload(warnings, status uint16) []byte {
	data := make([]byte, HeaderOffset+10)
	pos := HeaderOffset
	pos = gio.WriteUint8(data, pos, defines.EOFHeader)
	if eof.capability&CLIENT_PROTOCOL_41 != 0 {
		pos = gio.WriteUint16(data, pos, warnings)
		pos = gio.WriteUint16(data, pos, status)
	}
	return data[:pos]
}

func (eof *EOFPacket) Open(ctx context.Context, opts ...MysqlWritePacketOpt) error {
	newopts := updateOpts(opts...)
	eof.capability = newopts.capability
	eof.data = eof.makeEOFPayload(newopts.warnings, newopts.statusFlags)
	return nil
}
func (eof *EOFPacket) Write(ctx context.Context, out WriteBuffer) error {
	return out.Write(ctx, eof.data)
}
func (eof *EOFPacket) Close(context.Context) error {
	eof.data = nil
	return nil
}

// make Err packet
func (err *ERRPacket) makeErrPayload(errorCode uint16, sqlState, errorMessage string) []byte {
	data := make([]byte, HeaderOffset+9+len(errorMessage))
	pos := HeaderOffset
	pos = gio.WriteUint8(data, pos, defines.ErrHeader)
	pos = gio.WriteUint16(data, pos, errorCode)
	if err.capability&CLIENT_PROTOCOL_41 != 0 {
		pos = gio.WriteUint8(data, pos, '#')
		if len(sqlState) < 5 {
			stuff := "      "
			sqlState += stuff[:5-len(sqlState)]
		}
		pos = writeStringFix(data, pos, sqlState, 5)
	}
	pos = writeStringFix(data, pos, errorMessage, len(errorMessage))
	return data[:pos]
}

func (err *ERRPacket) Open(_ context.Context, opts ...MysqlWritePacketOpt) error {
	newopts := updateOpts(opts...)
	err.capability = newopts.capability
	err.data = err.makeErrPayload(newopts.errorCode, newopts.sqlState, newopts.errorMessage)
	return nil
}
func (err *ERRPacket) Write(ctx context.Context, out WriteBuffer) error {
	return out.Write(ctx, err.data)
}
func (err *ERRPacket) Close(context.Context) error {
	err.data = nil
	return nil
}

func (eofif *EOFPacketIf) Open(ctx context.Context, opts ...MysqlWritePacketOpt) error {
	newopts := updateOpts(opts...)
	//If the CLIENT_DEPRECATE_EOF client capabilities flag is not set, EOF_Packet
	if newopts.capability&CLIENT_DEPRECATE_EOF == 0 {
		eof := EOFPacket{}
		err := eof.Open(ctx, opts...)
		if err != nil {
			return err
		}
		eofif.data = eof.data
		err = eof.Close(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
func (eofif *EOFPacketIf) Write(ctx context.Context, out WriteBuffer) error {
	return out.Write(ctx, eofif.data)
}
func (eofif *EOFPacketIf) Close(ctx context.Context) error {
	eofif.data = nil
	return nil
}

func (eop *EOFOrOkPacket) Open(ctx context.Context, opts ...MysqlWritePacketOpt) error {
	newopts := updateOpts(opts...)
	//If the CLIENT_DEPRECATE_EOF client capabilities flag is set, OK_Packet; else EOF_Packet.
	if newopts.capability&CLIENT_DEPRECATE_EOF != 0 {
		okeof := OKPacketWithEOF{}
		err := okeof.Open(ctx, opts...)
		if err != nil {
			return err
		}
		eop.data = okeof.data
		err = okeof.Close(ctx)
		if err != nil {
			return err
		}
	} else {
		eof := EOFPacket{}
		err := eof.Open(ctx, opts...)
		if err != nil {
			return err
		}
		eop.data = eof.data
		err = eof.Close(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}
func (eop *EOFOrOkPacket) Write(ctx context.Context, out WriteBuffer) error {
	return out.Write(ctx, eop.data)
}
func (eop *EOFOrOkPacket) Close(context.Context) error {
	eop.data = nil
	return nil
}

func (lef *LengthEncodedNumber) Open(ctx context.Context, opts ...MysqlWritePacketOpt) error {
	newopts := updateOpts(opts...)
	data := make([]byte, HeaderOffset+20)
	pos := HeaderOffset
	pos = writeIntLenEnc(data, pos, newopts.number)
	lef.data = data[:pos]
	return nil
}
func (lef *LengthEncodedNumber) Write(ctx context.Context, out WriteBuffer) error {
	return out.Write(ctx, lef.data)
}
func (lef *LengthEncodedNumber) Close(context.Context) error {
	lef.data = nil
	return nil
}

// make the column information with the format of column definition41
func (def *ColumnDefinition) makeColumnDefinition41Payload(column *MysqlColumn, cmd int) []byte {
	space := HeaderOffset + 8*9 + //lenenc bytes of 8 fields
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
	pos := HeaderOffset

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

func (def *ColumnDefinition) Open(ctx context.Context, opts ...MysqlWritePacketOpt) error {
	newopts := updateOpts(opts...)
	if newopts.capability&CLIENT_PROTOCOL_41 != 0 {
		def.data = def.makeColumnDefinition41Payload(newopts.column, newopts.cmd)
	}
	return nil
}
func (def *ColumnDefinition) Write(ctx context.Context, out WriteBuffer) error {
	return out.Write(ctx, def.data)
}
func (def *ColumnDefinition) Close(context.Context) error {
	def.data = nil
	return nil
}

func (text *ResultSetRowText) Open(ctx context.Context, opts ...MysqlWritePacketOpt) error {
	newopts := updateOpts(opts...)
	text.colData = newopts.colData
	text.colDef = newopts.colDef
	text.lenEncBuffer = newopts.lenEncBuffer
	text.strconvBuffer = newopts.strconvBuffer
	return nil
}

func (text *ResultSetRowText) Write(ctx context.Context, out WriteBuffer) error {
	var err error
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
	return err
}
func (text *ResultSetRowText) Close(context.Context) error {
	text.colData = nil
	text.colDef = nil
	text.lenEncBuffer = nil
	text.strconvBuffer = nil
	return nil
}

func (bin *ResultSetRowBinary) Open(ctx context.Context, opts ...MysqlWritePacketOpt) error {
	newopts := updateOpts(opts...)
	bin.colData = newopts.colData
	bin.colDef = newopts.colDef
	bin.lenEncBuffer = newopts.lenEncBuffer
	bin.strconvBuffer = newopts.strconvBuffer
	return nil
}

func (bin *ResultSetRowBinary) Write(ctx context.Context, out WriteBuffer) error {
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
func (bin *ResultSetRowBinary) Close(context.Context) error {
	bin.colData = nil
	bin.colDef = nil
	bin.lenEncBuffer = nil
	bin.strconvBuffer = nil
	return nil
}
