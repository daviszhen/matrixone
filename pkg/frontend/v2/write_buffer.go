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
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func addSequenceId(sequenceId *atomic.Uint32, a uint8) {
	sequenceId.Add(uint32(a))
}

func getSequenceId(sequenceId *atomic.Uint32) uint8 {
	return uint8(sequenceId.Load())
}

func setSequenceID(sequenceId *atomic.Uint32, value uint8) {
	sequenceId.Store(uint32(value))
}

func (payloadBuf *MysqlPayloadWriteBuffer) Open(ctx context.Context, opts ...WriteBufferOpt) error {
	newopts := WriteBufferOptions{}
	for _, opt := range opts {
		opt(&newopts)
	}

	payloadBuf.conn = newopts.conn
	payloadBuf.clientAddr = newopts.conn.RemoteAddress()
	payloadBuf.thisAddr = ""
	payloadBuf.sequenceId = newopts.sequenceId
	payloadBuf.resetPacket()
	return nil
}

func (payloadBuf *MysqlPayloadWriteBuffer) addSequenceId(a uint8) {
	payloadBuf.sequenceId.Add(uint32(a))
}

func (payloadBuf *MysqlPayloadWriteBuffer) getSequenceId() uint8 {
	return uint8(payloadBuf.sequenceId.Load())
}

func (payloadBuf *MysqlPayloadWriteBuffer) setSequenceID(value uint8) {
	payloadBuf.sequenceId.Store(uint32(value))
}

func (payloadBuf *MysqlPayloadWriteBuffer) openPacket(_ context.Context) error {
	outbuf := payloadBuf.conn.OutBuf()
	n := 4
	outbuf.Grow(n)
	/*
		offset = writerIndex - readerIndex
		writerIndex = GetReaderIndex() + offset
	*/
	offset := outbuf.GetWriteIndex() - outbuf.GetReadIndex()
	writeIdx := beginWriteIndex(outbuf, offset)
	payloadBuf.beginOffset = offset
	writeIdx += n
	payloadBuf.bytesInOutBuffer += n
	outbuf.SetWriteIndex(writeIdx)
	return nil
}

func (payloadBuf *MysqlPayloadWriteBuffer) closePacket(ctx context.Context, appendZeroPacket bool) error {
	if !payloadBuf.isInPacket() {
		return nil
	}
	outbuf := payloadBuf.conn.OutBuf()
	payLoadLen := outbuf.GetWriteIndex() - beginWriteIndex(outbuf, payloadBuf.beginOffset) - 4
	if payLoadLen < 0 || payLoadLen > int(MaxPayloadSize) {
		return moerr.NewInternalError(ctx, "invalid payload len :%d curWriteIdx %d beginWriteIdx %d ",
			payLoadLen, outbuf.GetWriteIndex(), beginWriteIndex(outbuf, payloadBuf.beginOffset))
	}

	buf := outbuf.RawBuf()
	binary.LittleEndian.PutUint32(buf[beginWriteIndex(outbuf, payloadBuf.beginOffset):], uint32(payLoadLen))
	buf[beginWriteIndex(outbuf, payloadBuf.beginOffset)+3] = payloadBuf.getSequenceId()

	payloadBuf.addSequenceId(1)

	if appendZeroPacket && payLoadLen == int(MaxPayloadSize) { //last 16MB packet,append a zero packet
		//if the size of the last packet is exactly MaxPayloadSize, a zero-size payload should be sent
		err := payloadBuf.openPacket(ctx)
		if err != nil {
			return err
		}
		buf = outbuf.RawBuf()
		binary.LittleEndian.PutUint32(buf[beginWriteIndex(outbuf, payloadBuf.beginOffset):], uint32(0))
		buf[beginWriteIndex(outbuf, payloadBuf.beginOffset)+3] = payloadBuf.getSequenceId()
		payloadBuf.addSequenceId(1)
	}

	payloadBuf.resetPacket()
	return nil
}

// open a new row of the resultset
func (payloadBuf *MysqlPayloadWriteBuffer) openRow(ctx context.Context) error {
	return payloadBuf.openPacket(ctx)
}

// close a finished row of the resultset
func (payloadBuf *MysqlPayloadWriteBuffer) closeRow(ctx context.Context) error {
	err := payloadBuf.closePacket(ctx, true)
	if err != nil {
		return err
	}

	err = payloadBuf.flushOutBuffer()
	if err != nil {
		return err
	}
	return err
}

// flushOutBuffer the data in the outbuf into the network
func (payloadBuf *MysqlPayloadWriteBuffer) flushOutBuffer() error {
	if payloadBuf.bytesInOutBuffer >= payloadBuf.untilBytesInOutbufToFlush {
		payloadBuf.flushCount++
		// FIXME: use a suitable timeout value
		err := payloadBuf.conn.Flush(0)
		if err != nil {
			return err
		}
		payloadBuf.resetFlushOutBuffer()
	}
	return nil
}

func (payloadBuf *MysqlPayloadWriteBuffer) Write(ctx context.Context, elems []byte) error {
	outbuf := payloadBuf.conn.OutBuf()
	n := len(elems)
	i := 0
	curLen := 0
	hasDataLen := 0
	curDataLen := 0
	var err error
	var buf []byte
	for ; i < n; i += curLen {
		if !payloadBuf.isInPacket() {
			err = payloadBuf.openPacket(nil)
			if err != nil {
				return err
			}
		}
		//length of data in the packet
		hasDataLen = outbuf.GetWriteIndex() - beginWriteIndex(outbuf, payloadBuf.beginOffset) - HeaderLengthOfTheProtocol
		curLen = int(MaxPayloadSize) - hasDataLen
		curLen = Min(curLen, n-i)
		if curLen < 0 {
			return moerr.NewInternalError(ctx, "needLen %d < 0. hasDataLen %d n - i %d", curLen, hasDataLen, n-i)
		}
		outbuf.Grow(curLen)
		buf = outbuf.RawBuf()
		writeIdx := outbuf.GetWriteIndex()
		copy(buf[writeIdx:], elems[i:i+curLen])
		writeIdx += curLen
		payloadBuf.bytesInOutBuffer += curLen
		outbuf.SetWriteIndex(writeIdx)

		//> 16MB, split it
		curDataLen = outbuf.GetWriteIndex() - beginWriteIndex(outbuf, payloadBuf.beginOffset) - HeaderLengthOfTheProtocol
		if curDataLen == int(MaxPayloadSize) {
			err = payloadBuf.closePacket(ctx, i+curLen == n)
			if err != nil {
				return err
			}

			err = payloadBuf.flushOutBuffer()
			if err != nil {
				return err
			}
		}
	}
	return err
}
func (payloadBuf *MysqlPayloadWriteBuffer) Close(context.Context) error {
	return nil
}
func (payloadBuf *MysqlPayloadWriteBuffer) Flush(ctx context.Context) error {
	err := payloadBuf.closePacket(ctx, true)
	if err != nil {
		return err
	}
	return payloadBuf.flushOutBuffer()
}

func (endPoint *PacketEndPoint) read() (interface{}, error) {
	msg, err := endPoint.conn.Read(endPoint.option)
	if err != nil {
		if err == io.EOF {
			fmt.Fprintf(os.Stderr, "client close the socket\n")
			return &Packet{}, nil
		}
		return nil, err
	}
	return msg, err
}

func (endPoint *PacketEndPoint) receiveData(ctx context.Context) ([]byte, error) {
	data, err := endPoint.read()
	if err != nil {
		return nil, err
	}

	//get EOF. client close the socket.
	if data == nil {
		fmt.Fprintf(os.Stderr, "client close the socket 2\n")
		return nil, nil
	}

	packet, ok := data.(*Packet)
	setSequenceID(endPoint.sequenceId, uint8(packet.SequenceID+1))
	// var seq = getSequenceId(endPoint.sequenceId)
	if !ok {
		err = moerr.NewInternalError(ctx, "message is not Packet")
		// logError(routine.ses, routine.ses.GetDebugString(),
		// 	"Error occurred",
		// 	zap.Error(err))
		return nil, err
	}

	length := packet.Length
	payload := packet.Payload
	if len(payload) == 0 {
		return nil, moerr.NewInternalError(ctx, "payload is zero")
	}
	for uint32(length) == MaxPayloadSize {
		data, err = endPoint.read()
		if err != nil {
			// logError(routine.ses, routine.ses.GetDebugString(),
			// 	"Failed to read message",
			// 	zap.Error(err))
			return nil, err
		}

		//get EOF. client close the socket.
		if data == nil {
			return nil, nil
		}

		packet, ok = data.(*Packet)
		if !ok {
			err = moerr.NewInternalError(ctx, "message is not Packet")
			// logError(routine.ses, routine.ses.GetDebugString(),
			// 	"An error occurred",
			// 	zap.Error(err))
			return nil, err
		}

		setSequenceID(endPoint.sequenceId, uint8(packet.SequenceID+1))
		// seq = getSequenceId(endPoint.sequenceId)
		payload = append(payload, packet.Payload...)
		length = packet.Length
	}
	return payload, err
}

func (endPoint *PacketEndPoint) ReceivePayload(ctx context.Context) (*mysqlPayload, error) {
	payload, err := endPoint.receiveData(ctx)
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 {
		return nil, nil
	}
	return &mysqlPayload{
		cmd:  CommandType(payload[0]),
		data: payload[1:],
	}, err
}

func (endPoint *PacketEndPoint) ReceivePacket(ctx context.Context, packet MysqlReadPacket) error {
	payload, err := endPoint.receiveData(ctx)
	if err != nil {
		return err
	}
	if len(payload) == 0 {
		return moerr.NewInternalError(ctx, "payload is empty")
	}
	err = packet.Open(ctx)
	if err != nil {
		return err
	}
	return packet.Read(ctx, payload)
}

/*
SendPacket sends the packet to the mysql client.
*/
func (endPoint *PacketEndPoint) SendPacket(ctx context.Context, packet MysqlWritePacket, flush bool) error {
	err := packet.Write(ctx, endPoint.out)
	if err != nil {
		return err
	}
	if flush {
		err = endPoint.out.Flush(ctx)
		if err != nil {
			return err
		}
	}
	return err
}
