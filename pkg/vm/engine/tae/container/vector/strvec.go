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

package vector

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync/atomic"
	"unsafe"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/types"
)

func StrVectorConstructor(vf common.IVFile, useCompress bool, freeFunc base.MemoryFreeFunc) base.IMemoryNode {
	return NewStrVectorNode(vf, useCompress, freeFunc)
}

func NewStrVector(t types.Type, capacity uint64) *StrVector {
	return &StrVector{
		BaseVector: BaseVector{
			Type:  t,
			VMask: &nulls.Nulls{},
		},
		Data: &types.Bytes{
			Data:    make([]byte, 0),
			Offsets: make([]uint32, 0, capacity),
			Lengths: make([]uint32, 0, capacity),
		},
	}
}

func NewStrVectorNode(vf common.IVFile, useCompress bool, freeFunc base.MemoryFreeFunc) base.IMemoryNode {
	n := &StrVector{
		BaseVector: BaseVector{
			VMask: &nulls.Nulls{},
		},
		File:        vf,
		UseCompress: useCompress,
		FreeFunc:    freeFunc,
		// DataSource: &types.Bytes{
		// 	DataSource:    make([]byte, 0),
		// 	Offsets: make([]uint32, 0),
		// 	Lengths: make([]uint32, 0),
		// },
	}
	return n
}

func NewEmptyStrVector() *StrVector {
	return &StrVector{
		BaseVector: BaseVector{
			VMask: &nulls.Nulls{},
		},
		Data: &types.Bytes{
			Data:    make([]byte, 0),
			Offsets: make([]uint32, 0),
			Lengths: make([]uint32, 0),
		},
	}
}

func (v *StrVector) PlacementNew(t types.Type) {
	v.Type = t
	size := v.File.Stat().OriginSize()
	offsetCap := uint64(size / 2)
	lenCap := uint64(size / 2)
	offsetNode := common.GPool.Alloc(offsetCap)
	lenNode := common.GPool.Alloc(lenCap)
	if v.MNodes == nil {
		v.MNodes = make([]*common.MemNode, 2)
	}
	v.MNodes = append(v.MNodes, offsetNode)
	v.MNodes = append(v.MNodes, lenNode)
	offsets := unsafe.Slice((*uint32)(unsafe.Pointer(&offsetNode.Buf[0])), cap(offsetNode.Buf)/4)[:0]
	lengths := unsafe.Slice((*uint32)(unsafe.Pointer(&lenNode.Buf[0])), cap(lenNode.Buf)/4)[: 0 : lenCap/4]
	v.Data = &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: offsets,
		Lengths: lengths,
	}
}

func (v *StrVector) GetType() container.VectorType {
	return container.StrVec
}

func (v *StrVector) Close() error {
	v.VMask = nil
	v.Data = nil
	return nil
}

func (v *StrVector) Capacity() int {
	return cap(v.Data.Lengths)
}

func (v *StrVector) FreeMemory() {
	if v.MNodes != nil {
		for _, n := range v.MNodes {
			common.GPool.Free(n)
		}
	}
	if v.FreeFunc != nil {
		v.FreeFunc(v)
	}
}

func (v *StrVector) GetMemorySize() uint64 {
	v.RLock()
	defer v.RUnlock()
	return uint64(2*4*len(v.Data.Lengths) + len(v.Data.Data))
}

func (v *StrVector) GetMemoryCapacity() uint64 {
	if v.UseCompress {
		return uint64(v.File.Stat().Size())
	} else {
		return uint64(v.File.Stat().OriginSize())
	}
}

func (v *StrVector) SetValue(idx int, val any) error {
	return errors.New("not supported")
}

func (v *StrVector) GetValue(idx int) (any, error) {
	if idx >= v.Length() || idx < 0 {
		return nil, ErrVecInvalidOffset
	}
	if !v.IsReadonly() {
		v.RLock()
	}
	data := v.Data.Get(int64(idx))
	if !v.IsReadonly() {
		v.RUnlock()
	}
	return data, nil
}

func (v *StrVector) Append(n int, vals any) error {
	if v.IsReadonly() {
		return ErrVecWriteRo
	}
	v.Lock()
	defer v.Unlock()
	err := v.appendWithOffset(0, n, vals)
	if err != nil {
		return err
	}

	mask := v.StatMask & (^container.PosMask)
	pos := uint64(len(v.Data.Lengths)) & container.PosMask
	mask = mask | pos
	if len(v.Data.Lengths) == cap(v.Data.Lengths) {
		mask = mask | container.ReadonlyMask
	}
	atomic.StoreUint64(&v.StatMask, mask)
	return nil
}

func (v *StrVector) appendWithOffset(offset, n int, vals any) error {
	var data [][]byte
	switch v.Type.Oid {
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		data = vals.([][]byte)[offset : offset+n]
	default:
		return ErrVecTypeNotSupport
	}
	if len(v.Data.Offsets)+len(data) > cap(v.Data.Offsets) {
		return ErrVecInvalidOffset
	}
	return v.Data.Append(vals.([][]byte)[offset : offset+n])
}

func (v *StrVector) AppendVector(vec *gvec.Vector, offset int) (n int, err error) {
	if offset < 0 || offset >= gvec.Length(vec) {
		return n, ErrVecInvalidOffset
	}
	if v.IsReadonly() {
		return 0, ErrVecWriteRo
	}
	v.Lock()
	defer v.Unlock()
	n = v.Capacity() - v.Length()
	if n > gvec.Length(vec)-offset {
		n = gvec.Length(vec) - offset
	}
	startRow := v.Length()

	dataBytes := vec.Col.(*types.Bytes)
	insert := make([][]byte, 0, len(dataBytes.Lengths))
	for i := 0; i < len(dataBytes.Lengths); i++ {
		s := dataBytes.Offsets[i]
		e := s + dataBytes.Lengths[i]
		insert = append(insert, dataBytes.Data[s:e])
	}

	err = v.appendWithOffset(offset, n, insert)
	if err != nil {
		return n, err
	}
	if vec.Nsp.Np != nil {
		for row := startRow; row < startRow+gvec.Length(vec); row++ {
			if nulls.Contains(vec.Nsp, uint64(offset+row-startRow)) {
				nulls.Add(v.VMask, uint64(row))
			}
		}
	}
	mask := v.StatMask & (^container.PosMask)
	pos := uint64(len(v.Data.Lengths)) & container.PosMask
	mask = mask | pos
	if len(v.Data.Lengths) == cap(v.Data.Lengths) {
		mask = mask | container.ReadonlyMask
	}
	if nulls.Any(v.VMask) {
		mask = mask | container.HasNullMask
	}
	atomic.StoreUint64(&v.StatMask, mask)

	return n, err
}

func (v *StrVector) SliceReference(start, end int) (container.IVectorReader, error) {
	if !v.IsReadonly() {
		return nil, ErrVecNotRo
	}
	mask := container.ReadonlyMask | (uint64(end-start) & container.PosMask)
	vec := &StrVector{
		BaseVector: BaseVector{
			Type: v.Type,
		},
		Data: v.Data.Window(start, end),
	}
	if v.VMask.Np != nil {
		vmask := nulls.Range(v.VMask, uint64(start), uint64(end), &nulls.Nulls{})
		vec.VMask = vmask
		if nulls.Any(vmask) {
			mask = mask | container.HasNullMask
		}
	} else {
		vec.VMask = &nulls.Nulls{}
	}
	vec.StatMask = mask
	// Here due to using v.Data.Window(), we can't modify the capacity of the
	// internal byte slice, so the Capacity() would return wrong result.
	return vec, nil
}

func (v *StrVector) GetLatestView() IVector {
	if !v.IsReadonly() {
		v.RLock()
		defer v.RUnlock()
	}
	mask := atomic.LoadUint64(&v.StatMask)
	endPos := int(mask & container.PosMask)
	vec := &StrVector{
		BaseVector: BaseVector{
			StatMask: container.ReadonlyMask | mask,
			Type:     v.Type,
		},
		Data: v.Data.Window(0, int(endPos)),
	}
	if mask&container.HasNullMask != 0 {
		if mask&container.ReadonlyMask == 0 {
			vec.VMask = nulls.Range(v.VMask, 0, uint64(endPos), &nulls.Nulls{})
		} else {
			vec.VMask = nulls.Range(v.VMask, 0, uint64(endPos), &nulls.Nulls{})
		}
	} else {
		vec.VMask = &nulls.Nulls{}
	}
	return vec
}

func (v *StrVector) Window(start, end uint32) IVector {
	if !v.IsReadonly() {
		v.RLock()
		defer v.RUnlock()
	}
	mask := atomic.LoadUint64(&v.StatMask)
	endPos := int(mask & container.PosMask)
	mask = mask & ^container.PosMask
	if end > uint32(endPos) {
		end = uint32(endPos)
	}
	newPos := uint64(end-start) & container.PosMask
	newMask := mask | newPos
	vec := &StrVector{
		BaseVector: BaseVector{
			StatMask: container.ReadonlyMask | newMask,
			Type:     v.Type,
		},
		Data: v.Data.Window(int(start), int(end)),
	}
	if mask&container.HasNullMask != 0 {
		if mask&container.ReadonlyMask == 0 {
			var np *roaring64.Bitmap
			if v.VMask != nil {
				np = common.BM64Window(v.VMask.Np, int(start), int(end))
			}
			vec.VMask = &nulls.Nulls{Np: np}
		} else {
			vec.VMask = nulls.Range(v.VMask, uint64(start), uint64(end), &nulls.Nulls{})
		}
	} else {
		vec.VMask = &nulls.Nulls{}
	}
	return vec
}

func (v *StrVector) CopyToVectorWithBuffer(compressed *bytes.Buffer, deCompressed *bytes.Buffer) (*gvec.Vector, error) {
	if atomic.LoadUint64(&v.StatMask)&container.ReadonlyMask == 0 {
		return nil, ErrVecNotRo
	}
	nullSize := 0
	var nullbuf []byte
	var err error
	if nulls.Any(v.VMask) {
		if nullbuf, err = v.VMask.Show(); err != nil {
			return nil, err
		}
		nullSize = len(nullbuf)
	}
	capacity := types.TypeSize + 4 + nullSize + 4
	rows := len(v.Data.Offsets)
	capacity += 4
	if rows > 0 {
		capacity += 4 * rows
		capacity += len(v.Data.Data)
	}
	vec := gvec.New(v.Type)
	deCompressed.Reset()
	if capacity > deCompressed.Cap() {
		deCompressed.Grow(capacity)
	}
	buf := deCompressed.Bytes()
	buf = buf[:capacity]
	dBuf := buf
	copy(dBuf, types.EncodeType(v.Type))
	dBuf = dBuf[types.TypeSize:]
	copy(dBuf, types.EncodeFixed(uint32(nullSize)))
	dBuf = dBuf[4:]
	if nullSize > 0 {
		copy(dBuf, nullbuf)
		dBuf = dBuf[nullSize:]
	}
	copy(dBuf, types.EncodeFixed(uint32(rows)))
	dBuf = dBuf[4:]
	if rows > 0 {
		lenBuf := types.EncodeUint32Slice(v.Data.Lengths)
		copy(dBuf, lenBuf)
		dBuf = dBuf[len(lenBuf):]
		copy(dBuf, v.Data.Data)
	}
	err = vec.Read(buf)
	if err != nil {
		return nil, err
	}
	return vec, nil
}

func (v *StrVector) CopyToVector() (*gvec.Vector, error) {
	if atomic.LoadUint64(&v.StatMask)&container.ReadonlyMask == 0 {
		return nil, ErrVecNotRo
	}
	vec := gvec.New(v.Type)
	vec.Data = v.Data.Data
	switch v.Type.Oid {
	case types.Type_CHAR, types.Type_VARCHAR, types.Type_JSON:
		col := vec.Col.(*types.Bytes)
		col.Data = make([]byte, len(v.Data.Data))
		col.Lengths = make([]uint32, len(v.Data.Lengths))
		col.Offsets = make([]uint32, len(v.Data.Offsets))
		copy(col.Data[0:], v.Data.Data)
		copy(col.Lengths[0:], v.Data.Lengths)
		copy(col.Offsets[0:], v.Data.Offsets)
		vec.Nsp = nulls.Range(v.VMask, uint64(0), uint64(v.Length()), &nulls.Nulls{})
	default:
		return nil, ErrVecTypeNotSupport
	}
	return vec, nil
}

func (vec *StrVector) WriteTo(w io.Writer) (n int64, err error) {
	buf, err := vec.Marshal()
	if err != nil {
		return n, err
	}
	nw, err := w.Write(buf)
	return int64(nw), err
}

func (vec *StrVector) ReadFrom(r io.Reader) (n int64, err error) {
	capBuf := make([]byte, 8)
	_, err = r.Read(capBuf)
	if err != nil {
		return n, err
	}

	// TODO: will remove below os.File type check.
	switch f := r.(type) {
	case *os.File:
		if _, err = f.Seek(0, io.SeekStart); err != nil {
			return
		}
	}
	realSize := types.DecodeFixed[uint64](capBuf)
	buf := make([]byte, realSize)
	_, err = r.Read(buf)
	if err != nil {
		return n, err
	}
	copy(buf[0:], capBuf)
	err = vec.Unmarshal(buf)
	return int64(realSize), err
}

func (vec *StrVector) Unmarshal(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	buf := data
	vec.NodeCapacity = types.DecodeFixed[uint64](buf[:8])
	buf = buf[8:]
	vec.StatMask = types.DecodeFixed[uint64](buf[:8])
	buf = buf[8:]
	vec.Type = types.DecodeType(buf[:types.TypeSize])
	buf = buf[types.TypeSize:]
	nb := types.DecodeFixed[uint32](buf[:4])
	buf = buf[4:]
	if nb > 0 {
		if err := vec.VMask.Read(buf[:nb]); err != nil {
			return err
		}
		buf = buf[nb:]
	}
	cnt := types.DecodeFixed[int32](buf[:4])
	buf = buf[4:]
	if vec.Data != nil {
		vec.Data.Reset()
	} else {
		vec.Data = &types.Bytes{}
	}
	if cnt == 0 {
		return nil
	}
	lengths := types.DecodeUint32Slice(buf[:4*cnt])
	if len(lengths) > cap(vec.Data.Lengths) {
		vec.Data.Offsets = make([]uint32, cnt)
		vec.Data.Lengths = lengths
	} else {
		vec.Data.Lengths = vec.Data.Lengths[:len(lengths)]
		vec.Data.Offsets = vec.Data.Offsets[:len(lengths)]
		copy(vec.Data.Lengths, lengths)
	}
	vec.Data.Data = buf[4*cnt:]
	offset := uint32(0)
	for i, n := range vec.Data.Lengths {
		vec.Data.Offsets[i] = offset
		offset += n
	}

	return nil
}

func (vec *StrVector) Marshal() ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(types.EncodeFixed(uint64(0)))
	buf.Write(types.EncodeFixed(vec.StatMask))
	buf.Write(types.EncodeType(vec.Type))
	nb, err := vec.VMask.Show()
	if err != nil {
		return nil, err
	}
	buf.Write(types.EncodeFixed(uint32(len(nb))))
	if len(nb) > 0 {
		buf.Write(nb)
	}
	cnt := int32(len(vec.Data.Lengths))
	buf.Write(types.EncodeFixed(cnt))
	if cnt > 0 {
		buf.Write(types.EncodeUint32Slice(vec.Data.Lengths))
		buf.Write(vec.Data.Data)
	}

	buffer := buf.Bytes()
	capBuf := types.EncodeFixed(uint64(len(buffer)))
	copy(buffer[0:], capBuf)
	vec.NodeCapacity = uint64(len(buffer))
	return buf.Bytes(), nil
}

func (vec *StrVector) Reset() {
	vec.Data = nil
}
