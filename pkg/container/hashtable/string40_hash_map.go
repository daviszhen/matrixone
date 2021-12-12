// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashtable

import (
	"errors"
	"math/bits"
	"unsafe"
)

type String40HashMapCell struct {
	Hash   uint64
	Key    [5]uint64
	Mapped uint64
}

func (hdr *String40HashMapCell) StrKey() StringRef {
	return StringRef{
		Ptr: (*byte)(unsafe.Pointer(&hdr.Key[0])),
		Len: 40 - (bits.LeadingZeros64(hdr.Key[4]) >> 3),
	}
}

type String40HashMap struct {
	bucketCntBits uint8
	bucketCnt     uint64
	elemCnt       uint64
	maxElemCnt    uint64
	bucketData    []String40HashMapCell
}

func (ht *String40HashMap) Init() {
	ht.bucketCntBits = kInitialBucketCntBits
	ht.bucketCnt = kInitialBucketCnt
	ht.elemCnt = 0
	ht.maxElemCnt = kInitialBucketCnt * kLoadFactorNumerator / kLoadFactorDenominator
	ht.bucketData = make([]String40HashMapCell, kInitialBucketCnt)
}

func (ht *String40HashMap) Insert(hash uint64, key *[5]uint64) uint64 {
	ht.resizeOnDemand(1)

	if hash == 0 {
		hash = Crc32Int320Hash(key)
	}

	empty, _, cell := ht.findBucket(hash, key)
	if empty {
		ht.elemCnt++
		cell.Hash = hash
		cell.Key = *key
		cell.Mapped = ht.elemCnt
	}

	return cell.Mapped
}

func (ht *String40HashMap) InsertBatch(hashes []uint64, keys [][5]uint64, values []uint64) {
	ht.resizeOnDemand(uint64(len(keys)))

	if hashes[0] == 0 {
		Crc32Int320BatchHash(&keys[0], &hashes[0], len(keys))
	}

	for i := range keys {
		empty, _, cell := ht.findBucket(hashes[i], &keys[i])
		if empty {
			ht.elemCnt++
			cell.Hash = hashes[i]
			cell.Key = keys[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
}

func (ht *String40HashMap) InsertBatchWithRing(zs []int64, hashes []uint64, keys [][5]uint64, values []uint64) {
	ht.resizeOnDemand(uint64(len(keys)))

	if hashes[0] == 0 {
		Crc32Int320BatchHash(&keys[0], &hashes[0], len(keys))
	}

	for i := range keys {
		if zs[i] == 0 {
			continue
		}
		empty, _, cell := ht.findBucket(hashes[i], &keys[i])
		if empty {
			ht.elemCnt++
			cell.Hash = hashes[i]
			cell.Key = keys[i]
			cell.Mapped = ht.elemCnt
		}
		values[i] = cell.Mapped
	}
}

func (ht *String40HashMap) Find(hash uint64, key *[5]uint64) uint64 {
	if hash == 0 {
		hash = Crc32Int320Hash(key)
	}

	_, _, cell := ht.findBucket(hash, key)

	return cell.Mapped
}

func (ht *String40HashMap) FindBatch(hashes []uint64, keys [][5]uint64, values []uint64) {
	if hashes[0] == 0 {
		Crc32Int320BatchHash(&keys[0], &hashes[0], len(keys))
	}

	for i := range keys {
		_, _, cell := ht.findBucket(hashes[i], &keys[i])
		values[i] = cell.Mapped
	}
}

func (ht *String40HashMap) findBucket(hash uint64, key *[5]uint64) (empty bool, idx uint64, cell *String40HashMapCell) {
	mask := ht.bucketCnt - 1
	for idx = hash & mask; true; idx = (idx + 1) & mask {
		cell = &ht.bucketData[idx]
		empty = cell.Hash == 0
		if empty || cell.Key == *key {
			return
		}
	}

	return
}

func (ht *String40HashMap) resizeOnDemand(n uint64) {
	targetCnt := ht.elemCnt + n
	if targetCnt <= ht.maxElemCnt {
		return
	}

	newBucketCntBits := ht.bucketCntBits + 2
	newBucketCnt := uint64(1) << newBucketCntBits
	newMaxElemCnt := newBucketCnt * kLoadFactorNumerator / kLoadFactorDenominator
	for newMaxElemCnt < targetCnt {
		newBucketCntBits++
		newBucketCnt <<= 1
		newMaxElemCnt = newBucketCnt * kLoadFactorNumerator / kLoadFactorDenominator
	}

	oldBucketCnt := ht.bucketCnt
	oldBucketData := ht.bucketData

	ht.bucketCntBits = newBucketCntBits
	ht.bucketCnt = newBucketCnt
	ht.maxElemCnt = newMaxElemCnt
	ht.bucketData = make([]String40HashMapCell, newBucketCnt)

	for i := uint64(0); i < oldBucketCnt; i++ {
		cell := &oldBucketData[i]
		if cell.Hash != 0 {
			_, newIdx, _ := ht.findBucket(cell.Hash, &cell.Key)
			ht.bucketData[newIdx] = *cell
		}
	}
}

func (ht *String40HashMap) Cardinality() uint64 {
	return ht.elemCnt
}

type String40HashMapIterator struct {
	table *String40HashMap
	pos   uint64
}

func (it *String40HashMapIterator) Init(ht *String40HashMap) {
	it.table = ht
}

func (it *String40HashMapIterator) Next() (cell *String40HashMapCell, err error) {
	for it.pos < it.table.bucketCnt {
		cell = &it.table.bucketData[it.pos]
		if cell.Hash != 0 {
			break
		}
		it.pos++
	}

	if it.pos >= it.table.bucketCnt {
		err = errors.New("out of range")
		return
	}

	it.pos++

	return
}
