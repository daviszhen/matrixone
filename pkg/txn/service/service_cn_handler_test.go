// Copyright 2021 - 2022 Matrix Origin
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

package service

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	"github.com/matrixorigin/matrixone/pkg/txn/storage"
	"github.com/matrixorigin/matrixone/pkg/txn/storage/mem"
	"github.com/stretchr/testify/assert"
)

func TestReadBasic(t *testing.T) {
	sender := newTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := newTestTxnService(t, 1, sender, newTestClock(0))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close())
	}()

	sender.addTxnService(s)

	rTxn := newTestTxn(1, 1)
	resp := readTestData(t, sender, 1, rTxn, 1)
	checkReadResponses(t, resp, "")
}

func TestReadWithSelfWrite(t *testing.T) {
	sender := newTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := newTestTxnService(t, 1, sender, newTestClock(0))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close())
	}()
	sender.addTxnService(s)

	rwTxn := newTestTxn(1, 1)
	checkResponses(t, writeTestData(t, sender, 1, rwTxn, 1))
	checkReadResponses(t, readTestData(t, sender, 1, rwTxn, 1), "1-1-1")
	checkResponses(t, writeTestData(t, sender, 1, rwTxn, 2))
	checkReadResponses(t, readTestData(t, sender, 1, rwTxn, 2), "2-1-1")
}

func TestReadCannotBlockByUncomitted(t *testing.T) {
	sender := newTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := newTestTxnService(t, 1, sender, newTestClock(1))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close())
	}()
	sender.addTxnService(s)

	wTxn := newTestTxn(1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))

	rTxn := newTestTxn(2, 1)
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), "")
}

func TestReadCannotBlockByPreparedIfSnapshotTSIsLEPreparedTS(t *testing.T) {
	sender := newTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := newTestTxnService(t, 1, sender, newTestClock(1))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close())
	}()
	sender.addTxnService(s)

	wTxn := newTestTxn(1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	prepareTestTxn(t, sender, wTxn, 1) // prepare at 2

	rTxn := newTestTxn(2, 1)
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), "")

	rTxn = newTestTxn(2, 2)
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), "")
}

func TestReadWillBlockByPreparedIfSnapshotTSIsGTPreparedTS(t *testing.T) {
	sender := newTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := newTestTxnService(t, 1, sender, newTestClock(1))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close())
	}()
	sender.addTxnService(s)

	wTxn := newTestTxn(1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	prepareTestTxn(t, sender, wTxn, 1) // prepare at 2

	c := make(chan struct{})
	go func() {
		rTxn := newTestTxn(2, 3)
		readTestData(t, sender, 1, rTxn, 1)
		close(c)
	}()
	select {
	case <-c:
		assert.Fail(t, "cannot read")
	case <-time.After(time.Second):

	}
}

func TestReadAfterBlockTxnCommitted(t *testing.T) {
	sender := newTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := newTestTxnService(t, 1, sender, newTestClock(1))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close())
	}()
	sender.addTxnService(s)

	wTxn := newTestTxn(1, 1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	prepareTestTxn(t, sender, wTxn, 1) // prepare at 2

	c := make(chan struct{})
	go func() {
		rTxn := newTestTxn(2, 3)
		checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), "1-1-1")
		close(c)
	}()
	go func() {
		time.Sleep(time.Second)
		wTxn.CommitTS = newTestTimestamp(2) // commit at 2
		checkResponses(t, commitShardWriteData(t, sender, wTxn))
	}()

	select {
	case <-c:
	case <-time.After(time.Minute):
		assert.Fail(t, "cannot read")
	}
}

func TestReadAfterBlockTxnCommittedAndCannotReadCommittedValue(t *testing.T) {
	sender := newTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := newTestTxnService(t, 1, sender, newTestClock(1))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close())
	}()
	sender.addTxnService(s)

	wTxn := newTestTxn(1, 1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	prepareTestTxn(t, sender, wTxn, 1) // prepare at 2

	c := make(chan struct{})
	go func() {
		rTxn := newTestTxn(2, 3)
		checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), "")
		close(c)
	}()
	go func() {
		time.Sleep(time.Second)
		wTxn.CommitTS = newTestTimestamp(3) // commit at 3
		checkResponses(t, commitShardWriteData(t, sender, wTxn))
	}()

	select {
	case <-c:
	case <-time.After(time.Minute):
		assert.Fail(t, "cannot read")
	}
}

func TestReadAfterBlockTxnAborted(t *testing.T) {
	sender := newTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := newTestTxnService(t, 1, sender, newTestClock(1))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close())
	}()
	sender.addTxnService(s)

	wTxn := newTestTxn(1, 1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	prepareTestTxn(t, sender, wTxn, 1) // prepare at 2

	c := make(chan struct{})
	go func() {
		rTxn := newTestTxn(2, 3)
		checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), "")
		close(c)
	}()
	go func() {
		time.Sleep(time.Second)
		checkResponses(t, rollbackShardWriteData(t, sender, wTxn))
	}()

	select {
	case <-c:
	case <-time.After(time.Minute):
		assert.Fail(t, "cannot read")
	}
}

func TestReadCannotBlockByCommittingIfSnapshotTSIsLECommitTS(t *testing.T) {
	sender := newTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := newTestTxnService(t, 1, sender, newTestClock(1))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close())
	}()
	sender.addTxnService(s)

	wTxn := newTestTxn(1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	prepareTestTxn(t, sender, wTxn, 1) // prepare at 2

	wTxn.CommitTS = newTestTimestamp(2)
	assert.NoError(t, s.storage.(*mem.KVTxnStorage).Committing(wTxn))

	rTxn := newTestTxn(2, 1)
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), "")

	rTxn = newTestTxn(2, 2)
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), "")
}

func TestReadWillBlockByCommittingIfSnapshotTSIsGTCommitTS(t *testing.T) {
	sender := newTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := newTestTxnService(t, 1, sender, newTestClock(1))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close())
	}()
	sender.addTxnService(s)

	wTxn := newTestTxn(1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	prepareTestTxn(t, sender, wTxn, 1) // prepare at 2

	wTxn.CommitTS = newTestTimestamp(2)
	assert.NoError(t, s.storage.(*mem.KVTxnStorage).Committing(wTxn))

	c := make(chan struct{})
	go func() {
		rTxn := newTestTxn(2, 3)
		readTestData(t, sender, 1, rTxn, 1)
		close(c)
	}()
	select {
	case <-c:
		assert.Fail(t, "cannot read")
	case <-time.After(time.Second):

	}
}

func TestReadCommitted(t *testing.T) {
	sender := newTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := newTestTxnService(t, 1, sender, newTestClock(1))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close())
	}()
	sender.addTxnService(s)

	wTxn1 := newTestTxn(1, 1, 1) // commit at 2
	checkResponses(t, writeTestData(t, sender, 1, wTxn1, 1))
	checkResponses(t, commitWriteData(t, sender, wTxn1))

	wTxn2 := newTestTxn(2, 1, 1) // commit at 3
	checkResponses(t, writeTestData(t, sender, 1, wTxn2, 2))
	checkResponses(t, commitWriteData(t, sender, wTxn2))

	rTxn := newTestTxn(3, 2)
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), "")
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 2), "")

	rTxn = newTestTxn(3, 3)
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), string(getTestValue(1, wTxn1)))
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 2), "")

	rTxn = newTestTxn(3, 4)
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 1), string(getTestValue(1, wTxn1)))
	checkReadResponses(t, readTestData(t, sender, 1, rTxn, 2), string(getTestValue(2, wTxn2)))
}

func TestWriteBasic(t *testing.T) {
	sender := newTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := newTestTxnService(t, 1, sender, newTestClock(0))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close())
	}()

	sender.addTxnService(s)

	wTxn := newTestTxn(1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))

	kv := s.storage.(*mem.KVTxnStorage).GetUncommittedKV()
	v, ok := kv.Get(getTestKey(1))
	assert.True(t, ok)
	assert.Equal(t, getTestValue(1, wTxn), v)
}

func TestWriteWithWWConflict(t *testing.T) {
	sender := newTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := newTestTxnService(t, 1, sender, newTestClock(0))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close())
	}()

	sender.addTxnService(s)

	wTxn := newTestTxn(1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))

	wTxn2 := newTestTxn(2, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn2, 1),
		newTAEWriteError(storage.ErrWriteConflict))
}

func TestCommitWithSingleDNShard(t *testing.T) {
	sender := newTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s := newTestTxnService(t, 1, sender, newTestClock(1))
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Close())
	}()

	sender.addTxnService(s)

	n := byte(10)
	wTxn := newTestTxn(1, 1, 1)
	for i := byte(0); i < n; i++ {
		checkResponses(t, writeTestData(t, sender, 1, wTxn, i))
	}
	checkResponses(t, commitWriteData(t, sender, wTxn))

	for i := byte(0); i < n; i++ {
		var values [][]byte
		var timestamps []timestamp.Timestamp
		kv := s.storage.(*mem.KVTxnStorage).GetCommittedKV()

		kv.AscendRange(getTestKey(i), newTestTimestamp(0), newTestTimestamp(math.MaxInt64), func(value []byte, ts timestamp.Timestamp) {
			values = append(values, value)
			timestamps = append(timestamps, ts)
		})
		assert.Equal(t, [][]byte{getTestValue(i, wTxn)}, values)
		assert.Equal(t, []timestamp.Timestamp{newTestTimestamp(2)}, timestamps)
	}
}

func TestCommitWithMultiDNShards(t *testing.T) {
	sender := newTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s1 := newTestTxnService(t, 1, sender, newTestClock(1))
	assert.NoError(t, s1.Start())
	defer func() {
		assert.NoError(t, s1.Close())
	}()
	s2 := newTestTxnService(t, 2, sender, newTestClock(1))
	assert.NoError(t, s2.Start())
	defer func() {
		assert.NoError(t, s2.Close())
	}()

	sender.addTxnService(s1)
	sender.addTxnService(s2)

	wTxn := newTestTxn(1, 1, 1, 2)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	checkResponses(t, writeTestData(t, sender, 2, wTxn, 2))

	w1 := addTestWaiter(t, s1, wTxn, txn.TxnStatus_Committed)
	defer w1.close()
	w2 := addTestWaiter(t, s2, wTxn, txn.TxnStatus_Committed)
	defer w2.close()

	checkResponses(t, commitWriteData(t, sender, wTxn))

	checkWaiter(t, w1, txn.TxnStatus_Committed)
	checkWaiter(t, w2, txn.TxnStatus_Committed)

	checkData(t, wTxn, s1, 2, 1, true)
	checkData(t, wTxn, s2, 2, 2, true)
}

func TestCommitWithRollbackIfAnyPrepareFailed(t *testing.T) {
	sender := newTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s1 := newTestTxnService(t, 1, sender, newTestClock(1))
	assert.NoError(t, s1.Start())
	defer func() {
		assert.NoError(t, s1.Close())
	}()
	s2 := newTestTxnService(t, 2, sender, newTestClock(1))
	assert.NoError(t, s2.Start())
	defer func() {
		assert.NoError(t, s2.Close())
	}()

	sender.addTxnService(s1)
	sender.addTxnService(s2)

	wTxn1 := newTestTxn(1, 1, 1)
	writeTestData(t, sender, 1, wTxn1, 1)
	checkResponses(t, commitWriteData(t, sender, wTxn1)) // commit at 2

	wTxn := newTestTxn(1, 1, 1, 2)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	checkResponses(t, writeTestData(t, sender, 2, wTxn, 2))

	w1 := addTestWaiter(t, s1, wTxn, txn.TxnStatus_Aborted)
	defer w1.close()
	w2 := addTestWaiter(t, s2, wTxn, txn.TxnStatus_Aborted)
	defer w2.close()

	checkResponses(t, commitWriteData(t, sender, wTxn), newTAEPrepareError(storage.ErrWriteConflict))

	checkWaiter(t, w1, txn.TxnStatus_Aborted)
	checkWaiter(t, w2, txn.TxnStatus_Aborted)

	checkData(t, wTxn, s1, 2, 2, false)
	checkData(t, wTxn, s2, 2, 2, false)
}

func TestRollback(t *testing.T) {
	sender := newTestSender()
	defer func() {
		assert.NoError(t, sender.Close())
	}()

	s1 := newTestTxnService(t, 1, sender, newTestClock(1))
	assert.NoError(t, s1.Start())
	defer func() {
		assert.NoError(t, s1.Close())
	}()
	s2 := newTestTxnService(t, 2, sender, newTestClock(1))
	assert.NoError(t, s2.Start())
	defer func() {
		assert.NoError(t, s2.Close())
	}()

	sender.addTxnService(s1)
	sender.addTxnService(s2)

	wTxn := newTestTxn(1, 1, 1)
	checkResponses(t, writeTestData(t, sender, 1, wTxn, 1))
	wTxn.DNShards = append(wTxn.DNShards, newTestDNShard(2))
	checkResponses(t, writeTestData(t, sender, 2, wTxn, 2))

	w1 := addTestWaiter(t, s1, wTxn, txn.TxnStatus_Aborted)
	defer w1.close()
	w2 := addTestWaiter(t, s2, wTxn, txn.TxnStatus_Aborted)
	defer w2.close()

	checkResponses(t, rollbackWriteData(t, sender, wTxn))

	checkWaiter(t, w1, txn.TxnStatus_Aborted)
	checkWaiter(t, w2, txn.TxnStatus_Aborted)

	checkData(t, wTxn, s1, 2, 0, false)
	checkData(t, wTxn, s2, 2, 0, false)
}

func writeTestData(t *testing.T, sender rpc.TxnSender, toShard uint64, wTxn txn.TxnMeta, keys ...byte) []txn.TxnResponse {
	var requests []txn.TxnRequest
	for _, k := range keys {
		requests = append(requests, newTestWriteRequest(k, wTxn, toShard))
	}
	result, err := sender.Send(context.Background(), requests)
	assert.NoError(t, err)
	responses := result.Responses
	assert.Equal(t, len(keys), len(responses))
	return responses
}

func commitShardWriteData(t *testing.T, sender rpc.TxnSender, wTxn txn.TxnMeta) []txn.TxnResponse {
	result, err := sender.Send(context.Background(), []txn.TxnRequest{newTestCommitShardRequest(wTxn)})
	assert.NoError(t, err)
	responses := result.Responses
	return responses
}

func rollbackShardWriteData(t *testing.T, sender rpc.TxnSender, wTxn txn.TxnMeta) []txn.TxnResponse {
	result, err := sender.Send(context.Background(), []txn.TxnRequest{newTestRollbackShardRequest(wTxn)})
	assert.NoError(t, err)
	responses := result.Responses
	return responses
}

func commitWriteData(t *testing.T, sender rpc.TxnSender, wTxn txn.TxnMeta) []txn.TxnResponse {
	result, err := sender.Send(context.Background(), []txn.TxnRequest{newTestCommitRequest(wTxn)})
	assert.NoError(t, err)
	responses := result.Responses
	return responses
}

func rollbackWriteData(t *testing.T, sender rpc.TxnSender, wTxn txn.TxnMeta) []txn.TxnResponse {
	result, err := sender.Send(context.Background(), []txn.TxnRequest{newTestRollbackRequest(wTxn)})
	assert.NoError(t, err)
	responses := result.Responses
	return responses
}

func readTestData(t *testing.T, sender rpc.TxnSender, toShard uint64, rTxn txn.TxnMeta, keys ...byte) []txn.TxnResponse {
	var requests []txn.TxnRequest
	for _, k := range keys {
		requests = append(requests, newTestReadRequest(k, rTxn, toShard))
	}
	result, err := sender.Send(context.Background(), requests)
	assert.NoError(t, err)
	responses := result.Responses
	assert.Equal(t, len(keys), len(responses))
	return responses
}

func checkReadResponses(t *testing.T, response []txn.TxnResponse, expectValues ...string) {
	for idx, resp := range response {
		values := mem.MustParseGetPayload(resp.CNOpResponse.Payload)
		assert.Equal(t, expectValues[idx], string(values[0]))
	}
}

func checkResponses(t *testing.T, response []txn.TxnResponse, expectErrors ...*txn.TxnError) {
	if len(expectErrors) == 0 {
		expectErrors = make([]*txn.TxnError, len(response))
	}
	for idx, resp := range response {
		assert.Equal(t, expectErrors[idx], resp.TxnError)
	}
}

func checkData(t *testing.T, wTxn txn.TxnMeta, s *service, commitTS int64, k byte, committed bool) {
	assert.Nil(t, s.getTxnContext(wTxn.ID))

	kv := s.storage.(*mem.KVTxnStorage)

	if committed {
		kv.RLock()
		v, ok := kv.GetCommittedKV().Get(getTestKey(k), newTestTimestamp(commitTS))
		assert.True(t, ok)
		assert.Equal(t, getTestValue(k, wTxn), v)
		kv.RUnlock()
	} else {
		kv.RLock()
		n := 0
		kv.GetCommittedKV().AscendRange(getTestKey(k),
			newTestTimestamp(commitTS).Next(),
			newTestTimestamp(math.MaxInt64), func(b []byte, t timestamp.Timestamp) {
				n++
			})
		assert.Equal(t, 0, n)
		kv.RUnlock()
	}

	v, ok := kv.GetUncommittedKV().Get(getTestKey(k))
	assert.False(t, ok)
	assert.Empty(t, v)

	assert.Nil(t, kv.GetUncommittedTxn(wTxn.ID))
}

func addTestWaiter(t *testing.T, s *service, wTxn txn.TxnMeta, status txn.TxnStatus) *waiter {
	txnCtx := s.getTxnContext(wTxn.ID)
	assert.NotNil(t, txnCtx)
	w := acquireWaiter()
	assert.True(t, txnCtx.addWaiter(wTxn.ID, w, status))
	return w
}

func checkWaiter(t *testing.T, w *waiter, expectStatus txn.TxnStatus) {
	status, err := w.wait(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, expectStatus, status)
}
