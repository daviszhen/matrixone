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

import "time"

var (
	feAccounts *Accounts
)

func init() {
	feAccounts = NewAccounts()
}

func NewAccounts() *Accounts {
	accounts := &Accounts{}
	accounts.accountMu.accountId2Conn = make(map[int64]map[*Connection]uint64)
	accounts.accountMu.accountId2Account = make(map[int64]*Account)
	accounts.accountMu.accountName2Account = make(map[string]*Account)
	accounts.queueMu.killIdQueue = make(map[int64]KillRecord)
	return accounts
}

func (accs *Accounts) deleteConn(accId int64, conn *Connection) {
	if accId == sysAccountID || conn == nil {
		return
	}

	accs.accountMu.Lock()
	defer accs.accountMu.Unlock()
	_, ok := accs.accountMu.accountId2Conn[accId]
	if ok {
		delete(accs.accountMu.accountId2Conn[accId], conn)
	}
	if len(accs.accountMu.accountId2Conn[accId]) == 0 {
		delete(accs.accountMu.accountId2Conn, accId)
	}
}

func (accs *Accounts) recordConn(accId int64, conn *Connection, version uint64) {
	if accId == sysAccountID || conn == nil {
		return
	}

	accs.accountMu.Lock()
	defer accs.accountMu.Unlock()
	if _, ok := accs.accountMu.accountId2Conn[accId]; !ok {
		accs.accountMu.accountId2Conn[accId] = make(map[*Connection]uint64)
	}
	accs.accountMu.accountId2Conn[accId][conn] = version
}

func (accs *Accounts) enKillQueue(accId int64, version uint64) {
	if accId == sysAccountID {
		return
	}

	KillRecord := NewKillRecord(time.Now(), version)
	accs.queueMu.Lock()
	defer accs.queueMu.Unlock()
	accs.queueMu.killIdQueue[accId] = KillRecord

}

func (accs *Accounts) alterRoutineStatue(accId int64, status string) {
	if accId == sysAccountID {
		return
	}

	accs.accountMu.Lock()
	defer accs.accountMu.Unlock()
	if rts, ok := accs.accountMu.accountId2Conn[accId]; ok {
		for rt := range rts {
			if status == "restricted" {
				rt.setResricted(true)
			} else {
				rt.setResricted(false)
			}
		}
	}
}

func (accs *Accounts) deepCopyKillQueue() map[int64]KillRecord {
	accs.queueMu.RLock()
	defer accs.queueMu.RUnlock()

	tempKillQueue := make(map[int64]KillRecord, len(accs.queueMu.killIdQueue))
	for account, record := range accs.queueMu.killIdQueue {
		tempKillQueue[account] = record
	}
	return tempKillQueue
}

func (accs *Accounts) deepCopyRoutineMap() map[int64]map[*Connection]uint64 {
	accs.accountMu.RLock()
	defer accs.accountMu.RUnlock()
	tempRoutineMap := make(map[int64]map[*Connection]uint64, len(accs.accountMu.accountId2Conn))
	for account, rountine := range accs.accountMu.accountId2Conn {
		tempRountines := make(map[*Connection]uint64, len(rountine))
		for rt, version := range rountine {
			tempRountines[rt] = version
		}
		tempRoutineMap[account] = tempRountines
	}
	return tempRoutineMap
}
