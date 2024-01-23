package util

import (
    "fmt"
    "sync/atomic"
)

type CounterPosition int

const (
    PosExecreq CounterPosition = iota
    PosDocomquery
    PosExecutestmt
    PosCompile
    PosRun
    PosGetOverview
    PosTxn
    PosTxnRef
    PosSnapshot
    PosUpdateSnapshot
    PosSnapshotTS
    PosApplySnapshot
    PosRead
    PosWrite
    PosWriteAndCommit
    PosCommit
    PosRollback
    PosAddLockTable
    PosAddWaitLock
)

func (pos CounterPosition) String() string {
    switch pos {
    case PosExecreq:
        return "execreq"
    case PosDocomquery:
        return "docomquery"
    case PosExecutestmt:
        return "executestmt"
    case PosCompile:
        return "compile"
    case PosRun:
        return "run"
    case PosGetOverview:
        return "GetOverview"
    case PosTxn:
        return "Txn"
    case PosTxnRef:
        return "TxnRef"
    case PosSnapshot:
        return "Snapshot"
    case PosUpdateSnapshot:
        return "UpdateSnapshot"
    case PosSnapshotTS:
        return "SnapshotTS"
    case PosApplySnapshot:
        return "ApplySnapshot"
    case PosRead:
        return "Read"
    case PosWrite:
        return "Write"
    case PosWriteAndCommit:
        return "WriteAndCommit"
    case PosCommit:
        return "Commit"
    case PosRollback:
        return "Rollback"
    case PosAddLockTable:
        return "AddLockTable"
    case PosAddWaitLock:
        return "AddWaitLock"
    default:
        return fmt.Sprintf("undefined position %d", pos)
    }
}

type DebugCounter struct {
    counter [2]atomic.Uint64
    info    CounterPosition
}

func NewDebugCounter(pos CounterPosition) *DebugCounter {
    return &DebugCounter{info: pos}
}

func (sc *DebugCounter) Reset() {
    if sc == nil {
        return
    }
    sc.counter[0].Store(0)
    sc.counter[1].Store(0)
}

func (sc *DebugCounter) Close() {
    if sc == nil {
        return
    }
}

func (sc *DebugCounter) AddEnter() {
    if sc == nil {
        return
    }
    a := &sc.counter[0]
    a.Add(1)
}

func (sc *DebugCounter) AddExit() {
    if sc == nil {
        return
    }
    a := &sc.counter[1]
    a.Add(1)
}

func (sc *DebugCounter) String() string {
    if sc == nil {
        return ""
    }
    a := &sc.counter[0]
    b := &sc.counter[1]
    return fmt.Sprintf("%s [enter:%d,exit:%d]", sc.info, a.Load(), b.Load())
}
