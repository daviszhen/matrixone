package cdc

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/plan/tools"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

func NewPartitioner(
	q disttae.Queue[tools.Pair[*disttae.TableCtx, *disttae.DecoderInput]],
	outputChs map[uint64]chan tools.Pair[*disttae.TableCtx, *disttae.DecoderInput],
) Partitioner {
	return &tableIdPartitioner{
		q:         q,
		outputChs: outputChs,
	}
}

var _ Partitioner = new(tableIdPartitioner)

type tableIdPartitioner struct {
	q         disttae.Queue[tools.Pair[*disttae.TableCtx, *disttae.DecoderInput]]
	outputChs map[uint64]chan tools.Pair[*disttae.TableCtx, *disttae.DecoderInput]
}

func (p tableIdPartitioner) Partition(entry tools.Pair[*disttae.TableCtx, *disttae.DecoderInput]) {
	tableCtx := entry.Key

	if ch, ok := p.outputChs[tableCtx.TableId()]; !ok {
		_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Partitioner: no inputCh found for table{%v}\n", tableCtx.TableId())
	} else {
		ch <- entry
	}
}

func (p tableIdPartitioner) Run(_ context.Context, ar *ActiveRoutine) {
	_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Partitioner: start\n")
	defer fmt.Fprintf(os.Stderr, "^^^^^ Partitioner: end\n")

	for {
		select {
		case <-ar.Pause:
			return

		case <-ar.Cancel:
			return

		default:
			// TODO add a condition variable to avoid busy waiting
			if !p.q.Empty() {
				entry := p.q.Front()
				tableCtx := entry.Key
				decoderInput := entry.Value
				p.q.Pop()

				if decoderInput.IsDDL() {
					//TODO:action on ddl
					continue
				}

				// too many heartbeats, so put here to reduce logs
				//_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Partitioner: {%s} \n", decoderInput.TS().DebugString())
				if decoderInput.IsHeartbeat() {
					p.outputChs[HeartBeatTableId] <- entry
					continue
				}

				_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Partitioner: {%s} [%v(%v)].[%v(%v)]\n",
					decoderInput.TS().DebugString(), tableCtx.Db(), tableCtx.DBId(), tableCtx.Table(), tableCtx.TableId())

				p.Partition(entry)

				_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Partitioner: {%s} [%v(%v)].[%v(%v)], entry pushed\n",
					decoderInput.TS().DebugString(), tableCtx.Db(), tableCtx.DBId(), tableCtx.Table(), tableCtx.TableId())
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}
