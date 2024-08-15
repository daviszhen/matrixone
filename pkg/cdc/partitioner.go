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
	p.outputChs[tableCtx.TableId()] <- entry
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
			if !p.q.Empty() {
				entry := p.q.Front()
				tableCtx := entry.Key
				decoderInput := entry.Value
				p.q.Pop()

				//TODO:process heartbeat.to decoder? to sinker?
				if decoderInput.IsHeartbeat() {
					//_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Partitioner:{%s} heartbeat\n", decoderInput.TS().DebugString())
					continue
				}

				_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Partitioner: {%s} [%v(%v)].[%v(%v)]\n",
					decoderInput.TS().DebugString(), tableCtx.Db(), tableCtx.DBId(), tableCtx.Table(), tableCtx.TableId())

				p.Partition(entry)

				_, _ = fmt.Fprintf(os.Stderr, "^^^^^ Partitioner: {%s} [%v(%v)].[%v(%v)], entry pushed\n",
					decoderInput.TS().DebugString(), tableCtx.Db(), tableCtx.DBId(), tableCtx.Table(), tableCtx.TableId())
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}
}
