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

package indexbuild

import (
	"bytes"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const argName = "index_build"

func (arg *Argument) String(buf *bytes.Buffer) {
	buf.WriteString(argName)
	buf.WriteString(": index build ")
}

func (arg *Argument) Prepare(proc *process.Process) (err error) {
	ap := arg
	if ap.RuntimeFilterSpec == nil {
		panic("there must be runtime filter in index build!")
	}

	ap.ctr = new(container)
	if len(proc.Reg.MergeReceivers) > 1 {
		ap.ctr.InitReceiver(proc, true)
		ap.ctr.isMerge = true
	} else {
		ap.ctr.InitReceiver(proc, false)
	}

	return nil
}

func (arg *Argument) Call(proc *process.Process) (vm.CallResult, error) {
	if err, isCancel := vm.CancelCheck(proc); isCancel {
		return vm.CancelResult, err
	}

	anal := proc.GetAnalyze(arg.GetIdx(), arg.GetParallelIdx(), arg.GetParallelMajor())
	anal.Start()
	defer anal.Stop()
	result := vm.NewCallResult()
	ap := arg
	ctr := ap.ctr
	for {
		switch ctr.state {
		case ReceiveBatch:
			if err := ctr.build(ap, proc, anal, arg.GetIsFirst()); err != nil {
				return result, err
			}
			ctr.state = HandleRuntimeFilter

		case HandleRuntimeFilter:
			if err := ctr.handleRuntimeFilter(ap, proc); err != nil {
				return result, err
			}
			ctr.state = End
		default:
			if ctr.batch != nil {
				proc.PutBatch(ctr.batch)
				ctr.batch = nil
			}
			result.Batch = nil
			result.Status = vm.ExecStop
			return result, nil
		}
	}
}

func (ctr *container) collectBuildBatches(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool) error {
	var err error
	var currentBatch *batch.Batch
	for {
		if ap.ctr.isMerge {
			currentBatch, _, err = ctr.ReceiveFromAllRegs(anal)
		} else {
			currentBatch, _, err = ctr.ReceiveFromSingleReg(0, anal)
		}
		if err != nil {
			return err
		}
		if currentBatch == nil {
			break
		}
		if currentBatch.IsEmpty() {
			proc.PutBatch(currentBatch)
			continue
		}
		anal.Input(currentBatch, isFirst)
		// anal.Alloc(int64(currentBatch.Size())) @todo we need to redesin annalyze memory size
		ctr.batch, err = ctr.batch.AppendWithCopy(proc.Ctx, proc.Mp(), currentBatch)
		if err != nil {
			return err
		}
		proc.PutBatch(currentBatch)
		if ctr.batch.RowCount() > int(ap.RuntimeFilterSpec.UpperLimit) {
			// for index build, can exit early
			return nil
		}
	}
	return nil
}

func (ctr *container) build(ap *Argument, proc *process.Process, anal process.Analyze, isFirst bool) error {
	err := ctr.collectBuildBatches(ap, proc, anal, isFirst)
	if err != nil {
		return err
	}
	return nil
}

func (ctr *container) handleRuntimeFilter(ap *Argument, proc *process.Process) error {
	var runtimeFilter process.RuntimeFilterMessage
	runtimeFilter.Tag = ap.RuntimeFilterSpec.Tag

	if ap.RuntimeFilterSpec.Expr == nil {
		runtimeFilter.Typ = process.RuntimeFilter_PASS
		sendFilter(ap, proc, runtimeFilter)
		return nil
	} else if ctr.batch == nil || ctr.batch.RowCount() == 0 {
		runtimeFilter.Typ = process.RuntimeFilter_DROP
		sendFilter(ap, proc, runtimeFilter)
		return nil
	}

	inFilterCardLimit := ap.RuntimeFilterSpec.UpperLimit

	if ctr.batch.RowCount() > int(inFilterCardLimit) {
		runtimeFilter.Typ = process.RuntimeFilter_PASS
		sendFilter(ap, proc, runtimeFilter)
		return nil
	} else {
		if len(ctr.batch.Vecs) != 1 {
			panic("there must be only 1 vector in index build batch")
		}
		vec := ctr.batch.Vecs[0]
		vec.InplaceSort()
		data, err := vec.MarshalBinary()
		if err != nil {
			return err
		}

		runtimeFilter.Typ = process.RuntimeFilter_IN
		runtimeFilter.Card = int32(vec.Length())
		runtimeFilter.Data = data
		sendFilter(ap, proc, runtimeFilter)
	}
	return nil
}

func sendFilter(ap *Argument, proc *process.Process, runtimeFilter process.RuntimeFilterMessage) {
	anal := proc.GetAnalyze(ap.GetIdx(), ap.GetParallelIdx(), ap.GetParallelMajor())
	sendRuntimeFilterStart := time.Now()
	proc.SendMessage(runtimeFilter)
	anal.WaitStop(sendRuntimeFilterStart)
}
