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

package connector

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	buf.WriteString("pipe connector")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	fmt.Printf("!!!connector enter proc %p!!!\n",proc)
	defer func() {
		fmt.Printf("!!!connector exit proc %p!!!\n",proc)
	}()
	n := arg.(*Argument)
	reg := n.Reg
	bat := proc.Reg.InputBatch
	if bat == nil {
		fmt.Printf("!!!connector 000 proc %p!!!\n",proc)
		select {
		case <-reg.Ctx.Done():
			process.FreeRegisters(proc)
			fmt.Printf("!!!connector -2 -2 -2 proc %p!!!\n",proc)
			return true, nil
		case reg.Ch <- bat:
			fmt.Printf("!!!connector -3 -3 -3 proc %p!!!\n",proc)
			return false, nil
		}
	}
	if len(bat.Zs) == 0 {
		fmt.Printf("!!!connector -1 -1 -1 proc %p!!!\n",proc)
		return false, nil
	}
	fmt.Printf("!!!connector 111 proc %p!!!\n",proc)
	vecs := n.vecs[:0]
	for i := range bat.Vecs {
		if bat.Vecs[i].Or {
			vec, err := vector.Dup(bat.Vecs[i], proc.Mp)
			if err != nil {
				return false, err
			}
			vecs = append(vecs, vec)
		}
	}
	fmt.Printf("!!!connector 222 proc %p!!!\n",proc)
	for i := range bat.Vecs {
		if bat.Vecs[i].Or {
			bat.Vecs[i] = vecs[0]
			vecs = vecs[1:]
		}
	}
	fmt.Printf("!!!connector 333 proc %p!!!\n",proc)
	size := mheap.Size(proc.Mp)
	select {
	case <-reg.Ctx.Done():
		batch.Clean(bat, proc.Mp)
		process.FreeRegisters(proc)
		return true, fmt.Errorf("context is done")
	case reg.Ch <- bat:
		n.Mmu.Alloc(size)
		proc.Mp.Gm.Free(size)
		return false, nil
	}
}
