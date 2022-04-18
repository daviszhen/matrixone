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

package vm

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// String range instructions and call each operator's string function to show a query plan
func String(ins Instructions, buf *bytes.Buffer) {
	for i, in := range ins {
		if i > 0 {
			buf.WriteString(" -> ")
		}
		stringFunc[in.Op](in.Arg, buf)
	}
}

// Prepare range instructions and do init work for each operator's argument by calling its prepare function
func Prepare(ins Instructions, proc *process.Process) error {
	fmt.Printf("^^^vm.Prepare enter\n")
	defer func() {
		fmt.Printf("^^^vm.Prepare exit\n")
	}()
	for _, in := range ins {
		if err := prepareFunc[in.Op](proc, in.Arg); err != nil {
			return err
		}
	}
	return nil
}

func Run(ins Instructions, proc *process.Process) (bool, error) {
	var ok bool
	var end bool
	var err error
	fmt.Printf("---vm.Run enter---\n")
	defer func() {
		fmt.Printf("---vm.Run exit---\n")
	}()

	//defer func() {
	//	if e := recover(); e != nil {
	//		err = moerr.NewPanicError(e)
	//	}
	//}()
	for _, in := range ins {
		fmt.Printf("---before execFunc in.Op %d proc %p \n",in.Op,proc)
		if ok, err = execFunc[in.Op](proc, in.Arg); err != nil {
			fmt.Printf("---error execFunc in.Op %d proc %p \n",in.Op,proc)
			return ok || end, err
		}
		fmt.Printf("---after execFunc in.Op %d proc %p \n",in.Op,proc)
		if ok { // ok is true shows that at least one operator has done its work
			end = true
		}
	}
	return end, err
}
