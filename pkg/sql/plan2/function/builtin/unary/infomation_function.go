// Copyright 2022 Matrix Origin
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

package unary

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

/*
These functions get information from the session and system status.
*/

//
func adapter(vectors []*vector.Vector,
	proc *process.Process,
	resultType types.Type,
	parameterCount int,
	evaluateMemoryCapacityForJobFunc func(proc *process.Process, params ...interface{}) (int, error),
	jobFunc func(proc *process.Process, params ...interface{}) (interface{}, error),
) (*vector.Vector, error) {
	if parameterCount == 0 {
		//step 1: evaluate the capacity for result vector
		capacity, err := evaluateMemoryCapacityForJobFunc(proc)
		if err != nil {
			return nil, err
		}
		//step 2: allocate the memory for the result
		var resultSpace interface{}
		switch resultType.Oid {
		case types.T_varchar, types.T_char:
			resultSpace = &types.Bytes{
				Data:    make([]byte, capacity),
				Offsets: make([]uint32, 1),
				Lengths: make([]uint32, 1),
			}
		case types.T_uint64:
			resultSpace = make([]uint64, 1)
		}
		//step 3: evaluate the function and get the result
		result, err := jobFunc(proc, resultSpace)
		if err != nil {
			return nil, err
		}
		//step 4: fill the result vector
		resultVector := vector.NewConst(resultType)
		vector.SetCol(resultVector, result)
		return resultVector, nil
	} else if parameterCount == 1 {

		inputVector := vectors[0]
		if inputVector.IsScalar() {
			if inputVector.ConstVectorIsNull() {
				return proc.AllocScalarNullVector(resultType), nil
			}
			//step 1: get the input value
			//step 2: evaluate the capacity for result vector
			//step 3: allocate the memory for the result
			//step 4: evaluate the function and get the result
			//step 5: fill the result vector

			var result interface{}
			switch inputVector.Typ.Oid {
			case types.T_varchar, types.T_char:
				//
				//		inputValues := inputVector.Col.(*types.Bytes)
				//TODO: get the result

			}

			resultVector := vector.NewConst(resultType)
			vector.SetCol(resultVector, result)
			return resultVector, nil
		} else {
			//step 1: get the input value
			//step 2: evaluate the capacity for result vector
			//step 3: allocate the memory for the result
			//step 4: evaluate the function and get the result
			//step 5: fill the result vector
			return nil, fmt.Errorf("to implement")
		}
	}
	return nil, nil
}

func evaluateMemoryCapacityForDatabase(proc *process.Process, params ...interface{}) (int, error) {
	if proc.SessionInfo == nil {
		return 0, errorParameterIsInvalid
	}
	return len(proc.SessionInfo.GetDatabase()), nil
}

func doDatabase(proc *process.Process, params ...interface{}) (interface{}, error) {
	result := params[0].(*types.Bytes)
	dbName := proc.SessionInfo.GetDatabase()
	l := len(result.Data)
	copy(result.Data, []byte(dbName)[:l])
	result.Offsets[0] = 0
	result.Lengths[0] = uint32(l)
	return result, nil
}

// Database returns the default (current) database name
func Database(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return adapter(vectors, proc, types.Type{Oid: types.T_varchar, Size: 24},
		0,
		evaluateMemoryCapacityForDatabase,
		doDatabase)
}

func evaluateMemoryCapacityForUser(proc *process.Process, params ...interface{}) (int, error) {
	if proc.SessionInfo == nil {
		return 0, errorParameterIsInvalid
	}
	return len(proc.SessionInfo.GetUser()), nil
}

func doUser(proc *process.Process, params ...interface{}) (interface{}, error) {
	result := params[0].(*types.Bytes)
	dbName := proc.SessionInfo.GetUser()
	l := len(result.Data)
	copy(result.Data, []byte(dbName)[:l])
	result.Offsets[0] = 0
	result.Lengths[0] = uint32(l)
	return result, nil
}

// User returns the user name and host name provided by the client
func User(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return adapter(vectors, proc, types.Type{Oid: types.T_varchar, Size: 24},
		0,
		evaluateMemoryCapacityForUser,
		doUser)
}

func evaluateMemoryCapacityForConnectionID(proc *process.Process, params ...interface{}) (int, error) {
	return 8, nil
}

func doConnectionID(proc *process.Process, params ...interface{}) (interface{}, error) {
	result := params[0].([]uint64)
	result[0] = proc.SessionInfo.ConnectionID
	return result, nil
}

// ConnectionID returns the connection ID (thread ID) for the connection
func ConnectionID(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return adapter(vectors, proc, types.Type{Oid: types.T_uint64, Size: 8},
		0,
		evaluateMemoryCapacityForConnectionID,
		doConnectionID)
}
