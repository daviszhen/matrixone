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
	"errors"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/lengthutf8"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var (
	errorParameterIsNotString = errors.New("the parameter is not char or varchar")
	errorParameterIsInvalid   = errors.New("invalid parameter")
)

func LengthUTF8(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if len(vectors) == 0 || proc == nil {
		return nil, errorParameterIsInvalid
	}
	if vectors[0] == nil {
		return nil, errorParameterIsInvalid
	}
	inputVector := vectors[0]
	resultType := types.Type{Oid: types.T_uint64, Size: 8}
	resultElementSize := int(resultType.Size)
	if inputVector.IsScalar() && inputVector.ConstVectorIsNull() {
		return proc.AllocScalarNullVector(resultType), nil
	}

	inputValues, ok := inputVector.Col.(*types.Bytes)
	if !ok {
		return nil, errorParameterIsNotString
	}
	resultVector, err := proc.AllocVector(resultType, int64(resultElementSize*len(inputValues.Lengths)))
	if err != nil {
		return nil, err
	}
	resultValues := encoding.DecodeUint64Slice(resultVector.Data)
	resultValues = resultValues[:len(inputValues.Lengths)]
	nulls.Set(resultVector.Nsp, inputVector.Nsp)
	vector.SetCol(resultVector, lengthutf8.StrLengthUTF8(inputValues, resultValues))
	return resultVector, nil
}
