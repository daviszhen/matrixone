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

package overload

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vectorize/neg"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"
)

func UnaryEval(op int, typ types.T, c bool, v *vector.Vector, p *process.Process) (*vector.Vector, error) {
	if os, ok := UnaryOps[op]; ok {
		for _, o := range os {
			if unaryCheck(op, o.Typ, typ) {
				return o.Fn(v, p, c)
			}
		}
	}
	return nil, fmt.Errorf("'%s' not yet implemented for %s", OpName[op], typ)
}

func unaryCheck(op int, arg types.T, val types.T) bool {
	return arg == val
}

var UnaryOps = map[int][]*UnaryOp{
	UnaryMinus: {
		&UnaryOp{
			Typ:        types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(v *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				if v.Ref == 1 || v.Ref == 0 {
					v.Ref = 0
					vs := v.Col.([]int8)
					neg.Int8Neg(vs, vs)
					return v, nil
				}
				vs := v.Col.([]int8)
				vec, err := register.Get(proc, int64(len(vs)), v.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data)
				rs = rs[:len(vs)]
				vec.Col = rs
				vec.Nsp.Set(v.Nsp)
				vec.SetCol(neg.Int8Neg(vs, rs))
				return vec, nil
			},
		},
		&UnaryOp{
			Typ:        types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(v *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				if v.Ref == 1 || v.Ref == 0 {
					v.Ref = 0
					vs := v.Col.([]int16)
					neg.Int16Neg(vs, vs)
					return v, nil
				}
				vs := v.Col.([]int16)
				vec, err := register.Get(proc, int64(len(vs)*2), v.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data)
				rs = rs[:len(vs)]
				vec.Col = rs
				vec.Nsp.Set(v.Nsp)
				vec.SetCol(neg.Int16Neg(vs, rs))
				return vec, nil
			},
		},
		&UnaryOp{
			Typ:        types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(v *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				if v.Ref == 1 || v.Ref == 0 {
					v.Ref = 0
					vs := v.Col.([]int32)
					neg.Int32Neg(vs, vs)
					return v, nil
				}
				vs := v.Col.([]int32)
				vec, err := register.Get(proc, int64(len(vs)*4), v.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data)
				rs = rs[:len(vs)]
				vec.Col = rs
				vec.Nsp.Set(v.Nsp)
				vec.SetCol(neg.Int32Neg(vs, rs))
				return vec, nil
			},
		},
		&UnaryOp{
			Typ:        types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(v *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				if v.Ref == 1 || v.Ref == 0 {
					v.Ref = 0
					vs := v.Col.([]int64)
					neg.Int64Neg(vs, vs)
					return v, nil
				}
				vs := v.Col.([]int64)
				vec, err := register.Get(proc, int64(len(vs)*8), v.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
				rs = rs[:len(vs)]
				vec.Col = rs
				vec.Nsp.Set(v.Nsp)
				vec.SetCol(neg.Int64Neg(vs, rs))
				return vec, nil
			},
		},
		&UnaryOp{
			Typ:        types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(v *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				if v.Ref == 1 || v.Ref == 0 {
					v.Ref = 0
					vs := v.Col.([]float32)
					neg.Float32Neg(vs, vs)
					return v, nil
				}
				vs := v.Col.([]float32)
				vec, err := register.Get(proc, int64(len(vs)*4), v.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data)
				rs = rs[:len(vs)]
				vec.Col = rs
				vec.Nsp.Set(v.Nsp)
				vec.SetCol(neg.Float32Neg(vs, rs))
				return vec, nil
			},
		},
		&UnaryOp{
			Typ:        types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(v *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				if v.Ref == 1 || v.Ref == 0 {
					v.Ref = 0
					vs := v.Col.([]float64)
					neg.Float64Neg(vs, vs)
					return v, nil
				}
				vs := v.Col.([]float64)
				vec, err := register.Get(proc, int64(len(vs)*8), v.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data)
				rs = rs[:len(vs)]
				vec.Col = rs
				vec.Nsp.Set(v.Nsp)
				vec.SetCol(neg.Float64Neg(vs, rs))
				return vec, nil
			},
		},
	},
}
