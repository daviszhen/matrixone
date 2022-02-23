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

package tuplecodec

import (
	"fmt"
	"github.com/lni/goutils/random"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"math/rand"
	"strconv"
)

func makeAttributes(ts ...types.T)([]string, []*engine.AttributeDef){
	var names []string
	var attrs []*engine.AttributeDef
	var name string

	gen_attr := func(name string,t types.T) *engine.AttributeDef {
		return &engine.AttributeDef{Attr: engine.Attribute{
			Name:    name,
			Alg:     0,
			Type:    types.Type{
				Oid:       t,
				Size:      0,
				Width:     0,
				Precision: 0,
			},
			Default: engine.DefaultExpr{},
		}}
	}

	for _, t := range ts {
		switch t {
		case types.T_int8:
			name = "T_int8"
		case types.T_int16:
			name = "T_int16"
		case types.T_int32:
			name = "T_int32"
		case types.T_int64:
			name = "T_int64"
		case types.T_uint8:
			name = "T_uint8"
		case types.T_uint16:
			name = "T_uint16"
		case types.T_uint32:
			name = "T_uint32"
		case types.T_uint64:
			name = "T_uint64"
		case types.T_float32:
			name = "T_float32"
		case types.T_float64:
			name = "T_float64"
		case types.T_char, types.T_varchar:
			name = "T_char_varchar"
		case types.T_date:
			name = "T_date"
		case types.T_datetime:
			name = "T_datetime"
		default:
			panic("unsupported vector type")
		}

		names = append(names,name)
		attrs = append(attrs,gen_attr(name,t))
	}
	return names, attrs
}

//makeBatch allocates a batch for test
func makeBatch(batchSize int,attrName []string,cols []*engine.AttributeDef) *batch.Batch {
	batchData := batch.New(true, attrName)

	batchData.Zs = make([]int64,batchSize)
	for i := 0; i < batchSize; i++ {
		batchData.Zs[i] = 1
	}
	//alloc space for vector
	for i := 0; i < len(attrName); i++ {
		vec := vector.New(cols[i].Attr.Type)
		switch vec.Typ.Oid {
		case types.T_int8:
			vec.Col = make([]int8, batchSize)
		case types.T_int16:
			vec.Col = make([]int16, batchSize)
		case types.T_int32:
			vec.Col = make([]int32, batchSize)
		case types.T_int64:
			vec.Col = make([]int64, batchSize)
		case types.T_uint8:
			vec.Col = make([]uint8, batchSize)
		case types.T_uint16:
			vec.Col = make([]uint16, batchSize)
		case types.T_uint32:
			vec.Col = make([]uint32, batchSize)
		case types.T_uint64:
			vec.Col = make([]uint64, batchSize)
		case types.T_float32:
			vec.Col = make([]float32, batchSize)
		case types.T_float64:
			vec.Col = make([]float64, batchSize)
		case types.T_char, types.T_varchar:
			vBytes := &types.Bytes{
				Offsets: make([]uint32, batchSize),
				Lengths: make([]uint32, batchSize),
				Data:    nil,
			}
			vec.Col = vBytes
		case types.T_date:
			vec.Col = make([]types.Date, batchSize)
		case types.T_datetime:
			vec.Col = make([]types.Datetime, batchSize)
		default:
			panic("unsupported vector type")
		}
		batchData.Vecs[i] = vec
	}

	return batchData
}

func randomLines(rowCnt int, attrName []string, cols []*engine.AttributeDef) [][]string{
	var lines [][]string

	for i := 0; i < rowCnt; i++ {
		var line []string
		for j := 0; j < len(attrName); j++ {
			var field string
			var d interface{}
			switch cols[j].Attr.Type.Oid {
			case types.T_int8:
				d = rand.Int31n((1 << 7 -1))
			case types.T_int16:
				d = rand.Int31n((1 << 15 -1))
			case types.T_int32:
				d = rand.Int31()
			case types.T_int64:
				d = rand.Int63()
			case types.T_uint8:
				d = rand.Int31n((1 << 8 -1))
			case types.T_uint16:
				d = rand.Int31n((1 << 16 -1))
			case types.T_uint32:
				d = rand.Int31()
			case types.T_uint64:
				d = rand.Int63()
			case types.T_float32:
				d = rand.Float32()
			case types.T_float64:
				d = rand.Float64()
			case types.T_char, types.T_varchar:
				d = random.String(10)
			case types.T_date:
				d = "2022-02-23"
			case types.T_datetime:
				d = "2022-02-23 00:00:00"
			default:
				panic("unsupported type")
			}
			field = fmt.Sprintf("%v",d)
			line = append(line,field)
		}
		lines = append(lines,line)
	}

	return lines
}

func fillBatch(lines [][]string,batchData *batch.Batch) {
	for i, line := range lines {
		rowIdx := i
		for j, field := range line {
			colIdx := j

			isNullOrEmpty := len(field) == 0 || field == "\\N"

			//put it into batch
			vec := batchData.Vecs[colIdx]
			//vecAttr := batchData.Attrs[colIdx]

			switch vec.Typ.Oid {
			case types.T_int8:
				cols := vec.Col.([]int8)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseInt(field, 10, 8)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = int8(d)
				}
			case types.T_int16:
				cols := vec.Col.([]int16)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseInt(field, 10, 16)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = int16(d)
				}
			case types.T_int32:
				cols := vec.Col.([]int32)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseInt(field, 10, 32)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = int32(d)
				}
			case types.T_int64:
				cols := vec.Col.([]int64)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseInt(field, 10, 64)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = d
				}
			case types.T_uint8:
				cols := vec.Col.([]uint8)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseUint(field, 10, 8)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = uint8(d)
				}
			case types.T_uint16:
				cols := vec.Col.([]uint16)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseUint(field, 10, 16)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = uint16(d)
				}
			case types.T_uint32:
				cols := vec.Col.([]uint32)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseUint(field, 10, 32)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = uint32(d)
				}
			case types.T_uint64:
				cols := vec.Col.([]uint64)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseUint(field, 10, 64)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = uint64(d)
				}
			case types.T_float32:
				cols := vec.Col.([]float32)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					d, err := strconv.ParseFloat(field, 32)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = float32(d)
				}
			case types.T_float64:
				cols := vec.Col.([]float64)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					fs := field
					d, err := strconv.ParseFloat(fs, 64)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = d
				}
			case types.T_char, types.T_varchar:
				vBytes := vec.Col.(*types.Bytes)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
					vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
					vBytes.Lengths[rowIdx] = uint32(len(field))
				} else {
					vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
					vBytes.Data = append(vBytes.Data, field...)
					vBytes.Lengths[rowIdx] = uint32(len(field))
				}
			case types.T_date:
				cols := vec.Col.([]types.Date)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					fs := field
					d, err := types.ParseDate(fs)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = d
				}
			case types.T_datetime:
				cols := vec.Col.([]types.Datetime)
				if isNullOrEmpty {
					nulls.Add(vec.Nsp, uint64(rowIdx))
				} else {
					fs := field
					d, err := types.ParseDatetime(fs)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v", field, err)
						d = 0
					}
					cols[rowIdx] = d
				}
			default:
				panic("unsupported oid")
			}
		}
	}
}