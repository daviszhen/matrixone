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
package reverse

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"unicode/utf8"
)

var (
	ReverseChar    func(*types.Bytes, *types.Bytes) *types.Bytes
	ReverseVarChar func(*types.Bytes, *types.Bytes) *types.Bytes
)

func init() {
	ReverseChar = reverse
	ReverseVarChar = reverse
}

func reverse(xs *types.Bytes, rs *types.Bytes) *types.Bytes {
	var retCursor uint32

	for idx, offset := range xs.Offsets {
		cursor := offset
		curLen := xs.Lengths[idx]

		// handle with unicode
		if curLen != 0 {
			//reverse
			bytes := xs.Data[cursor : cursor+curLen]
			source := 0
			target := curLen
			for source < len(bytes) {
				r, readed := utf8.DecodeRune(bytes[source:])
				if r == utf8.RuneError {
					return nil
				}

				p := target - uint32(readed)
				w := utf8.EncodeRune(rs.Data[p:], r)
				if w == utf8.RuneError {
					return nil
				}
				source += readed
				target = p
			}
		}

		retCursor += curLen
		rs.Lengths[idx] = xs.Lengths[idx]
		rs.Offsets[idx] = xs.Offsets[idx]
	}

	return rs
}
