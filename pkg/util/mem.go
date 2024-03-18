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

package util

type MemSize struct {
	sz int64
}

func (m *MemSize) Size() uint64 {
	if m == nil {
		return 0
	}
	return uint64(max(0, m.sz))
}

func (m *MemSize) Add(sz int) {
	if m != nil {
		m.sz += int64(sz)
	}
}

func (m *MemSize) Sub(sz int) {
	if m != nil {
		m.sz -= int64(sz)
	}
}
