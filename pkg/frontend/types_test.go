// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/config"
)

func BenchmarkSessionAllocator(b *testing.B) {
	allocator := NewSessionAllocator(&config.ParameterUnit{
		SV: &config.FrontendParameters{
			GuestMmuLimitation: 1 << 30,
		},
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		slice, err := allocator.Alloc(i%(1<<20) + 1)
		if err != nil {
			b.Fatal(err)
		}
		allocator.Free(slice)
	}
}

func BenchmarkParallelSessionAllocator(b *testing.B) {
	allocator := NewSessionAllocator(&config.ParameterUnit{
		SV: &config.FrontendParameters{
			GuestMmuLimitation: 1 << 30,
		},
	})
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			slice, err := allocator.Alloc(i%(32*(1<<10)) + 1)
			if err != nil {
				b.Fatal(err)
			}
			allocator.Free(slice)
		}
	})
}
