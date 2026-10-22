// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package system

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeMemoryDomainStatFixture(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "memory.stat")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

// The v2 aggregate kernel counter covers
// kernel_stack+pagetables+percpu+vmalloc+slab but never socket buffers. The
// accounting reader must add the separately exposed sock field exactly once,
// or connection-heavy CNs silently shrink the protective non-Go reserve.
func TestReadCgroupAccountingCountsV2SockOutsideAggregateKernel(t *testing.T) {
	path := writeMemoryDomainStatFixture(t, "anon 2048\n"+
		"file 4096\n"+
		"kernel 1000\n"+
		"kernel_stack 200\n"+
		"pagetables 300\n"+
		"percpu 100\n"+
		"slab 400\n"+
		"vmalloc 0\n"+
		"sock 700\n"+
		"shmem 0\n"+
		"inactive_file 4096\n"+
		"active_file 0\n")
	_, kernel, nonGo := readCgroupAccounting(path)
	require.Equal(t, uint64(1700), kernel)
	require.Equal(t, uint64(1700), nonGo)
}

// With no aggregate kernel counter the zero-kernel fallback still counts the
// slab-side component names, and sock must be added exactly once, not twice.
func TestReadCgroupAccountingZeroKernelFallbackCountsSockOnce(t *testing.T) {
	path := writeMemoryDomainStatFixture(t, "anon 2048\n"+
		"file 0\n"+
		"kernel_stack 100\n"+
		"pagetables 200\n"+
		"percpu 50\n"+
		"slab 250\n"+
		"sock 700\n")
	_, kernel, _ := readCgroupAccounting(path)
	require.Equal(t, uint64(1300), kernel)
}

// The v1 hierarchical counters use total_ names; the total_ mapping in the
// value helper must not double count socket buffers either.
func TestReadCgroupAccountingV1CountsSockOnce(t *testing.T) {
	path := writeMemoryDomainStatFixture(t, "total_cache 0\n"+
		"total_rss 2048\n"+
		"total_kernel_stack 100\n"+
		"total_pagetables 200\n"+
		"total_percpu 50\n"+
		"total_slab 250\n"+
		"total_sock 700\n")
	_, kernel, _ := readCgroupAccounting(path)
	require.Equal(t, uint64(1300), kernel)
}

// A saturating kernel aggregate must leave the accounting non-negative and
// pinned at the uint64 ceiling instead of wrapping into a small value.
func TestReadCgroupAccountingSockAdditionSaturates(t *testing.T) {
	path := writeMemoryDomainStatFixture(t, "kernel 18446744073709551615\n"+"sock 700\n")
	_, kernel, nonGo := readCgroupAccounting(path)
	require.Equal(t, ^uint64(0), kernel)
	require.Equal(t, ^uint64(0), nonGo)
}
