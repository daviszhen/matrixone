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

package fileservice

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

func Test_readCache(t *testing.T) {
	slowCacheReadThreshold = time.Second

	size := int64(128)
	m := NewMemCache(fscache.ConstCapacity(size), nil, nil, "")
	defer m.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*3)
	defer cancel()

	newReadVec := func() *IOVector {
		vec := &IOVector{
			FilePath: "foo",
			Entries: []IOEntry{
				{
					Size: 3,
					ToCacheData: func(reader io.Reader, data []byte, allocator CacheDataAllocator) (fscache.Data, error) {
						cacheData := allocator.AllocateCacheData(1)
						cacheData.Bytes()[0] = 42
						return cacheData, nil
					},
				},
			},
		}
		return vec
	}

	err := readCache(ctx, m, newReadVec())
	assert.NoError(t, err)
}
