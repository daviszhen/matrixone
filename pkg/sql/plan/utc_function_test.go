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

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildUTCFunctions(t *testing.T) {
	for _, sql := range []string{
		"select utc_date()",
		"select utc_time()",
		"select utc_time(3)",
		"select utc_timestamp(3)",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(false), t, sql)
			require.NoError(t, err)
		})
	}
}
