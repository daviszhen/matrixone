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

package function

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func newUTCFunctionTestProcess(t *testing.T) *process.Process {
	t.Helper()
	proc := testutil.NewProcess(t)
	proc.GetSessionInfo().TimeZone = time.FixedZone("UTC-5", -5*60*60)
	// This UTC instant is still the previous calendar day in the session zone.
	// It ensures UTC functions cannot accidentally use the session time zone.
	proc.Base.UnixTime = time.Date(2024, time.January, 2, 3, 4, 5, 123456789, time.UTC).UnixNano()
	return proc
}

func TestUTCFunctionsUseStatementTimestamp(t *testing.T) {
	proc := newUTCFunctionTestProcess(t)

	date, err := types.ParseDateCast("2024-01-02")
	require.NoError(t, err)
	timeScale0, err := types.ParseTime("03:04:05", 0)
	require.NoError(t, err)
	timeScale3, err := types.ParseTime("03:04:05.123", 3)
	require.NoError(t, err)
	datetimeScale0, err := types.ParseDatetime("2024-01-02 03:04:05", 0)
	require.NoError(t, err)
	datetimeScale3, err := types.ParseDatetime("2024-01-02 03:04:05.123", 3)
	require.NoError(t, err)

	for _, tc := range []struct {
		name   string
		inputs []FunctionTestInput
		expect FunctionTestResult
		fn     fEvalFn
	}{
		{
			name:   "utc_date",
			expect: NewFunctionTestResult(types.T_date.ToType(), false, []types.Date{date}, nil),
			fn:     UtcDate,
		},
		{
			name:   "utc_time without fsp",
			expect: NewFunctionTestResult(types.New(types.T_time, 0, 0), false, []types.Time{timeScale0}, nil),
			fn:     builtInUtcTime,
		},
		{
			name: "utc_time with fsp",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_int64.ToType(), []int64{3}, nil),
			},
			expect: NewFunctionTestResult(types.New(types.T_time, 0, 3), false, []types.Time{timeScale3}, nil),
			fn:     builtInUtcTime,
		},
		{
			name:   "utc_timestamp without fsp",
			expect: NewFunctionTestResult(types.New(types.T_datetime, 0, 0), false, []types.Datetime{datetimeScale0}, nil),
			fn:     UTCTimestamp,
		},
		{
			name: "utc_timestamp with fsp",
			inputs: []FunctionTestInput{
				NewFunctionTestConstInput(types.T_int64.ToType(), []int64{3}, nil),
			},
			expect: NewFunctionTestResult(types.New(types.T_datetime, 0, 3), false, []types.Datetime{datetimeScale3}, nil),
			fn:     UTCTimestamp,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testCase := NewFunctionTestCase(proc, tc.inputs, tc.expect, tc.fn)
			ok, info := testCase.Run()
			require.True(t, ok, info)
		})
	}
}

func TestUTCFunctionsResolve(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []types.Type
		ret  types.T
	}{
		{name: "utc_date", ret: types.T_date},
		{name: "utc_time", ret: types.T_time},
		{name: "utc_time", args: []types.Type{types.T_int64.ToType()}, ret: types.T_time},
		{name: "utc_timestamp", ret: types.T_datetime},
		{name: "utc_timestamp", args: []types.Type{types.T_int64.ToType()}, ret: types.T_datetime},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fn, err := GetFunctionByName(context.Background(), tc.name, tc.args)
			require.NoError(t, err)
			require.Equal(t, tc.ret, fn.GetReturnType().Oid)
		})
	}
}
