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

package motrace

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/google/gops/agent"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/util/batchpipe"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/util/export/etl"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
	"github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/util/trace"
)

var buf = new(bytes.Buffer)
var err1 = moerr.NewInternalError(context.Background(), "test1")
var err2 = errutil.Wrapf(err1, "test2")
var traceIDSpanIDColumnStr string
var traceIDSpanIDCsvStr string

func noopReportError(context.Context, error, int) {}

var dummyBaseTime time.Time

func init() {
	// Tips: Op 'time.Local = time.FixedZone(...)' would cause DATA RACE against to time.Now()

	CU := *config.NewOBCUConfig()
	CU.SetDefaultValues()

	dummyBaseTime = time.Unix(0, 0).UTC()
	SV := config.ObservabilityParameters{
		CU:   CU,
		CUv1: CU,
	}
	SV.SetDefaultValues("v0.test.0")
	SV.TraceExportInterval = 15
	SV.LongQueryTime = 0
	SV.EnableTraceDebug = true
	if err, _ := InitWithConfig(
		context.Background(),
		&SV,
		EnableTracer(true),
		withMOVersion("v0.test.0"),
		WithNode("node_uuid", trace.NodeTypeStandalone),
		WithFSWriterFactory(&dummyFSWriterFactory{}),
		WithSQLExecutor(func() internalExecutor.InternalExecutor {
			return nil
		}),
	); err != nil {
		panic(err)
	}
	errutil.SetErrorReporter(noopReportError)

	sc := trace.SpanFromContext(DefaultContext()).SpanContext()
	traceIDSpanIDColumnStr = fmt.Sprintf(`"%s", "%s"`, sc.TraceID.String(), sc.SpanID.String())
	traceIDSpanIDCsvStr = fmt.Sprintf(`%s,%s`, sc.TraceID.String(), sc.SpanID.String())

	if err := agent.Listen(agent.Options{}); err != nil {
		_ = moerr.NewInternalErrorf(DefaultContext(), "listen gops agent failed: %s", err)
		panic(err)
	}
	fmt.Println("Finish tests init.")
}

type dummyStringWriter struct {
	buf      *bytes.Buffer
	callback func(*bytes.Buffer)
	backoff  table.BackOff
}

func (w *dummyStringWriter) WriteString(s string) (n int, err error) {
	return fmt.Printf("dummyStringWriter: %s\n", s)
}
func (w *dummyStringWriter) WriteRow(row *table.Row) error {
	fmt.Printf("dummyStringWriter: %v\n", row.ToStrings())
	return nil
}
func (w *dummyStringWriter) SetBuffer(buf *bytes.Buffer, callback func(buffer *bytes.Buffer)) {
	w.buf = buf
	w.callback = callback
}

// NeedBuffer implements table.BufferSettable
func (w *dummyStringWriter) NeedBuffer() bool { return true }
func (w *dummyStringWriter) SetupBackOff(backoff table.BackOff) {
	w.backoff = backoff
}
func (w *dummyStringWriter) FlushAndClose() (int, error) {
	if w.backoff != nil {
		_ = w.backoff.Count()
	}
	if w.callback != nil {
		w.callback(w.buf)
	}
	return 0, nil
}
func (w *dummyStringWriter) GetContent() string    { return "" }
func (w *dummyStringWriter) GetContentLength() int { return 0 }

func (w *dummyStringWriter) Write(p []byte) (n int, err error) {
	return fmt.Printf("dummyStringWriter: %s\n", p)
}

func (w *dummyStringWriter) Close() error { return nil }

type dummyFSWriterFactory struct{}

func (f *dummyFSWriterFactory) GetRowWriter(ctx context.Context, account string, tbl *table.Table, ts time.Time) table.RowWriter {
	return &dummyStringWriter{}
}
func (f *dummyFSWriterFactory) GetWriter(ctx context.Context, fp string) io.WriteCloser {
	return &dummyStringWriter{}
}

func Test_newBuffer2Sql_base(t *testing.T) {

	buf := NewItemBuffer()
	byteBuf := new(bytes.Buffer)
	assert.Equal(t, true, buf.IsEmpty())
	buf.Add(&MOSpan{})
	assert.Equal(t, false, buf.IsEmpty())
	assert.Equal(t, false, buf.ShouldFlush())
	assert.Equal(t, "", buf.GetBatch(context.TODO(), byteBuf))
	buf.Reset()
	assert.Equal(t, true, buf.IsEmpty())
}

func Test_buffer2Sql_IsEmpty(t *testing.T) {
	type fields struct {
		Reminder      batchpipe.Reminder
		buf           []IBuffer2SqlItem
		sizeThreshold int
		batchFunc     genBatchFunc
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "empty",
			fields: fields{
				Reminder:      batchpipe.NewConstantClock(time.Hour),
				buf:           []IBuffer2SqlItem{},
				sizeThreshold: mpool.GB,
				batchFunc:     nil,
			},
			want: true,
		},
		{
			name: "not_empty",
			fields: fields{
				Reminder:      batchpipe.NewConstantClock(time.Hour),
				buf:           []IBuffer2SqlItem{&MOZapLog{}},
				sizeThreshold: mpool.GB,
				batchFunc:     nil,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &itemBuffer{
				buf: tt.fields.buf,
				BufferConfig: BufferConfig{
					Reminder:      tt.fields.Reminder,
					sizeThreshold: tt.fields.sizeThreshold,
					genBatchFunc:  tt.fields.batchFunc,
				},
			}
			if got := b.IsEmpty(); got != tt.want {
				t.Errorf("IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_buffer2Sql_Reset(t *testing.T) {
	type fields struct {
		Reminder      batchpipe.Reminder
		buf           []IBuffer2SqlItem
		sizeThreshold int
		batchFunc     genBatchFunc
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "empty",
			fields: fields{
				Reminder:      batchpipe.NewConstantClock(time.Hour),
				buf:           []IBuffer2SqlItem{},
				sizeThreshold: mpool.GB,
				batchFunc:     nil,
			},
			want: true,
		},
		{
			name: "not_empty",
			fields: fields{
				Reminder:      batchpipe.NewConstantClock(time.Hour),
				buf:           []IBuffer2SqlItem{&MOZapLog{}},
				sizeThreshold: mpool.GB,
				batchFunc:     nil,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &itemBuffer{
				buf: tt.fields.buf,
				BufferConfig: BufferConfig{
					Reminder:      tt.fields.Reminder,
					sizeThreshold: tt.fields.sizeThreshold,
					genBatchFunc:  tt.fields.batchFunc,
				},
			}
			b.Reset()
			if got := b.IsEmpty(); got != tt.want {
				t.Errorf("IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_withSizeThreshold(t *testing.T) {
	type args struct {
		size int64
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{name: "1  B", args: args{size: 1}, want: 1},
		{name: "1 KB", args: args{size: mpool.KB}, want: 1 << 10},
		{name: "1 MB", args: args{size: mpool.MB}, want: 1 << 20},
		{name: "1 GB", args: args{size: mpool.GB}, want: 1 << 30},
		{name: "1.001 GB", args: args{size: mpool.GB + mpool.MB}, want: 1<<30 + 1<<20},
	}
	buf := &itemBuffer{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			BufferWithSizeThreshold(tt.args.size).apply(&buf.BufferConfig)
			if got := buf.sizeThreshold; got != tt.want {
				t.Errorf("BufferWithSizeThreshold() = %v, want %v", got, tt.want)
			}
		})
	}
}

/*
var gCtrlSqlCh = make(chan struct{}, 1)
func Test_batchSqlHandler_NewItemBatchHandler(t1 *testing.T) {
	gCtrlSqlCh <- struct{}{}
	type fields struct {
		defaultOpts []BufferOption
		ch          chan string
	}
	type args struct {
		batch string
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		want   func(batch any)
	}{
		{
			name: "nil",
			fields: fields{
				defaultOpts: []BufferOption{BufferWithSizeThreshold(GB)},
				ch:          make(chan string, 10),
			},
			args: args{
				batch: "batch",
			},
			want: func(batch any) {},
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			WithSQLExecutor(newDummyExecutorFactory(tt.fields.ch)).apply(&GetTracerProvider().tracerProviderConfig)
			t := batchSqlHandler{
				defaultOpts: tt.fields.defaultOpts,
			}

			got := t.NewItemBatchHandler(context.TODO())
			go got(tt.args.batch)
			batch, ok := <-tt.fields.ch
			if ok {
				require.Equal(t1, tt.args.batch, batch)
			} else {
				t1.Log("exec sql Done.")
			}
			//close(tt.fields.ch)
		})
	}
	WithSQLExecutor(func() internalExecutor.InternalExecutor { return nil }).apply(&GetTracerProvider().tracerProviderConfig)
	<-gCtrlSqlCh
}*/

var genFactory = func() table.WriterFactory {
	return table.NewWriterFactoryGetter(
		func(ctx context.Context, account string, tbl *table.Table, ts time.Time) table.RowWriter {
			return etl.NewCSVWriter(ctx, &dummyStringWriter{})
		},
		nil,
	)
}

var message66bytes = "123456789-223456789-323456789-423456789-523456789-623456789-123456"
var extra66bytes = `{"task":"gc-process-4","duration":0.048722765,"soft-gc":27.155712495,"merge-table":0.048722236,"files-to-gc":["0192b0d8-5371-701d-a511-453f42a650fb_00000","0192b0df-2a31-7d20-a8bc-c0444f9da0c5_00000",....]}'`

// Test_genCsvData_long_log ut for https://github.com/matrixorigin/MO-Cloud/issues/4235
func Test_genCsvData_long_log(t *testing.T) {
	// for case 'single_zap_long_long'
	GetTracerProvider().MaxLogMessageSize = 64

	errorFormatter.Store("%v")
	logStackFormatter.Store("%n")
	type args struct {
		in  []IBuffer2SqlItem
		buf *bytes.Buffer
	}
	sc := trace.SpanContextWithIDs(_1TraceID, _1SpanID)
	tests := []struct {
		name string
		args args
		want any
	}{
		{
			name: "single_zap_long_long",
			args: args{
				in: []IBuffer2SqlItem{
					&MOZapLog{
						Level:       zapcore.InfoLevel,
						SpanContext: &sc,
						Timestamp:   dummyBaseTime,
						Caller:      "trace/buffer_pipe_sql_test.go:912",
						Message:     message66bytes,
						Extra:       extra66bytes,
					},
				},
				buf: buf,
			},
			want: `log_info,node_uuid,Standalone,0000000000000001,00000000-0000-0000-0000-000000000001,,1970-01-01 00:00:00.000000,info,trace/buffer_pipe_sql_test.go:912,123456789-223456789-323456789-423456789-523456789-623456789-1234,"{""task"":""gc-process-4"",""duration"":0.048722765,""soft-gc"":27.15571",0,,,,0,0001-01-01 00:00:00.000000,0001-01-01 00:00:00.000000,0,{},internal,,
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := genETLData(context.TODO(), tt.args.in, tt.args.buf, genFactory())
			require.NotEqual(t, nil, got)
			req, ok := got.(table.ExportRequests)
			require.Equal(t, true, ok)
			require.Equal(t, 1, len(req))
			batch := req[0].(*table.RowRequest)
			content := batch.GetContent()
			assert.Equalf(t, tt.want, content, "genETLData(%v, %v)", content, tt.args.buf)
			t.Logf("%s", tt.want)
		})
	}
}

func Test_genCsvData(t *testing.T) {
	errorFormatter.Store("%v")
	logStackFormatter.Store("%n")
	type args struct {
		in  []IBuffer2SqlItem
		buf *bytes.Buffer
	}
	sc := trace.SpanContextWithIDs(_1TraceID, _1SpanID)
	tests := []struct {
		name string
		args args
		want any
	}{
		{
			name: "single_span",
			args: args{
				in: []IBuffer2SqlItem{
					&MOSpan{
						SpanConfig: trace.SpanConfig{SpanContext: trace.SpanContext{TraceID: _1TraceID, SpanID: _1SpanID}, Parent: trace.NoopSpan{}},
						Name:       "span1",
						StartTime:  dummyBaseTime,
						EndTime:    dummyBaseTime.Add(time.Microsecond),
						Duration:   time.Microsecond,
						tracer:     gTracer.(*MOTracer),
					},
				},
				buf: buf,
			},
			want: `span_info,node_uuid,Standalone,0000000000000001,00000000-0000-0000-0000-000000000001,,1970-01-01 00:00:00.000001,,,,{},0,,,span1,0,1970-01-01 00:00:00.000000,1970-01-01 00:00:00.000001,1000,"{""Node"":{""node_uuid"":""node_uuid"",""node_type"":""Standalone""},""version"":""v0.test.0""}",internal,,
`,
		},
		{
			name: "multi_span",
			args: args{
				in: []IBuffer2SqlItem{
					&MOSpan{
						SpanConfig: trace.SpanConfig{SpanContext: trace.SpanContext{TraceID: _1TraceID, SpanID: _1SpanID, Kind: trace.SpanKindStatement}, Parent: trace.NoopSpan{}},
						Name:       "span1",
						StartTime:  dummyBaseTime,
						EndTime:    dummyBaseTime.Add(time.Microsecond),
						Duration:   time.Microsecond,
						tracer:     gTracer.(*MOTracer),
					},
					&MOSpan{
						SpanConfig: trace.SpanConfig{SpanContext: trace.SpanContext{TraceID: _1TraceID, SpanID: _2SpanID, Kind: trace.SpanKindRemote}, Parent: trace.NoopSpan{}},
						Name:       "span2",
						StartTime:  dummyBaseTime.Add(time.Microsecond),
						EndTime:    dummyBaseTime.Add(time.Millisecond),
						Duration:   time.Millisecond - time.Microsecond,
						tracer:     gTracer.(*MOTracer),
					},
					&MOSpan{
						SpanConfig: trace.SpanConfig{SpanContext: trace.SpanContext{TraceID: _1TraceID, SpanID: _2SpanID, Kind: trace.SpanKindRemote}, Parent: trace.NoopSpan{}},
						Name:       "empty_end",
						StartTime:  dummyBaseTime.Add(time.Microsecond),
						Duration:   0,
						tracer:     gTracer.(*MOTracer),
						//EndTime:    table.ZeroTime,
						ExtraFields: []zap.Field{zap.String("str", "field"), zap.Int64("int", 0)},
					},
				},
				buf: buf,
			},
			want: `span_info,node_uuid,Standalone,0000000000000001,00000000-0000-0000-0000-000000000001,,1970-01-01 00:00:00.000001,,,,{},0,,,span1,0,1970-01-01 00:00:00.000000,1970-01-01 00:00:00.000001,1000,"{""Node"":{""node_uuid"":""node_uuid"",""node_type"":""Standalone""},""version"":""v0.test.0""}",statement,,
span_info,node_uuid,Standalone,0000000000000002,00000000-0000-0000-0000-000000000001,,1970-01-01 00:00:00.001000,,,,{},0,,,span2,0,1970-01-01 00:00:00.000001,1970-01-01 00:00:00.001000,999000,"{""Node"":{""node_uuid"":""node_uuid"",""node_type"":""Standalone""},""version"":""v0.test.0""}",remote,,
span_info,node_uuid,Standalone,0000000000000002,00000000-0000-0000-0000-000000000001,,0001-01-01 00:00:00.000000,,,,"{""str"":""field"",""int"":0}",0,,,empty_end,0,1970-01-01 00:00:00.000001,0001-01-01 00:00:00.000000,0,"{""Node"":{""node_uuid"":""node_uuid"",""node_type"":""Standalone""},""version"":""v0.test.0""}",remote,,
`,
		},
		{
			name: "single_zap",
			args: args{
				in: []IBuffer2SqlItem{
					&MOZapLog{
						Level:       zapcore.InfoLevel,
						SpanContext: &sc,
						Timestamp:   dummyBaseTime,
						Caller:      "trace/buffer_pipe_sql_test.go:912",
						Message:     "info message",
						Extra:       "{}",
					},
				},
				buf: buf,
			},
			want: `log_info,node_uuid,Standalone,0000000000000001,00000000-0000-0000-0000-000000000001,,1970-01-01 00:00:00.000000,info,trace/buffer_pipe_sql_test.go:912,info message,{},0,,,,0,0001-01-01 00:00:00.000000,0001-01-01 00:00:00.000000,0,{},internal,,
`,
		},
		{
			name: "multi_zap",
			args: args{
				in: []IBuffer2SqlItem{
					&MOZapLog{
						Level:       zapcore.InfoLevel,
						SpanContext: &sc,
						Timestamp:   dummyBaseTime,
						Caller:      "trace/buffer_pipe_sql_test.go:939",
						Message:     "info message",
						Extra:       "{}",
					},
					&MOZapLog{
						Level:       zapcore.DebugLevel,
						SpanContext: &sc,
						Timestamp:   dummyBaseTime.Add(time.Microsecond + time.Millisecond),
						Caller:      "trace/buffer_pipe_sql_test.go:939",
						Message:     "debug message",
						Extra:       "{}",
					},
				},
				buf: buf,
			},
			want: `log_info,node_uuid,Standalone,0000000000000001,00000000-0000-0000-0000-000000000001,,1970-01-01 00:00:00.000000,info,trace/buffer_pipe_sql_test.go:939,info message,{},0,,,,0,0001-01-01 00:00:00.000000,0001-01-01 00:00:00.000000,0,{},internal,,
log_info,node_uuid,Standalone,0000000000000001,00000000-0000-0000-0000-000000000001,,1970-01-01 00:00:00.001001,debug,trace/buffer_pipe_sql_test.go:939,debug message,{},0,,,,0,0001-01-01 00:00:00.000000,0001-01-01 00:00:00.000000,0,{},internal,,
`,
		},
		{
			name: "single_statement",
			args: args{
				in: []IBuffer2SqlItem{
					&StatementInfo{
						StatementID:          _1TraceID,
						TransactionID:        _1TxnID,
						SessionID:            _1SesID,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            []byte("show tables"),
						StatementFingerprint: "show tables",
						StatementTag:         "",
						ExecPlan:             nil,
						RequestAt:            dummyBaseTime,
						ResponseAt:           dummyBaseTime,
					},
				},
				buf: buf,
			},
			want: `00000000-0000-0000-0000-000000000001,00000000000000000000000000000001,00000000-0000-0000-0000-000000000001,MO,moroot,,system,show tables,,show tables,node_uuid,Standalone,1970-01-01 00:00:00.000000,1970-01-01 00:00:00.000000,0,Running,0,,{},0,0,"[0,0,0,0,0,0,0,0,0,0,0]",,,0,,0,0,0,0.0000
`,
		},
		{
			name: "multi_statement",
			args: args{
				in: []IBuffer2SqlItem{
					&StatementInfo{
						StatementID:          _1TraceID,
						TransactionID:        _1TxnID,
						SessionID:            _1SesID,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            []byte("show tables"),
						StatementFingerprint: "show tables",
						StatementTag:         "",
						ExecPlan:             nil,
						RequestAt:            dummyBaseTime,
						ResponseAt:           dummyBaseTime,
					},
					&StatementInfo{
						StatementID:          _2TraceID,
						TransactionID:        _1TxnID,
						SessionID:            _1SesID,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            []byte("show databases"),
						StatementFingerprint: "show databases",
						StatementTag:         "dcl",
						RequestAt:            dummyBaseTime.Add(time.Microsecond),
						ResponseAt:           dummyBaseTime.Add(time.Microsecond + time.Second),
						Duration:             time.Microsecond + time.Second,
						Status:               StatementStatusFailed,
						Error:                moerr.NewInternalError(DefaultContext(), "test error"),
						ExecPlan:             nil,
					},
				},
				buf: buf,
			},
			want: `00000000-0000-0000-0000-000000000001,00000000000000000000000000000001,00000000-0000-0000-0000-000000000001,MO,moroot,,system,show tables,,show tables,node_uuid,Standalone,1970-01-01 00:00:00.000000,1970-01-01 00:00:00.000000,0,Running,0,,{},0,0,"[0,0,0,0,0,0,0,0,0,0,0]",,,0,,0,0,0,0.0000
00000000-0000-0000-0000-000000000002,00000000000000000000000000000001,00000000-0000-0000-0000-000000000001,MO,moroot,,system,show databases,dcl,show databases,node_uuid,Standalone,1970-01-01 00:00:00.000001,1970-01-01 00:00:01.000001,1000001000,Failed,20101,internal error: test error,{},0,0,"[0,0,0,0,0,0,0,0,0,0,0]",,,0,,0,0,0,0.0000
`,
		},
		{
			name: "single_error",
			args: args{
				in: []IBuffer2SqlItem{
					&MOErrorHolder{Error: err1, Timestamp: dummyBaseTime},
				},
				buf: buf,
			},
			want: `error_info,node_uuid,Standalone,0,,,1970-01-01 00:00:00.000000,,,,{},20101,internal error: test1,internal error: test1,,0,0001-01-01 00:00:00.000000,0001-01-01 00:00:00.000000,0,{},,,
`,
		},
		{
			name: "multi_error",
			args: args{
				in: []IBuffer2SqlItem{
					&MOErrorHolder{Error: err1, Timestamp: dummyBaseTime},
					&MOErrorHolder{Error: err2, Timestamp: dummyBaseTime.Add(time.Millisecond + time.Microsecond)},
				},
				buf: buf,
			},
			want: `error_info,node_uuid,Standalone,0,,,1970-01-01 00:00:00.000000,,,,{},20101,internal error: test1,internal error: test1,,0,0001-01-01 00:00:00.000000,0001-01-01 00:00:00.000000,0,{},,,
error_info,node_uuid,Standalone,0,,,1970-01-01 00:00:00.001001,,,,{},20101,test2: internal error: test1,test2: internal error: test1,,0,0001-01-01 00:00:00.000000,0001-01-01 00:00:00.000000,0,{},,,
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := genETLData(context.TODO(), tt.args.in, tt.args.buf, genFactory())
			require.NotEqual(t, nil, got)
			req, ok := got.(table.ExportRequests)
			require.Equal(t, true, ok)
			require.Equal(t, 1, len(req))
			batch := req[0].(*table.RowRequest)
			content := batch.GetContent()
			assert.Equalf(t, tt.want, content, "genETLData(%v, %v)", content, tt.args.buf)
			t.Logf("%s", tt.want)
		})
	}
}

func Test_genCsvData_diffAccount(t *testing.T) {
	type args struct {
		in  []IBuffer2SqlItem
		buf *bytes.Buffer
	}
	tests := []struct {
		name       string
		args       args
		wantReqCnt int
		want       []string
	}{
		{
			name: "single_statement",
			args: args{
				in: []IBuffer2SqlItem{
					&StatementInfo{
						StatementID:          _1TraceID,
						TransactionID:        _1TxnID,
						SessionID:            _1SesID,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            []byte("show tables"),
						StatementFingerprint: "show tables",
						StatementTag:         "",
						ExecPlan:             nil,
						RequestAt:            dummyBaseTime,
						ResponseAt:           dummyBaseTime,
					},
				},
				buf: buf,
			},
			wantReqCnt: 1,
			want: []string{`00000000-0000-0000-0000-000000000001,00000000000000000000000000000001,00000000-0000-0000-0000-000000000001,MO,moroot,,system,show tables,,show tables,node_uuid,Standalone,1970-01-01 00:00:00.000000,1970-01-01 00:00:00.000000,0,Running,0,,{},0,0,"[0,0,0,0,0,0,0,0,0,0,0]",,,0,,0,0,0,0.0000
`},
		},
		{
			name: "multi_statement",
			args: args{
				in: []IBuffer2SqlItem{
					&StatementInfo{
						StatementID:          _1TraceID,
						TransactionID:        _1TxnID,
						SessionID:            _1SesID,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            []byte("show tables"),
						StatementFingerprint: "show tables",
						StatementTag:         "",
						ExecPlan:             nil,
						RequestAt:            dummyBaseTime,
						ResponseAt:           dummyBaseTime,
					},
					&StatementInfo{
						StatementID:          _2TraceID,
						TransactionID:        _1TxnID,
						SessionID:            _1SesID,
						Account:              "sys",
						User:                 "moroot",
						Database:             "system",
						Statement:            []byte("show databases"),
						StatementFingerprint: "show databases",
						StatementTag:         "dcl",
						RequestAt:            dummyBaseTime.Add(time.Microsecond),
						ResponseAt:           dummyBaseTime.Add(time.Microsecond + time.Second),
						Duration:             time.Microsecond + time.Second,
						Status:               StatementStatusFailed,
						Error:                moerr.NewInternalError(DefaultContext(), "test error"),
						ExecPlan:             nil,
					},
				},
				buf: buf,
			},
			wantReqCnt: 1,
			want: []string{`00000000-0000-0000-0000-000000000001,00000000000000000000000000000001,00000000-0000-0000-0000-000000000001,MO,moroot,,system,show tables,,show tables,node_uuid,Standalone,1970-01-01 00:00:00.000000,1970-01-01 00:00:00.000000,0,Running,0,,{},0,0,"[0,0,0,0,0,0,0,0,0,0,0]",,,0,,0,0,0,0.0000
`, `00000000-0000-0000-0000-000000000002,00000000000000000000000000000001,00000000-0000-0000-0000-000000000001,sys,moroot,,system,show databases,dcl,show databases,node_uuid,Standalone,1970-01-01 00:00:00.000001,1970-01-01 00:00:01.000001,1000001000,Failed,20101,internal error: test error,{},0,0,"[0,0,0,0,0,0,0,0,0,0,0]",,,0,,0,0,0,0.0000
`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := genETLData(DefaultContext(), tt.args.in, tt.args.buf, genFactory())
			require.NotEqual(t, nil, got)
			reqs, ok := got.(table.ExportRequests)
			require.Equal(t, true, ok)
			require.Equal(t, tt.wantReqCnt, len(reqs))
			require.Equal(t, len(tt.args.in), len(tt.want))
			for _, req := range reqs {
				found := false
				batch := req.(*table.RowRequest)
				for idx, w := range tt.want {
					if strings.Contains(batch.GetContent(), w) {
						found = true
						t.Logf("idx %d: %s", idx, w)
					}
				}
				assert.Equalf(t, true, found, "genETLData: %v", batch.GetContent())
			}
		})
	}
}

func Test_genCsvData_LongQueryTime(t *testing.T) {
	errorFormatter.Store("%v")
	logStackFormatter.Store("%n")
	type args struct {
		preapre func([]IBuffer2SqlItem)

		in     []IBuffer2SqlItem
		buf    *bytes.Buffer
		queryT int64
	}
	tests := []struct {
		name string
		args args
		want any
	}{
		{
			name: "multi_statement",
			args: args{
				preapre: func(in []IBuffer2SqlItem) {
					for _, item := range in {
						item.(*StatementInfo).ExecPlan2Stats(context.TODO())
					}
				},
				in: []IBuffer2SqlItem{
					&StatementInfo{
						StatementID:          _1TraceID,
						TransactionID:        _1TxnID,
						SessionID:            _1SesID,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            []byte("show tables"),
						StatementFingerprint: "show tables",
						StatementTag:         "",
						ExecPlan:             nil,
						Duration:             time.Second - time.Nanosecond,
						ResultCount:          1,
					},
					&StatementInfo{
						StatementID:          _1TraceID,
						TransactionID:        _1TxnID,
						SessionID:            _1SesID,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            []byte("show tables"),
						StatementFingerprint: "show tables",
						StatementTag:         "",
						ExecPlan:             NewDummySerializableExecPlan(nil, dummySerializeExecPlan, uuid.UUID(_1TraceID)),
						Duration:             time.Second - time.Nanosecond,
						ResultCount:          2,
						RequestAt:            dummyBaseTime,
						ResponseAt:           dummyBaseTime,
					},
					&StatementInfo{
						StatementID:          _2TraceID,
						TransactionID:        _1TxnID,
						SessionID:            _1SesID,
						Account:              "MO",
						User:                 "moroot",
						Database:             "system",
						Statement:            []byte("show databases"),
						StatementFingerprint: "show databases",
						StatementTag:         "dcl",
						RequestAt:            dummyBaseTime.Add(time.Microsecond),
						ResponseAt:           dummyBaseTime.Add(time.Microsecond + time.Second),
						Duration:             time.Second,
						Status:               StatementStatusFailed,
						Error:                moerr.NewInternalError(DefaultContext(), "test error"),
						ExecPlan:             NewDummySerializableExecPlan(map[string]string{"key": "val"}, dummySerializeExecPlan, uuid.UUID(_2TraceID)),
						SqlSourceType:        "internal",
						ResultCount:          3,
					},
				},
				buf:    buf,
				queryT: int64(time.Second),
			},
			want: `00000000-0000-0000-0000-000000000001,00000000000000000000000000000001,00000000-0000-0000-0000-000000000001,MO,moroot,,system,show tables,,show tables,node_uuid,Standalone,0001-01-01 00:00:00.000000,0001-01-01 00:00:00.000000,999999999,Running,0,,{},0,0,"[0,0,0,0,0,0,0,0,0,0,0]",,,0,,0,1,0,0.0000
00000000-0000-0000-0000-000000000001,00000000000000000000000000000001,00000000-0000-0000-0000-000000000001,MO,moroot,,system,show tables,,show tables,node_uuid,Standalone,1970-01-01 00:00:00.000000,1970-01-01 00:00:00.000000,999999999,Running,0,,"{""code"":200,""message"":""no exec plan""}",0,0,"[5,0,0,0,0,0,0,0,0,0,0]",,,0,,0,2,0,0.0000
00000000-0000-0000-0000-000000000002,00000000000000000000000000000001,00000000-0000-0000-0000-000000000001,MO,moroot,,system,show databases,dcl,show databases,node_uuid,Standalone,1970-01-01 00:00:00.000001,1970-01-01 00:00:01.000001,1000000000,Failed,20101,internal error: test error,"{""key"":""val""}",1,1,"[5,1,2.000,3,4,5,0,0,44.0161,0,0]",,,0,internal,0,3,0,44.0161
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			GetTracerProvider().longQueryTime = tt.args.queryT
			if tt.args.preapre != nil {
				tt.args.preapre(tt.args.in)
			}
			got := genETLData(DefaultContext(), tt.args.in, tt.args.buf, genFactory())
			require.NotEqual(t, nil, got)
			req, ok := got.(table.ExportRequests)
			require.Equal(t, true, ok)
			require.Equal(t, 1, len(req))
			batch := req[0].(*table.RowRequest)
			content := batch.GetContent()
			assert.Equalf(t, tt.want, content, "genETLData(%v, %v)", content, tt.args.buf)
			t.Logf("%s", tt.want)
			GetTracerProvider().longQueryTime = 0
		})
	}
}
