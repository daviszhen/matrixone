// Code generated by MockGen. DO NOT EDIT.
// Source: pkg/vm/engine/types.go

// Package mock_engine is a generated GoMock package.
package mock_frontend

import (
	reflect "reflect"

	roaring64 "github.com/RoaringBitmap/roaring/roaring64"
	gomock "github.com/golang/mock/gomock"
	batch "github.com/matrixorigin/matrixone/pkg/container/batch"
	engine "github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// MockStatistics is a mock of Statistics interface.
type MockStatistics struct {
	ctrl     *gomock.Controller
	recorder *MockStatisticsMockRecorder
}

// MockStatisticsMockRecorder is the mock recorder for MockStatistics.
type MockStatisticsMockRecorder struct {
	mock *MockStatistics
}

// NewMockStatistics creates a new mock instance.
func NewMockStatistics(ctrl *gomock.Controller) *MockStatistics {
	mock := &MockStatistics{ctrl: ctrl}
	mock.recorder = &MockStatisticsMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockStatistics) EXPECT() *MockStatisticsMockRecorder {
	return m.recorder
}

// Rows mocks base method.
func (m *MockStatistics) Rows() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Rows")
	ret0, _ := ret[0].(int64)
	return ret0
}

// Rows indicates an expected call of Rows.
func (mr *MockStatisticsMockRecorder) Rows() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Rows", reflect.TypeOf((*MockStatistics)(nil).Rows))
}

// Size mocks base method.
func (m *MockStatistics) Size(arg0 string) int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Size", arg0)
	ret0, _ := ret[0].(int64)
	return ret0
}

// Size indicates an expected call of Size.
func (mr *MockStatisticsMockRecorder) Size(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Size", reflect.TypeOf((*MockStatistics)(nil).Size), arg0)
}

// MockTableDef is a mock of TableDef interface.
type MockTableDef struct {
	ctrl     *gomock.Controller
	recorder *MockTableDefMockRecorder
}

// MockTableDefMockRecorder is the mock recorder for MockTableDef.
type MockTableDefMockRecorder struct {
	mock *MockTableDef
}

// NewMockTableDef creates a new mock instance.
func NewMockTableDef(ctrl *gomock.Controller) *MockTableDef {
	mock := &MockTableDef{ctrl: ctrl}
	mock.recorder = &MockTableDefMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTableDef) EXPECT() *MockTableDefMockRecorder {
	return m.recorder
}

// tableDef mocks base method.
func (m *MockTableDef) tableDef() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "tableDef")
}

// tableDef indicates an expected call of tableDef.
func (mr *MockTableDefMockRecorder) tableDef() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "tableDef", reflect.TypeOf((*MockTableDef)(nil).tableDef))
}

// MockRelation is a mock of Relation interface.
type MockRelation struct {
	ctrl     *gomock.Controller
	recorder *MockRelationMockRecorder
}

// MockRelationMockRecorder is the mock recorder for MockRelation.
type MockRelationMockRecorder struct {
	mock *MockRelation
}

// NewMockRelation creates a new mock instance.
func NewMockRelation(ctrl *gomock.Controller) *MockRelation {
	mock := &MockRelation{ctrl: ctrl}
	mock.recorder = &MockRelationMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRelation) EXPECT() *MockRelationMockRecorder {
	return m.recorder
}

// AddTableDef mocks base method.
func (m *MockRelation) AddTableDef(arg0 uint64, arg1 engine.TableDef) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddTableDef", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// AddTableDef indicates an expected call of AddTableDef.
func (mr *MockRelationMockRecorder) AddTableDef(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddTableDef", reflect.TypeOf((*MockRelation)(nil).AddTableDef), arg0, arg1)
}

// Close mocks base method.
func (m *MockRelation) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockRelationMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockRelation)(nil).Close))
}

// DelTableDef mocks base method.
func (m *MockRelation) DelTableDef(arg0 uint64, arg1 engine.TableDef) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DelTableDef", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// DelTableDef indicates an expected call of DelTableDef.
func (mr *MockRelationMockRecorder) DelTableDef(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DelTableDef", reflect.TypeOf((*MockRelation)(nil).DelTableDef), arg0, arg1)
}

// ID mocks base method.
func (m *MockRelation) ID() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ID")
	ret0, _ := ret[0].(string)
	return ret0
}

// ID indicates an expected call of ID.
func (mr *MockRelationMockRecorder) ID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ID", reflect.TypeOf((*MockRelation)(nil).ID))
}

// NewReader mocks base method.
func (m *MockRelation) NewReader(arg0 int) []engine.Reader {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewReader", arg0)
	ret0, _ := ret[0].([]engine.Reader)
	return ret0
}

// NewReader indicates an expected call of NewReader.
func (mr *MockRelationMockRecorder) NewReader(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewReader", reflect.TypeOf((*MockRelation)(nil).NewReader), arg0)
}

// Nodes mocks base method.
func (m *MockRelation) Nodes() engine.Nodes {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Nodes")
	ret0, _ := ret[0].(engine.Nodes)
	return ret0
}

// Nodes indicates an expected call of Nodes.
func (mr *MockRelationMockRecorder) Nodes() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Nodes", reflect.TypeOf((*MockRelation)(nil).Nodes))
}

// Rows mocks base method.
func (m *MockRelation) Rows() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Rows")
	ret0, _ := ret[0].(int64)
	return ret0
}

// Rows indicates an expected call of Rows.
func (mr *MockRelationMockRecorder) Rows() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Rows", reflect.TypeOf((*MockRelation)(nil).Rows))
}

// Size mocks base method.
func (m *MockRelation) Size(arg0 string) int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Size", arg0)
	ret0, _ := ret[0].(int64)
	return ret0
}

// Size indicates an expected call of Size.
func (mr *MockRelationMockRecorder) Size(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Size", reflect.TypeOf((*MockRelation)(nil).Size), arg0)
}

// TableDefs mocks base method.
func (m *MockRelation) TableDefs() []engine.TableDef {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TableDefs")
	ret0, _ := ret[0].([]engine.TableDef)
	return ret0
}

// TableDefs indicates an expected call of TableDefs.
func (mr *MockRelationMockRecorder) TableDefs() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TableDefs", reflect.TypeOf((*MockRelation)(nil).TableDefs))
}

// Write mocks base method.
func (m *MockRelation) Write(arg0 uint64, arg1 *batch.Batch) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Write", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Write indicates an expected call of Write.
func (mr *MockRelationMockRecorder) Write(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Write", reflect.TypeOf((*MockRelation)(nil).Write), arg0, arg1)
}

// MockReader is a mock of Reader interface.
type MockReader struct {
	ctrl     *gomock.Controller
	recorder *MockReaderMockRecorder
}

// MockReaderMockRecorder is the mock recorder for MockReader.
type MockReaderMockRecorder struct {
	mock *MockReader
}

// NewMockReader creates a new mock instance.
func NewMockReader(ctrl *gomock.Controller) *MockReader {
	mock := &MockReader{ctrl: ctrl}
	mock.recorder = &MockReaderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockReader) EXPECT() *MockReaderMockRecorder {
	return m.recorder
}

// NewFilter mocks base method.
func (m *MockReader) NewFilter() engine.Filter {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewFilter")
	ret0, _ := ret[0].(engine.Filter)
	return ret0
}

// NewFilter indicates an expected call of NewFilter.
func (mr *MockReaderMockRecorder) NewFilter() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewFilter", reflect.TypeOf((*MockReader)(nil).NewFilter))
}

// NewSparseFilter mocks base method.
func (m *MockReader) NewSparseFilter() engine.SparseFilter {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewSparseFilter")
	ret0, _ := ret[0].(engine.SparseFilter)
	return ret0
}

// NewSparseFilter indicates an expected call of NewSparseFilter.
func (mr *MockReaderMockRecorder) NewSparseFilter() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewSparseFilter", reflect.TypeOf((*MockReader)(nil).NewSparseFilter))
}

// NewSummarizer mocks base method.
func (m *MockReader) NewSummarizer() engine.Summarizer {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewSummarizer")
	ret0, _ := ret[0].(engine.Summarizer)
	return ret0
}

// NewSummarizer indicates an expected call of NewSummarizer.
func (mr *MockReaderMockRecorder) NewSummarizer() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewSummarizer", reflect.TypeOf((*MockReader)(nil).NewSummarizer))
}

// Read mocks base method.
func (m *MockReader) Read(arg0 []uint64, arg1 []string) (*batch.Batch, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Read", arg0, arg1)
	ret0, _ := ret[0].(*batch.Batch)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Read indicates an expected call of Read.
func (mr *MockReaderMockRecorder) Read(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockReader)(nil).Read), arg0, arg1)
}

// MockFilter is a mock of Filter interface.
type MockFilter struct {
	ctrl     *gomock.Controller
	recorder *MockFilterMockRecorder
}

// MockFilterMockRecorder is the mock recorder for MockFilter.
type MockFilterMockRecorder struct {
	mock *MockFilter
}

// NewMockFilter creates a new mock instance.
func NewMockFilter(ctrl *gomock.Controller) *MockFilter {
	mock := &MockFilter{ctrl: ctrl}
	mock.recorder = &MockFilterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockFilter) EXPECT() *MockFilterMockRecorder {
	return m.recorder
}

// Btw mocks base method.
func (m *MockFilter) Btw(arg0 string, arg1, arg2 interface{}) (*roaring64.Bitmap, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Btw", arg0, arg1, arg2)
	ret0, _ := ret[0].(*roaring64.Bitmap)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Btw indicates an expected call of Btw.
func (mr *MockFilterMockRecorder) Btw(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Btw", reflect.TypeOf((*MockFilter)(nil).Btw), arg0, arg1, arg2)
}

// Eq mocks base method.
func (m *MockFilter) Eq(arg0 string, arg1 interface{}) (*roaring64.Bitmap, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Eq", arg0, arg1)
	ret0, _ := ret[0].(*roaring64.Bitmap)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Eq indicates an expected call of Eq.
func (mr *MockFilterMockRecorder) Eq(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Eq", reflect.TypeOf((*MockFilter)(nil).Eq), arg0, arg1)
}

// Ge mocks base method.
func (m *MockFilter) Ge(arg0 string, arg1 interface{}) (*roaring64.Bitmap, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Ge", arg0, arg1)
	ret0, _ := ret[0].(*roaring64.Bitmap)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Ge indicates an expected call of Ge.
func (mr *MockFilterMockRecorder) Ge(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ge", reflect.TypeOf((*MockFilter)(nil).Ge), arg0, arg1)
}

// Gt mocks base method.
func (m *MockFilter) Gt(arg0 string, arg1 interface{}) (*roaring64.Bitmap, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Gt", arg0, arg1)
	ret0, _ := ret[0].(*roaring64.Bitmap)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Gt indicates an expected call of Gt.
func (mr *MockFilterMockRecorder) Gt(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Gt", reflect.TypeOf((*MockFilter)(nil).Gt), arg0, arg1)
}

// Le mocks base method.
func (m *MockFilter) Le(arg0 string, arg1 interface{}) (*roaring64.Bitmap, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Le", arg0, arg1)
	ret0, _ := ret[0].(*roaring64.Bitmap)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Le indicates an expected call of Le.
func (mr *MockFilterMockRecorder) Le(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Le", reflect.TypeOf((*MockFilter)(nil).Le), arg0, arg1)
}

// Lt mocks base method.
func (m *MockFilter) Lt(arg0 string, arg1 interface{}) (*roaring64.Bitmap, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Lt", arg0, arg1)
	ret0, _ := ret[0].(*roaring64.Bitmap)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Lt indicates an expected call of Lt.
func (mr *MockFilterMockRecorder) Lt(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Lt", reflect.TypeOf((*MockFilter)(nil).Lt), arg0, arg1)
}

// Ne mocks base method.
func (m *MockFilter) Ne(arg0 string, arg1 interface{}) (*roaring64.Bitmap, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Ne", arg0, arg1)
	ret0, _ := ret[0].(*roaring64.Bitmap)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Ne indicates an expected call of Ne.
func (mr *MockFilterMockRecorder) Ne(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ne", reflect.TypeOf((*MockFilter)(nil).Ne), arg0, arg1)
}

// MockSummarizer is a mock of Summarizer interface.
type MockSummarizer struct {
	ctrl     *gomock.Controller
	recorder *MockSummarizerMockRecorder
}

// MockSummarizerMockRecorder is the mock recorder for MockSummarizer.
type MockSummarizerMockRecorder struct {
	mock *MockSummarizer
}

// NewMockSummarizer creates a new mock instance.
func NewMockSummarizer(ctrl *gomock.Controller) *MockSummarizer {
	mock := &MockSummarizer{ctrl: ctrl}
	mock.recorder = &MockSummarizerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockSummarizer) EXPECT() *MockSummarizerMockRecorder {
	return m.recorder
}

// Count mocks base method.
func (m *MockSummarizer) Count(arg0 string, arg1 *roaring64.Bitmap) (uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Count", arg0, arg1)
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Count indicates an expected call of Count.
func (mr *MockSummarizerMockRecorder) Count(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Count", reflect.TypeOf((*MockSummarizer)(nil).Count), arg0, arg1)
}

// Max mocks base method.
func (m *MockSummarizer) Max(arg0 string, arg1 *roaring64.Bitmap) (interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Max", arg0, arg1)
	ret0, _ := ret[0].(interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Max indicates an expected call of Max.
func (mr *MockSummarizerMockRecorder) Max(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Max", reflect.TypeOf((*MockSummarizer)(nil).Max), arg0, arg1)
}

// Min mocks base method.
func (m *MockSummarizer) Min(arg0 string, arg1 *roaring64.Bitmap) (interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Min", arg0, arg1)
	ret0, _ := ret[0].(interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Min indicates an expected call of Min.
func (mr *MockSummarizerMockRecorder) Min(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Min", reflect.TypeOf((*MockSummarizer)(nil).Min), arg0, arg1)
}

// NullCount mocks base method.
func (m *MockSummarizer) NullCount(arg0 string, arg1 *roaring64.Bitmap) (uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NullCount", arg0, arg1)
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NullCount indicates an expected call of NullCount.
func (mr *MockSummarizerMockRecorder) NullCount(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NullCount", reflect.TypeOf((*MockSummarizer)(nil).NullCount), arg0, arg1)
}

// Sum mocks base method.
func (m *MockSummarizer) Sum(arg0 string, arg1 *roaring64.Bitmap) (int64, uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Sum", arg0, arg1)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(uint64)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// Sum indicates an expected call of Sum.
func (mr *MockSummarizerMockRecorder) Sum(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Sum", reflect.TypeOf((*MockSummarizer)(nil).Sum), arg0, arg1)
}

// MockSparseFilter is a mock of SparseFilter interface.
type MockSparseFilter struct {
	ctrl     *gomock.Controller
	recorder *MockSparseFilterMockRecorder
}

// MockSparseFilterMockRecorder is the mock recorder for MockSparseFilter.
type MockSparseFilterMockRecorder struct {
	mock *MockSparseFilter
}

// NewMockSparseFilter creates a new mock instance.
func NewMockSparseFilter(ctrl *gomock.Controller) *MockSparseFilter {
	mock := &MockSparseFilter{ctrl: ctrl}
	mock.recorder = &MockSparseFilterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockSparseFilter) EXPECT() *MockSparseFilterMockRecorder {
	return m.recorder
}

// Btw mocks base method.
func (m *MockSparseFilter) Btw(arg0 string, arg1, arg2 interface{}) (engine.Reader, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Btw", arg0, arg1, arg2)
	ret0, _ := ret[0].(engine.Reader)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Btw indicates an expected call of Btw.
func (mr *MockSparseFilterMockRecorder) Btw(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Btw", reflect.TypeOf((*MockSparseFilter)(nil).Btw), arg0, arg1, arg2)
}

// Eq mocks base method.
func (m *MockSparseFilter) Eq(arg0 string, arg1 interface{}) (engine.Reader, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Eq", arg0, arg1)
	ret0, _ := ret[0].(engine.Reader)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Eq indicates an expected call of Eq.
func (mr *MockSparseFilterMockRecorder) Eq(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Eq", reflect.TypeOf((*MockSparseFilter)(nil).Eq), arg0, arg1)
}

// Ge mocks base method.
func (m *MockSparseFilter) Ge(arg0 string, arg1 interface{}) (engine.Reader, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Ge", arg0, arg1)
	ret0, _ := ret[0].(engine.Reader)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Ge indicates an expected call of Ge.
func (mr *MockSparseFilterMockRecorder) Ge(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ge", reflect.TypeOf((*MockSparseFilter)(nil).Ge), arg0, arg1)
}

// Gt mocks base method.
func (m *MockSparseFilter) Gt(arg0 string, arg1 interface{}) (engine.Reader, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Gt", arg0, arg1)
	ret0, _ := ret[0].(engine.Reader)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Gt indicates an expected call of Gt.
func (mr *MockSparseFilterMockRecorder) Gt(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Gt", reflect.TypeOf((*MockSparseFilter)(nil).Gt), arg0, arg1)
}

// Le mocks base method.
func (m *MockSparseFilter) Le(arg0 string, arg1 interface{}) (engine.Reader, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Le", arg0, arg1)
	ret0, _ := ret[0].(engine.Reader)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Le indicates an expected call of Le.
func (mr *MockSparseFilterMockRecorder) Le(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Le", reflect.TypeOf((*MockSparseFilter)(nil).Le), arg0, arg1)
}

// Lt mocks base method.
func (m *MockSparseFilter) Lt(arg0 string, arg1 interface{}) (engine.Reader, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Lt", arg0, arg1)
	ret0, _ := ret[0].(engine.Reader)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Lt indicates an expected call of Lt.
func (mr *MockSparseFilterMockRecorder) Lt(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Lt", reflect.TypeOf((*MockSparseFilter)(nil).Lt), arg0, arg1)
}

// Ne mocks base method.
func (m *MockSparseFilter) Ne(arg0 string, arg1 interface{}) (engine.Reader, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Ne", arg0, arg1)
	ret0, _ := ret[0].(engine.Reader)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Ne indicates an expected call of Ne.
func (mr *MockSparseFilterMockRecorder) Ne(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ne", reflect.TypeOf((*MockSparseFilter)(nil).Ne), arg0, arg1)
}

// MockDatabase is a mock of Database interface.
type MockDatabase struct {
	ctrl     *gomock.Controller
	recorder *MockDatabaseMockRecorder
}

// MockDatabaseMockRecorder is the mock recorder for MockDatabase.
type MockDatabaseMockRecorder struct {
	mock *MockDatabase
}

// NewMockDatabase creates a new mock instance.
func NewMockDatabase(ctrl *gomock.Controller) *MockDatabase {
	mock := &MockDatabase{ctrl: ctrl}
	mock.recorder = &MockDatabaseMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDatabase) EXPECT() *MockDatabaseMockRecorder {
	return m.recorder
}

// Create mocks base method.
func (m *MockDatabase) Create(arg0 uint64, arg1 string, arg2 []engine.TableDef) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Create", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// Create indicates an expected call of Create.
func (mr *MockDatabaseMockRecorder) Create(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Create", reflect.TypeOf((*MockDatabase)(nil).Create), arg0, arg1, arg2)
}

// Delete mocks base method.
func (m *MockDatabase) Delete(arg0 uint64, arg1 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Delete indicates an expected call of Delete.
func (mr *MockDatabaseMockRecorder) Delete(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockDatabase)(nil).Delete), arg0, arg1)
}

// Relation mocks base method.
func (m *MockDatabase) Relation(arg0 string) (engine.Relation, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Relation", arg0)
	ret0, _ := ret[0].(engine.Relation)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Relation indicates an expected call of Relation.
func (mr *MockDatabaseMockRecorder) Relation(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Relation", reflect.TypeOf((*MockDatabase)(nil).Relation), arg0)
}

// Relations mocks base method.
func (m *MockDatabase) Relations() []string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Relations")
	ret0, _ := ret[0].([]string)
	return ret0
}

// Relations indicates an expected call of Relations.
func (mr *MockDatabaseMockRecorder) Relations() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Relations", reflect.TypeOf((*MockDatabase)(nil).Relations))
}

// MockEngine is a mock of Engine interface.
type MockEngine struct {
	ctrl     *gomock.Controller
	recorder *MockEngineMockRecorder
}

// MockEngineMockRecorder is the mock recorder for MockEngine.
type MockEngineMockRecorder struct {
	mock *MockEngine
}

// NewMockEngine creates a new mock instance.
func NewMockEngine(ctrl *gomock.Controller) *MockEngine {
	mock := &MockEngine{ctrl: ctrl}
	mock.recorder = &MockEngineMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockEngine) EXPECT() *MockEngineMockRecorder {
	return m.recorder
}

// Create mocks base method.
func (m *MockEngine) Create(arg0 uint64, arg1 string, arg2 int) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Create", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// Create indicates an expected call of Create.
func (mr *MockEngineMockRecorder) Create(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Create", reflect.TypeOf((*MockEngine)(nil).Create), arg0, arg1, arg2)
}

// Database mocks base method.
func (m *MockEngine) Database(arg0 string) (engine.Database, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Database", arg0)
	ret0, _ := ret[0].(engine.Database)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Database indicates an expected call of Database.
func (mr *MockEngineMockRecorder) Database(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Database", reflect.TypeOf((*MockEngine)(nil).Database), arg0)
}

// Databases mocks base method.
func (m *MockEngine) Databases() []string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Databases")
	ret0, _ := ret[0].([]string)
	return ret0
}

// Databases indicates an expected call of Databases.
func (mr *MockEngineMockRecorder) Databases() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Databases", reflect.TypeOf((*MockEngine)(nil).Databases))
}

// Delete mocks base method.
func (m *MockEngine) Delete(arg0 uint64, arg1 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Delete indicates an expected call of Delete.
func (mr *MockEngineMockRecorder) Delete(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockEngine)(nil).Delete), arg0, arg1)
}

// Node mocks base method.
func (m *MockEngine) Node(arg0 string) *engine.NodeInfo {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Node", arg0)
	ret0, _ := ret[0].(*engine.NodeInfo)
	return ret0
}

// Node indicates an expected call of Node.
func (mr *MockEngineMockRecorder) Node(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Node", reflect.TypeOf((*MockEngine)(nil).Node), arg0)
}
