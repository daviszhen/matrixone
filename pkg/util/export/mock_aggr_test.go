// Copyright 2024 Matrix Origin
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

// Code generated by MockGen. DO NOT EDIT.
// Source: pkg/util/export/table/aggr.go

// Package export is a generated GoMock package.
package export

import (
	reflect "reflect"
	time "time"

	gomock "github.com/golang/mock/gomock"
	table "github.com/matrixorigin/matrixone/pkg/util/export/table"
)

// MockWindowKey is a mock of WindowKey interface.
type MockWindowKey struct {
	ctrl     *gomock.Controller
	recorder *MockWindowKeyMockRecorder
}

// MockWindowKeyMockRecorder is the mock recorder for MockWindowKey.
type MockWindowKeyMockRecorder struct {
	mock *MockWindowKey
}

// NewMockWindowKey creates a new mock instance.
func NewMockWindowKey(ctrl *gomock.Controller) *MockWindowKey {
	mock := &MockWindowKey{ctrl: ctrl}
	mock.recorder = &MockWindowKeyMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockWindowKey) EXPECT() *MockWindowKeyMockRecorder {
	return m.recorder
}

// Before mocks base method.
func (m *MockWindowKey) Before(end time.Time) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Before", end)
	ret0, _ := ret[0].(bool)
	return ret0
}

// Before indicates an expected call of Before.
func (mr *MockWindowKeyMockRecorder) Before(end interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Before", reflect.TypeOf((*MockWindowKey)(nil).Before), end)
}

// MockItem is a mock of Item interface.
type MockItem struct {
	ctrl     *gomock.Controller
	recorder *MockItemMockRecorder
}

// MockItemMockRecorder is the mock recorder for MockItem.
type MockItemMockRecorder struct {
	mock *MockItem
}

// NewMockItem creates a new mock instance.
func NewMockItem(ctrl *gomock.Controller) *MockItem {
	mock := &MockItem{ctrl: ctrl}
	mock.recorder = &MockItemMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockItem) EXPECT() *MockItemMockRecorder {
	return m.recorder
}

// Free mocks base method.
func (m *MockItem) Free() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Free")
}

// Free indicates an expected call of Free.
func (mr *MockItemMockRecorder) Free() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Free", reflect.TypeOf((*MockItem)(nil).Free))
}

// GetName mocks base method.
func (m *MockItem) GetName() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetName")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetName indicates an expected call of GetName.
func (mr *MockItemMockRecorder) GetName() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetName", reflect.TypeOf((*MockItem)(nil).GetName))
}

// Key mocks base method.
func (m *MockItem) Key(duration time.Duration) table.WindowKey {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Key", duration)
	ret0, _ := ret[0].(table.WindowKey)
	return ret0
}

// Key indicates an expected call of Key.
func (mr *MockItemMockRecorder) Key(duration interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Key", reflect.TypeOf((*MockItem)(nil).Key), duration)
}

// MockAggregator is a mock of Aggregator interface.
type MockAggregator struct {
	ctrl     *gomock.Controller
	recorder *MockAggregatorMockRecorder
}

// MockAggregatorMockRecorder is the mock recorder for MockAggregator.
type MockAggregatorMockRecorder struct {
	mock *MockAggregator
}

// NewMockAggregator creates a new mock instance.
func NewMockAggregator(ctrl *gomock.Controller) *MockAggregator {
	mock := &MockAggregator{ctrl: ctrl}
	mock.recorder = &MockAggregatorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockAggregator) EXPECT() *MockAggregatorMockRecorder {
	return m.recorder
}

// AddItem mocks base method.
func (m *MockAggregator) AddItem(i table.Item) (table.Item, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddItem", i)
	ret0, _ := ret[0].(table.Item)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AddItem indicates an expected call of AddItem.
func (mr *MockAggregatorMockRecorder) AddItem(i interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddItem", reflect.TypeOf((*MockAggregator)(nil).AddItem), i)
}

// Close mocks base method.
func (m *MockAggregator) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockAggregatorMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockAggregator)(nil).Close))
}

// GetResults mocks base method.
func (m *MockAggregator) GetResults() []table.Item {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetResults")
	ret0, _ := ret[0].([]table.Item)
	return ret0
}

// GetResults indicates an expected call of GetResults.
func (mr *MockAggregatorMockRecorder) GetResults() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetResults", reflect.TypeOf((*MockAggregator)(nil).GetResults))
}

// GetWindow mocks base method.
func (m *MockAggregator) GetWindow() time.Duration {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetWindow")
	ret0, _ := ret[0].(time.Duration)
	return ret0
}

// GetWindow indicates an expected call of GetWindow.
func (mr *MockAggregatorMockRecorder) GetWindow() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetWindow", reflect.TypeOf((*MockAggregator)(nil).GetWindow))
}

// PopResultsBeforeWindow mocks base method.
func (m *MockAggregator) PopResultsBeforeWindow(end time.Time) []table.Item {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PopResultsBeforeWindow", end)
	ret0, _ := ret[0].([]table.Item)
	return ret0
}

// PopResultsBeforeWindow indicates an expected call of PopResultsBeforeWindow.
func (mr *MockAggregatorMockRecorder) PopResultsBeforeWindow(end interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PopResultsBeforeWindow", reflect.TypeOf((*MockAggregator)(nil).PopResultsBeforeWindow), end)
}
