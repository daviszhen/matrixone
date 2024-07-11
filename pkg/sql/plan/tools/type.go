// Copyright 2024 Matrix Origin
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

package tools

import (
	"context"
	"strings"

	plan2 "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
)

type MatchResult struct {
	IsMatch    bool
	RetAliases plan.UnorderedMap[string, string]
}

func NewMatchResult(isMatch bool, retAliases plan.UnorderedMap[string, string]) *MatchResult {
	return &MatchResult{
		IsMatch:    isMatch,
		RetAliases: retAliases,
	}
}

func (mr *MatchResult) String() string {
	sb := strings.Builder{}
	if mr.IsMatch {
		sb.WriteString("match")
	} else {
		sb.WriteString("no match")
	}
	if len(mr.RetAliases) > 0 {
		sb.WriteString(" {")
		for k, v := range mr.RetAliases {
			sb.WriteString(k)
			sb.WriteString(": ")
			sb.WriteString(v)
			sb.WriteString(",")
		}
		sb.WriteString("}")
	}
	return sb.String()
}

func Matched() *MatchResult {
	return NewMatchResult(true, nil)
}

func MatchedWithAliases(aliases plan.UnorderedMap[string, string]) *MatchResult {
	return NewMatchResult(true, aliases)
}

func MatchedWithAlias(alias, ref string) *MatchResult {
	aliases := make(plan.UnorderedMap[string, string])
	aliases.Insert(alias, ref)
	return NewMatchResult(true, aliases)
}

func FailMatched() *MatchResult {
	return NewMatchResult(false, nil)
}

type Matcher interface {
	// SimpleMatch check the intuitive properties about Node like type, datatype, etc.
	SimpleMatch(*plan2.Node) bool

	// DeepMatch check the internal structure about Node
	DeepMatch(context.Context, *plan2.Node, plan.UnorderedMap[string, string]) (*MatchResult, error)

	String() string
}

type SColDef struct {
	Name string
	Type plan2.Type
}

type RValueMatcher interface {
	GetAssignedVar(*plan2.Node, plan.UnorderedMap[string, string]) (*SColDef, error)
	String() string
}

// MatchPattern denotes the structure pattern that the Plan
// should have.
type MatchPattern struct {
	Matchers []Matcher       //matchers for components in Node
	Children []*MatchPattern // children pattern of children nodes
	AnyTree  bool
}

type Domain struct {
}

type AssertConfig struct {
}

type MatchingState struct {
	Patterns []*MatchPattern
}

type StringPair struct {
	Key   string
	Value string
}

func NewStringPair(k, v string) StringPair {
	return StringPair{
		Key:   k,
		Value: v,
	}
}

type AssignPair struct {
	Key   string
	Value *ExprMatcher
}

func NewAssignPair(k string, v *ExprMatcher) AssignPair {
	return AssignPair{
		Key:   k,
		Value: v,
	}
}
