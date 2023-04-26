// Copyright 2021 Matrix Origin
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

package like

import (
	"bytes"
	"fmt"
	"regexp"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
)

const (
	DEFAULT_ESCAPE_CHAR = '\\'
)

// <source column> like 'rule'
// XXX: rs here is the selection list.
func BtSliceAndConst(xs [][]byte, pat []byte, rs []bool) ([]bool, error) {
	return BtSliceNullAndConst(xs, pat, nil, rs)
}

func isNotNull(n *nulls.Nulls, i uint64) bool {
	if n == nil {
		return true
	}
	return !n.Contains(i)
}

func removeEscapeChar(src []byte, escapeChar byte) []byte {
	var target []byte
	max := len(src)
	for i := 0; i < max; i++ {
		if src[i] == escapeChar && i+1 < max {
			i = i + 1
		}
		target = append(target, src[i])
	}
	return target
}

func BtSliceNullAndConst(xs [][]byte, pat []byte, ns *nulls.Nulls, rs []bool) ([]bool, error) {
	// Opt Rule #1: if pat is empty string, only empty string like empty string.
	n := uint32(len(pat))
	if n == 0 {
		for i, s := range xs {
			rs[i] = isNotNull(ns, uint64(i)) && len(s) == 0
		}
		return rs, nil
	}

	// Opt Rule #2: anything matches %
	if n == 1 && pat[0] == '%' {
		for i := range xs {
			rs[i] = isNotNull(ns, uint64(i))
		}
		return rs, nil
	}

	// Opt Rule #3: single char matches _.
	// XXX in UTF8 world, should we do single RUNE matches _?
	if n == 1 && pat[0] == '_' {
		for i, s := range xs {
			rs[i] = isNotNull(ns, uint64(i)) && len(s) == 1
		}
		return rs, nil
	}

	// Opt Rule #3.1: single char, no wild card, so it is a simple compare eq.
	if n == 1 && pat[0] != '_' && pat[0] != '%' {
		for i, s := range xs {
			rs[i] = isNotNull(ns, uint64(i)) && len(s) == 1 && s[0] == pat[0]
		}
		return rs, nil
	}

	// Opt Rule #4.  [_%]somethingInBetween[_%]
	if n > 1 {
		c0 := pat[0]   // first character
		c1 := pat[n-1] // last character
		if !bytes.ContainsAny(pat[1:len(pat)-1], "_%") {
			if n > 2 && pat[n-2] == DEFAULT_ESCAPE_CHAR {
				c1 = DEFAULT_ESCAPE_CHAR
			}
			switch {
			case !(c0 == '%' || c0 == '_') && !(c1 == '%' || c1 == '_'):
				// Rule 4.1: no wild card, so it is a simple compare eq.
				for i, s := range xs {
					rs[i] = isNotNull(ns, uint64(i)) && uint32(len(s)) == n && bytes.Equal(pat, s)
				}
				return rs, nil
			case c0 == '_' && !(c1 == '%' || c1 == '_'):
				// Rule 4.2: _foobarzoo,
				for i, s := range xs {
					rs[i] = isNotNull(ns, uint64(i)) && uint32(len(s)) == n && bytes.Equal(pat[1:], s[1:])
				}
				return rs, nil
			case c0 == '%' && !(c1 == '%' || c1 == '_'):
				// Rule 4.3, %foobarzoo, it turns into a suffix match.
				suffix := removeEscapeChar(pat[1:], DEFAULT_ESCAPE_CHAR)
				for i, s := range xs {
					rs[i] = isNotNull(ns, uint64(i)) && bytes.HasSuffix(s, suffix)
				}
				return rs, nil
			case c1 == '_' && !(c0 == '%' || c0 == '_'):
				// Rule 4.4, foobarzoo_, it turns into eq ingoring last char.
				prefix := removeEscapeChar(pat[:n-1], DEFAULT_ESCAPE_CHAR)
				for i, s := range xs {
					rs[i] = isNotNull(ns, uint64(i)) && uint32(len(s)) == n && bytes.Equal(prefix, s[:n-1])
				}
				return rs, nil
			case c1 == '%' && !(c0 == '%' || c0 == '_'):
				// Rule 4.5 foobarzoo%, prefix match
				prefix := removeEscapeChar(pat[:n-1], DEFAULT_ESCAPE_CHAR)
				for i, s := range xs {
					rs[i] = isNotNull(ns, uint64(i)) && bytes.HasPrefix(s, prefix)
				}
				return rs, nil
			case c0 == '%' && c1 == '%':
				// Rule 4.6 %foobarzoo%, now it is contains
				substr := removeEscapeChar(pat[1:n-1], DEFAULT_ESCAPE_CHAR)
				for i, s := range xs {
					rs[i] = isNotNull(ns, uint64(i)) && bytes.Contains(s, substr)
				}
				return rs, nil
			case c0 == '%' && c1 == '_':
				// Rule 4.7 %foobarzoo_,
				suffix := removeEscapeChar(pat[1:n-1], DEFAULT_ESCAPE_CHAR)
				for i, s := range xs {
					rs[i] = isNotNull(ns, uint64(i)) && len(s) > 0 && bytes.HasSuffix(s[:len(s)-1], suffix)
				}
				return rs, nil
			case c0 == '_' && c1 == '%':
				// Rule 4.8 _foobarzoo%
				prefix := removeEscapeChar(pat[1:n-1], DEFAULT_ESCAPE_CHAR)
				for i, s := range xs {
					rs[i] = isNotNull(ns, uint64(i)) && len(s) > 0 && bytes.HasPrefix(s[1:], prefix)
				}
				return rs, nil
			}
		} else if c0 == '%' && c1 == '%' && !bytes.Contains(pat[1:len(pat)-1], []byte{'_'}) && !bytes.Contains(pat, []byte{'\\', '%'}) {
			pat0 := pat[1:]
			var subpats [][]byte
			for {
				idx := bytes.IndexByte(pat0, '%')
				if idx == -1 {
					break
				}
				subpats = append(subpats, pat0[:idx])
				pat0 = pat0[idx+1:]
			}

		outer:
			for i, s := range xs {
				if !isNotNull(ns, uint64(i)) {
					continue
				}

				for _, sp := range subpats {
					idx := bytes.Index(s, sp)
					if idx == -1 {
						continue outer
					}
					s = s[idx+len(sp):]
				}

				rs[i] = true
			}
			return rs, nil
		}
	}

	// Done opt rules, fall back to regexp
	reg, err := regexp.Compile(convert(pat))
	if err != nil {
		return nil, err
	}
	for i, s := range xs {
		rs[i] = isNotNull(ns, uint64(i)) && reg.Match(s)
	}
	return rs, nil
}

// 'source' like 'rule'
func BtConstAndConst(s []byte, pat []byte) (bool, error) {
	ss := [][]byte{s}
	rs := []bool{false}
	rs, err := BtSliceAndConst(ss, pat, rs)
	if err != nil {
		return false, err
	}
	return rs[0], nil
}

// <source column> like <rule column>
func BtSliceAndSlice(xs [][]byte, exprs [][]byte, rs []bool) ([]bool, error) {
	if len(xs) != len(exprs) {
		return nil, moerr.NewInternalErrorNoCtx("unexpected error when LIKE operator")
	}

	for i := range xs {
		isLike, err := BtConstAndConst(xs[i], exprs[i])
		if err != nil {
			return nil, err
		}
		rs[i] = isLike
	}
	return rs, nil
}

// 'source' like <rule column>
func BtConstAndSliceNull(p []byte, exprs [][]byte, ns *nulls.Nulls, rs []bool) ([]bool, error) {
	for i, ex := range exprs {
		rs[i] = false
		if isNotNull(ns, uint64(i)) {
			k, err := BtConstAndConst(p, ex)
			if err != nil {
				return nil, err
			}
			rs[i] = k
		}
	}
	return rs, nil
}

// <source column may contains null> like
func BtSliceNullAndSliceNull(xs [][]byte, exprs [][]byte, ns *nulls.Nulls, rs []bool) ([]bool, error) {
	for i := range xs {
		rs[i] = false
		if isNotNull(ns, uint64(i)) {
			k, err := BtConstAndConst(xs[i], exprs[i])
			if err != nil {
				return nil, err
			}
			rs[i] = k
		}
	}
	return rs, nil
}

func convert(pat []byte) string {
	return fmt.Sprintf("^(?s:%s)$", replace(pat))
}

func replace(s []byte) []byte {
	var oldCharactor rune

	r := make([]byte, len(s)*2+7)
	w := 0
	start := 0
	for len(s) > start {
		character, wid := utf8.DecodeRune(s[start:])
		if oldCharactor == '\\' {
			w += copy(r[w:], s[start:start+wid])
			start += wid
			oldCharactor = 0
			continue
		}
		switch character {
		case '_':
			w += copy(r[w:], []byte{'.'})
		case '%':
			w += copy(r[w:], []byte{'.', '*'})
		case '(':
			w += copy(r[w:], []byte{'\\', '('})
		case ')':
			w += copy(r[w:], []byte{'\\', ')'})
		case '\\':
		default:
			w += copy(r[w:], s[start:start+wid])
		}
		start += wid
		oldCharactor = character
	}
	return r[:w]
}
