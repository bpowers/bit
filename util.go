// Copyright 2021 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package bit

import (
	"bytes"
)

// special case of SplitN that doesn't require allocation
func split2(s []byte, sep byte) (l []byte, r []byte, ok bool) {
	m := bytes.IndexByte(s, sep)
	if m < 0 {
		return nil, nil, false
	}

	l = s[:m]
	r = s[m+1:]
	ok = true
	return
}

type stringSet map[string]struct{}

func (set stringSet) Contains(s string) bool {
	_, ok := set[s]
	return ok
}

func (set stringSet) Add(s string) {
	set[s] = struct{}{}
}
