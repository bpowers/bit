// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bytesutil

import (
	"bytes"
)

// Cut slices s around the first instance of sep,
// returning the text before and after sep.
// The found result reports whether sep appears in s.
// If sep does not appear in s, cut returns s, nil, false.
//
// Cut returns slices of the original slice s, not copies.
//
// TODO(bpowers): Cut was added to the Go standard library
//   in 1.18.  Once we depend on that, we can remove this
//   copy.  (this function is copied from the stdlib)
func Cut(s []byte, sep byte) (l []byte, r []byte, ok bool) {
	if i := bytes.IndexByte(s, sep); i >= 0 {
		return s[:i], s[i+1:], true
	}
	return s, nil, false
}
