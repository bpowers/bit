// Copyright 2021 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package unsafestring

import (
	"reflect"
	"unsafe"
)

// ToBytes returns a byte slice aliasing to the contents of the input string.
// Many hash functions are written to take []byte as input -- this lets us
// provide an API that takes a string and use those hash functions without a
// temporary allocation (and the garbage and copying string contents an allocation
// implies).
//
// SAFETY: the returned byte slice MUST NOT be written to, only read.
func ToBytes(s string) (b []byte) {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := *(*reflect.StringHeader)(unsafe.Pointer(&s))
	bh.Data = sh.Data
	bh.Len = sh.Len
	bh.Cap = sh.Len
	return b
}
