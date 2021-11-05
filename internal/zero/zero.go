// Copyright 2021 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

// Package zero provides functions to zero slices of specific types.
package zero

func Bytes(b []byte) {
	for i := 0; i < len(b); i++ {
		b[i] = 0
	}
}

func ByteSlices(b [][]byte) {
	for i := 0; i < len(b); i++ {
		b[i] = nil
	}
}

func U32(b []uint32) {
	for i := 0; i < len(b); i++ {
		b[i] = 0
	}
}
