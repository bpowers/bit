// Copyright 2021 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

// Package zero provides functions to zero slices of specific types.
package zero

// Bytes zeros a byte slice.
func Bytes(b []byte) {
	for i := 0; i < len(b); i++ {
		b[i] = 0
	}
}

// ByteSlices zeros (fills with nils) a slice of byte slices
func ByteSlices(b [][]byte) {
	for i := 0; i < len(b); i++ {
		b[i] = nil
	}
}

// Uint32 zeros a uint32 slice.
func Uint32(b []uint32) {
	for i := 0; i < len(b); i++ {
		b[i] = 0
	}
}
