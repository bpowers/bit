// Copyright 2021 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package zero

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBytes(t *testing.T) {
	for _, input := range [][]byte{
		{},
		{'a', 'b', 'c'},
	} {
		initialLen := len(input)
		initialCap := cap(input)
		// slices are zero'd by default
		expected := make([]byte, len(input))
		Bytes(input)
		require.Equal(t, expected, input)
		// len and cap should be unchanged
		require.Equal(t, initialLen, len(input))
		require.Equal(t, initialCap, cap(input))
	}
}

func TestByteSlices(t *testing.T) {
	for _, input := range [][][]byte{
		{},
		{[]byte("a"), []byte("bb")},
	} {
		initialLen := len(input)
		initialCap := cap(input)
		// slices are zero'd by default
		expected := make([][]byte, len(input))
		ByteSlices(input)
		require.Equal(t, expected, input)
		// len and cap should be unchanged
		require.Equal(t, initialLen, len(input))
		require.Equal(t, initialCap, cap(input))
	}
}

func TestUint32(t *testing.T) {
	for _, input := range [][]uint32{
		{},
		{1, 2, 3},
	} {
		initialLen := len(input)
		initialCap := cap(input)
		// slices are zero'd by default
		expected := make([]uint32, len(input))
		Uint32(input)
		require.Equal(t, expected, input)
		// len and cap should be unchanged
		require.Equal(t, initialLen, len(input))
		require.Equal(t, initialCap, cap(input))
	}
}
