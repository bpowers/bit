// Copyright 2021 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package unsafestring

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToBytes(t *testing.T) {
	for _, input := range []string{
		"",
		"abc",
		"😀",
	} {
		allocs := testing.AllocsPerRun(1, func() {
			initialLen := len(input)
			b := ToBytes(input)
			if input != string(b) {
				t.Fatal("expected contents equal")
			}
			// len and cap should match the string
			if initialLen != len(b) {
				t.Fatal("expected lens equal")
			}
			if initialLen != cap(b) {
				t.Fatal("expected cap equal to string len")
			}
		})
		require.Zero(t, allocs)
	}
}
