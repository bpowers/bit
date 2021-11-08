// Copyright 2021 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package bytesutil

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplit2(t *testing.T) {
	sep := byte(',')
	for _, testcase := range []string{
		"",
		"a,b",
		",a,b,",
		"a,b,",
	} {
		input := []byte(testcase)
		expected := bytes.SplitN(input, []byte{sep}, 2)
		var actualL, actualR []byte
		var ok bool
		allocs := testing.AllocsPerRun(1, func() {
			actualL, actualR, ok = Cut(input, sep)
		})
		require.Zero(t, allocs)
		require.True(t, len(expected) <= 2)
		if len(expected) < 2 {
			require.False(t, ok)
		} else {
			expectedL := expected[0]
			expectedR := expected[1]
			require.Equal(t, expectedL, actualL)
			require.Equal(t, expectedR, actualR)
		}
	}
}
