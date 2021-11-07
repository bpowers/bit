// Copyright 2021 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package ondisk

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// createUnlinkedTestFile creates a test file that is already removed from the
// file system -- just close it (or exit the program) and it will be cleaned up.
func createUnlinkedTestFile() *os.File {
	f, err := os.CreateTemp("", "bit-internal-array.*.test")
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = os.Remove(f.Name())
	}()
	return f
}

func TestUint32Array(t *testing.T) {
	const arrayLen = 12
	f := createUnlinkedTestFile()
	arr := NewUint32Array(f, arrayLen, 8)
	err := arr.Set(12, 0)
	require.Error(t, err)
	_, err = arr.Get(13)
	require.Error(t, err)
	for i := int64(0); i < arrayLen; i++ {
		err := arr.Set(i, uint32(i*2))
		require.NoError(t, err)
	}
	for i := int64(0); i < arrayLen; i++ {
		v, err := arr.Get(i)
		require.NoError(t, err)
		require.Equal(t, uint32(i*2), v)
	}

	// if we truncate the file, reads should fail
	err = f.Truncate(0)
	require.NoError(t, err)
	_, err = arr.Get(0)
	require.Error(t, err)

	// if we close a file, we expect errors
	_ = f.Close()
	_, err = arr.Get(0)
	require.Error(t, err)
	err = arr.Set(0, 12)
	require.Error(t, err)
}

func TestUint64Array(t *testing.T) {
	const arrayLen = 12
	f := createUnlinkedTestFile()
	arr := NewUint64Array(f, arrayLen, 8)
	err := arr.Set(12, 0)
	require.Error(t, err)
	_, err = arr.Get(13)
	require.Error(t, err)
	for i := int64(0); i < arrayLen; i++ {
		err := arr.Set(i, uint64(i*2))
		require.NoError(t, err)
	}
	for i := int64(0); i < arrayLen; i++ {
		v, err := arr.Get(i)
		require.NoError(t, err)
		require.Equal(t, uint64(i*2), v)
	}

	// if we truncate the file, reads should fail
	err = f.Truncate(0)
	require.NoError(t, err)
	_, err = arr.Get(0)
	require.Error(t, err)

	// if we close a file, we expect errors
	_ = f.Close()
	_, err = arr.Get(0)
	require.Error(t, err)
	err = arr.Set(0, 12)
	require.Error(t, err)
}
