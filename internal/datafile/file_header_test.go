// Copyright 2023 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package datafile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileHeader_RoundTrip(t *testing.T) {
	var zero [32]byte

	origH, err := newFileHeader()
	require.NoError(t, err)
	require.Equal(t, uint32(magicDataHeader), origH.magic)
	require.Equal(t, uint32(fileFormatVersion), origH.formatVersion)
	require.NotEqual(t, zero, origH.fileID)
	origH.recordCount = 3
	origH.indexStart = 129
	origH.indexLevel0Count = 512
	origH.indexLevel1Count = 1298

	// this should be an error
	err = origH.MarshalTo(nil)
	assert.Error(t, err)

	var newH fileHeader
	headerBytes := make([]byte, fileHeaderSize)
	// this should be an error -- missing magic number
	err = newH.UnmarshalBytes(headerBytes)
	assert.Error(t, err)

	err = origH.MarshalTo(headerBytes)
	require.NoError(t, err)

	// this should be an error
	err = newH.UnmarshalBytes(nil)
	assert.Error(t, err)

	err = newH.UnmarshalBytes(headerBytes)
	require.NoError(t, err)

	assert.Equal(t, origH, &newH)

	// test that deserializing an unknown version is broken
	origH.formatVersion = 666
	err = origH.MarshalTo(headerBytes)
	require.NoError(t, err)
	// this should be an error
	err = newH.UnmarshalBytes(headerBytes)
	assert.Error(t, err)

	_ = zero
}

func TestFileHeader_UpdateRecordCount(t *testing.T) {
	origH, err := newFileHeader()
	require.NoError(t, err)
	require.Equal(t, uint32(magicDataHeader), origH.magic)
	require.Equal(t, uint32(fileFormatVersion), origH.formatVersion)
	origH.recordCount = 3
	origH.indexStart = 129
	origH.indexLevel0Count = 512
	origH.indexLevel1Count = 1298

	buf := safeBuffer{
		buf: make([]byte, fileHeaderSize),
	}

	const newRecordCount = uint64(999)

	err = origH.UpdateRecordCount(newRecordCount, &buf)
	require.NoError(t, err)

	var newH fileHeader
	err = newH.UnmarshalBytes([]byte(buf.String()))
	require.NoError(t, err)

	assert.Equal(t, origH, &newH)
	assert.Equal(t, newRecordCount, newH.recordCount)
}

func TestFileHeader_UpdateIndex(t *testing.T) {
	origH, err := newFileHeader()
	require.NoError(t, err)
	require.Equal(t, uint32(magicDataHeader), origH.magic)
	require.Equal(t, uint32(fileFormatVersion), origH.formatVersion)
	origH.recordCount = 3
	origH.indexStart = 129
	origH.indexLevel0Count = 512
	origH.indexLevel1Count = 1298

	buf := safeBuffer{
		buf: make([]byte, fileHeaderSize),
	}

	const (
		newIndexStart  = uint64(11111)
		newLevel0Count = uint64(22222)
		newLevel1Count = uint64(33333)
	)

	err = origH.UpdateIndex(newIndexStart, newLevel0Count, newLevel1Count, &buf)
	require.NoError(t, err)

	var newH fileHeader
	err = newH.UnmarshalBytes([]byte(buf.String()))
	require.NoError(t, err)

	assert.Equal(t, origH, &newH)
	assert.Equal(t, newIndexStart, newH.indexStart)
	assert.Equal(t, newLevel0Count, newH.indexLevel0Count)
	assert.Equal(t, newLevel1Count, newH.indexLevel1Count)
}
