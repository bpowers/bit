// Copyright 2023 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package datafile

import (
	"errors"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type safeBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return string(s.buf)
}

func (s *safeBuffer) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf = append(s.buf, p...)
	return len(p), nil
}

func (s *safeBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if int(off)+len(p) > len(s.buf) {
		return 0, errors.New("writeAt out of bounds")
	}

	return copy(s.buf[off:int(off)+len(p)], p), nil
}

func (s *safeBuffer) ReadAt(p []byte, off int64) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if int(off) > len(s.buf) {
		return 0, errors.New("writeAt out of bounds")
	}

	end := int(off) + len(p)
	if end > len(s.buf) {
		end = len(s.buf)
	}

	return copy(p, s.buf[off:end]), nil
}

func (s *safeBuffer) Close() error {
	return nil
}

var _ FileWriter = &safeBuffer{}

type testWriter struct {
	inner            FileWriter
	writeShouldError bool
}

func (c *testWriter) Write(p []byte) (n int, err error) {
	if c.writeShouldError {
		return 0, errors.New("write failed")
	}
	return c.inner.Write(p)
}

func (c *testWriter) WriteAt(p []byte, off int64) (n int, err error) {
	if c.writeShouldError {
		return 0, errors.New("write failed")
	}
	return c.inner.WriteAt(p, off)
}

var _ FileWriter = &testWriter{}

func TestNewWriter_Errors(t *testing.T) {
	var fileBytes safeBuffer
	writer := &testWriter{
		inner:            &fileBytes,
		writeShouldError: true,
	}

	_, err := NewWriter(writer)
	assert.Error(t, err)
}

func TestWriter_TooBigErrors(t *testing.T) {
	var fileBytes safeBuffer

	w, err := NewWriter(&fileBytes)
	require.NoError(t, err)

	var k, v []byte

	// key too big should be an error
	k = make([]byte, MaxKeyLen+1)
	v = make([]byte, 1)
	_, err = w.Write(k, v)
	assert.Error(t, err)

	// 0-sized key should be an error
	k = make([]byte, 0)
	v = make([]byte, 1)
	_, err = w.Write(k, v)
	assert.Error(t, err)

	// value too big should be an error
	k = make([]byte, 1)
	v = make([]byte, maxValueLen+1)
	_, err = w.Write(k, v)
	assert.Error(t, err)

	err = w.Finish()
	require.NoError(t, err)
	// multiple finishes should be fine
	err = w.Finish()
	require.NoError(t, err)

	{
		origOff := w.off

		w.off = 0
		_, err := w.Write([]byte("k"), []byte("v"))
		assert.Error(t, err)

		w.off = maxOffset + 1
		_, err = w.Write([]byte("k"), []byte("v"))
		assert.Error(t, err)

		w.off = origOff
	}

	// double check: we shouldn't have written any records, but we _should_ have padded
	// out to 2 MB
	assert.Equal(t, hugePageSize, len(fileBytes.String()))
	var h fileHeader
	err = h.UnmarshalBytes([]byte(fileBytes.String()))
	require.NoError(t, err)
	assert.Equal(t, uint64(0), h.recordCount)
}

func TestWriter_Finish(t *testing.T) {
	var fileBytes safeBuffer

	w, err := NewWriter(&fileBytes)
	require.NoError(t, err)

	for i := 0; i < 1000; i++ {
		k := []byte(strconv.FormatInt(int64(i), 10))
		v := make([]byte, maxValueLen)
		for j := 0; j < len(v); j++ {
			v[j] = byte(i % 256)
		}
		_, err := w.Write(k, v)
		require.NoError(t, err)
	}

	err = w.Finish()
	require.NoError(t, err)

	contents := fileBytes.String()
	assert.Equal(t, 0, len(contents)%hugePageSize)
	var h fileHeader
	err = h.UnmarshalBytes([]byte(contents[:fileHeaderSize]))
	require.NoError(t, err)
	assert.Equal(t, uint64(1000), h.recordCount)
}

func TestWriter_RoundTrip(t *testing.T) {
	var fileBytes safeBuffer

	w, err := NewWriter(&fileBytes)
	require.NoError(t, err)

	for i := 0; i < 1000; i++ {
		k := []byte(strconv.FormatInt(int64(i), 10))
		v := make([]byte, maxValueLen)
		for j := 0; j < len(v); j++ {
			v[j] = byte(i % 256)
		}
		_, err := w.Write(k, v)
		require.NoError(t, err)
	}

	err = w.Finish()
	require.NoError(t, err)

	contents := fileBytes.String()
	assert.Equal(t, 0, len(contents)%hugePageSize)
	var h fileHeader
	err = h.UnmarshalBytes([]byte(contents[:fileHeaderSize]))
	require.NoError(t, err)
	assert.Equal(t, uint64(1000), h.recordCount)

	dataFile, err := os.CreateTemp("", "bit-test.*.data")
	require.NoError(t, err)
	defer func() {
		_ = os.Remove(dataFile.Name())
	}()
	n, err := dataFile.WriteString(contents)
	require.NoError(t, err)
	require.Equal(t, n, len(contents))
	require.NoError(t, dataFile.Close())

	r, err := NewMMapReaderWithPath(dataFile.Name())
	require.NoError(t, err)
	require.NotNil(t, r)

	assert.Equal(t, int64(1000), r.Len())

	i := 0
	it := r.Iter()
	assert.Equal(t, int64(1000), it.Len())
	for item, ok := it.Next(); ok; item, ok = it.Next() {
		assert.Equal(t, strconv.FormatInt(int64(i), 10), string(item.Key))

		k2, v2, err := it.ReadAt(item.PackedOffset())
		require.NoError(t, err)
		require.Equal(t, item.Key, k2)
		require.Equal(t, item.Value, v2)
		i++
	}
	require.Equal(t, 1000, i)

	// should be safe for multiple closes
	it.Close()
	it.Close()
}

func TestReader_Errors(t *testing.T) {
	_, err := NewMMapReaderWithPath("/doesnt/exist")
	assert.Error(t, err)

	_, err = NewMMapReaderWithPath("/dev/null")
	assert.Error(t, err)
}
