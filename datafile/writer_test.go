// Copyright 2023 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package datafile

import (
	"errors"
	"sync"
	"sync/atomic"
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

func (s *safeBuffer) Close() error {
	return nil
}

func (s *safeBuffer) Sync() error {
	return nil
}

var _ FileWriter = &safeBuffer{}

type closeChecker struct {
	inner            FileWriter
	writeShouldError bool
	closeCalled      atomic.Bool
}

func (c *closeChecker) Write(p []byte) (n int, err error) {
	if c.writeShouldError {
		return 0, errors.New("write failed")
	}
	return c.inner.Write(p)
}

func (c *closeChecker) WriteAt(p []byte, off int64) (n int, err error) {
	if c.writeShouldError {
		return 0, errors.New("write failed")
	}
	return c.inner.WriteAt(p, off)
}

func (c *closeChecker) Close() error {
	c.closeCalled.Store(true)
	return c.inner.Close()
}

func (c *closeChecker) Sync() error {
	return c.inner.Sync()
}

var _ FileWriter = &closeChecker{}

func TestNewWriter_Errors(t *testing.T) {
	var fileBytes safeBuffer
	writer := &closeChecker{
		inner:            &fileBytes,
		writeShouldError: true,
	}

	_, err := NewWriter(writer)
	assert.Error(t, err)
	assert.True(t, writer.closeCalled.Load())
}

func TestWriter_TooBigErrors(t *testing.T) {
	var fileBytes safeBuffer

	w, err := NewWriter(&fileBytes)
	require.NoError(t, err)

	var k, v []byte

	// key too big should be an error
	k = make([]byte, maximumKeyLength+1)
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
	v = make([]byte, maximumValueLength+1)
	_, err = w.Write(k, v)
	assert.Error(t, err)

	err = w.Close()
	require.NoError(t, err)

	// double check: we shouldn't have written any records
	assert.Equal(t, fileHeaderSize, len(fileBytes.String()))
}
