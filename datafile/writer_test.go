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
)

type safeBuffer struct {
	mu  sync.Mutex
	buf []byte
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
