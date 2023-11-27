// Copyright 2023 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package datafile

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/dgryski/go-farm"
)

const (
	defaultBufferSize = 4 * 1024 * 1024
	recordHeaderSize  = 4 + 1 + 2 // 32-bit checksum of the value + 8-bit key length + 16-bit value length

	maxOffset   = (1 << 40) - 1
	MaxKeyLen   = (1 << 8) - 1
	maxValueLen = (1 << 16) - 1

	headerKeyLenOff   = 4
	headerValueLenOff = 5

	hugePageSize = 2 * 1024 * 1024
)

var (
	InvalidOffset = errors.New("invalid offset")
)

type nopWriter struct{}

func (nopWriter) Write([]byte) (int, error) {
	return 0, io.EOF
}

// FileWriter is usually an *os.File, but specified as an interface for easier testing.
type FileWriter interface {
	io.Writer
	io.WriterAt
}

type Writer struct {
	f        FileWriter
	h        *fileHeader
	w        *bufio.Writer
	off      uint64
	count    uint64
	finished atomic.Bool
}

func NewWriter(f FileWriter) (*Writer, error) {
	h, err := newFileHeader()
	if err != nil {
		return nil, fmt.Errorf("newFileHeader: %w", err)
	}
	w := &Writer{
		f: f,
		h: h,
		w: bufio.NewWriterSize(f, defaultBufferSize),
	}

	if headerLen, err := w.h.WriteTo(w.w); err != nil {
		return nil, fmt.Errorf("fileHeader.WriteTo: %w", err)
	} else {
		w.off = uint64(headerLen)
	}

	// try to expose errors when writing to the backing file early
	if err := w.w.Flush(); err != nil {
		return nil, fmt.Errorf("flush: %w", err)
	}

	return w, nil
}

func (w *Writer) SetIndexMetadata(level0Len, level1Len uint64) error {
	if err := w.h.UpdateIndex(w.off, level0Len, level1Len, w.f); err != nil {
		return fmt.Errorf("h.UpdateIndex: %w", err)
	}

	return nil
}

func (w *Writer) writeRecordHeader(key, value []byte) (int, error) {
	if len(key) == 0 {
		return 0, fmt.Errorf("empty key not supported")
	}
	if len(key) > MaxKeyLen {
		return 0, fmt.Errorf("key %q too long", string(key))
	}
	if len(value) > maxValueLen {
		return 0, fmt.Errorf("value %q too long", string(value))
	}

	var header [recordHeaderSize]byte

	checksum := uint32(farm.Hash64(value))
	binary.LittleEndian.PutUint32(header[:4], checksum)
	header[headerKeyLenOff] = uint8(len(key))
	binary.LittleEndian.PutUint16(header[headerValueLenOff:headerValueLenOff+2], uint16(len(value)))

	return w.w.Write(header[:])
}

func (w *Writer) Write(key, value []byte) (off uint64, err error) {
	off = w.off
	if off == 0 {
		return 0, errors.New("invariant broken: always expect *Writer.off to be > 0")
	}

	if off > maxOffset {
		return 0, errors.New("data file has grown too large (>262 petabytes)")
	}

	headerWritten, err := w.writeRecordHeader(key, value)
	if err != nil {
		return 0, fmt.Errorf("bufio.Write 1: %e", err)
	}
	keyWritten, err := w.w.Write(key)
	if err != nil {
		return 0, fmt.Errorf("bufio.Write 2: %e", err)
	}
	valueWritten, err := w.w.Write(value)
	if err != nil {
		return 0, fmt.Errorf("bufio.Write 3: %e", err)
	}

	recordLen := uint64(headerWritten + keyWritten + valueWritten)
	w.off += recordLen
	w.count += 1

	return off, nil
}

func (w *Writer) Finish() error {
	if alreadyFinished := w.finished.Swap(true); alreadyFinished {
		// nothing to do - already cleaned up
		return nil
	}

	defer func() {
		w.w.Reset(&nopWriter{})
		w.w = nil
	}()

	// pad up so the header + data portion of our file is a multiple of 2MB
	if w.off%hugePageSize != 0 {
		zeroes := make([]byte, hugePageSize-(w.off%hugePageSize))
		_, _ = w.w.Write(zeroes)
		w.off += uint64(len(zeroes))
	}

	if err := w.w.Flush(); err != nil {
		return fmt.Errorf("bufio.Flush: %w", err)
	}

	return w.h.UpdateRecordCount(w.count, w.f)
}
