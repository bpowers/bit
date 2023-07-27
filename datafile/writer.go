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

	maximumOffset      = (1 << 40) - 1
	maximumKeyLength   = (1 << 8) - 1
	maximumValueLength = (1 << 16) - 1

	headerKeyLenOff   = 4
	headerValueLenOff = 5
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

	// try to catch errors when writing to the backing file early
	if err := w.w.Flush(); err != nil {
		return nil, fmt.Errorf("flush: %w", err)
	}

	return w, nil
}

func (w *Writer) writeRecordHeader(key, value []byte) (int, error) {
	if len(key) == 0 {
		return 0, fmt.Errorf("empty key not supported")
	}
	if len(key) > maximumKeyLength {
		return 0, fmt.Errorf("key %q too long", string(key))
	}
	if len(value) > maximumValueLength {
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

	if off > maximumOffset {
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
		w.f = nil
	}()

	if err := w.w.Flush(); err != nil {
		return fmt.Errorf("bufio.Flush: %w", err)
	}

	return w.h.UpdateRecordCount(w.count, w.f)
}
