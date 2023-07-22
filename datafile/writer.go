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
	"os"

	"github.com/dgryski/go-farm"
)

const (
	magicDataHeader   = 0xC0FFEE0D
	fileFormatVersion = 3
	defaultBufferSize = 4 * 1024 * 1024
	recordHeaderSize  = 4 + 1 + 2 // 32-bit checksum of the value + 8-bit key length + 16-bit value length
	fileHeaderSize    = 128

	maximumOffset      = (1 << 48) - 1
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

type Writer struct {
	f     *os.File
	w     *bufio.Writer
	off   uint64
	count uint64
}

func NewWriter(f *os.File) (*Writer, error) {
	w := &Writer{
		f: f,
		w: bufio.NewWriterSize(f, defaultBufferSize),
	}
	if err := w.writeHeader(); err != nil {
		_ = w.Close()
		return nil, err
	}

	return w, nil
}

func (w *Writer) writeHeader() error {
	// make the header the minimum cache-width we expect to see
	var headerBuf [fileHeaderSize]byte
	binary.LittleEndian.PutUint32(headerBuf[:4], magicDataHeader)
	// current file format version
	binary.LittleEndian.PutUint32(headerBuf[4:8], fileFormatVersion)

	_, err := w.w.Write(headerBuf[:])
	if err != nil {
		return fmt.Errorf("bufio.Write: %e", err)
	}
	w.off += uint64(fileHeaderSize)
	return nil
}

func (w *Writer) writeRecordHeader(key, value []byte) (int, error) {
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

func (w *Writer) Close() error {
	if err := w.w.Flush(); err != nil {
		return fmt.Errorf("bufio.Flush: %e", err)
	}
	w.w.Reset(&nopWriter{})
	w.w = nil

	var recordCountBuf [8]byte
	binary.LittleEndian.PutUint64(recordCountBuf[:], w.count)
	if _, err := w.f.WriteAt(recordCountBuf[:], 8); err != nil {
		return fmt.Errorf("f.WriteAt: %e", err)
	}

	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("f.Sync: %e", err)
	}

	if err := w.f.Close(); err != nil {
		return fmt.Errorf("f.Close: %e", err)
	}

	return nil
}
