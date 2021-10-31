// Copyright 2021 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package dataio

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/dgryski/go-farm"

	"github.com/bpowers/bit/internal/exp/mmap"
)

const (
	magicDataHeader    = 0xC0FFEE0D
	defaultBufferSize  = 4 * 1024 * 1024
	recordHeaderSize   = 4 + 4 // 32-bit record length + 32-bit checksum of the value
	maximumKeyLength   = 256
	maximumValueLength = 1 << 24
)

type nopWriter struct{}

func (*nopWriter) Write([]byte) (int, error) {
	return 0, io.EOF
}

type Writer struct {
	f   *os.File
	w   *bufio.Writer
	off uint64
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
	var headerBuf [128]byte
	binary.LittleEndian.PutUint32(headerBuf[:4], magicDataHeader)
	// current file format version
	binary.LittleEndian.PutUint32(headerBuf[4:8], 1)

	written, err := w.w.Write(headerBuf[:])
	if err != nil {
		return fmt.Errorf("bufio.Write: %e", err)
	}
	if written != 128 {
		panic("invariant broken")
	}
	w.off += uint64(written)
	if w.off != 128 {
		panic("invariant broken 2")
	}
	return nil
}

func padLen(b []byte) uint64 {
	return (8 - (uint64(len(b)) % 8)) % 8
}

func (w *Writer) Write(key, value []byte) (off uint64, err error) {
	off = w.off
	if len(key) > maximumKeyLength {
		return 0, fmt.Errorf("key length %d greater than %d", len(key), maximumKeyLength)
	}
	if len(value) > maximumValueLength {
		return 0, fmt.Errorf("value length %d greater than %d", len(value), maximumValueLength)
	}

	checksum := uint32(farm.Hash64(value))
	var header [recordHeaderSize]byte
	packedSize := (uint32(len(value)) << 8) | (uint32(len(key)) & 0xff)
	binary.LittleEndian.PutUint32(header[:4], checksum)
	binary.LittleEndian.PutUint32(header[4:], packedSize)
	headerWritten, err := w.w.Write(header[:])
	if err != nil {
		return 0, fmt.Errorf("bufio.Write 1: %e", err)
	}
	keyWritten, err := w.w.Write(key)
	if err != nil {
		return 0, fmt.Errorf("bufio.Write 2: %e", err)
	}
	// zeroed buffer used as padding
	var zeroBuf [8]byte
	keyPadLen := padLen(key)
	keyPadWritten, err := w.w.Write(zeroBuf[:keyPadLen])
	if err != nil {
		return 0, fmt.Errorf("bufio.Write 3: %e", err)
	}
	valueWritten, err := w.w.Write(value)
	if err != nil {
		return 0, fmt.Errorf("bufio.Write 3: %e", err)
	}
	valuePadLen := padLen(value)
	valuePadWritten, err := w.w.Write(zeroBuf[:valuePadLen])
	if err != nil {
		return 0, fmt.Errorf("bufio.Write 3: %e", err)
	}
	recordLen := uint64(headerWritten + keyWritten + keyPadWritten + valueWritten + valuePadWritten)
	if recordLen%8 != 0 {
		panic(fmt.Errorf("invariant broken: expected record to be 64-bit aligned, but has length %d", recordLen))
	}
	w.off += recordLen
	err = nil
	return
}

func (w *Writer) Close() error {
	if err := w.w.Flush(); err != nil {
		return fmt.Errorf("bufio.Flush: %e", err)
	}

	w.w.Reset(&nopWriter{})
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("f.Sync: %e", err)
	}

	if err := w.f.Close(); err != nil {
		return fmt.Errorf("f.Close: %e", err)
	}

	return nil
}

type Reader struct {
	mmap *mmap.ReaderAt
}

func NewMMapReaderWithPath(path string) (*Reader, error) {
	m, err := mmap.Open(path)
	if err != nil {
		return nil, fmt.Errorf("mmap.Open(%s): %e", path, err)
	}

	fileMagic := binary.LittleEndian.Uint32(m.Data()[:4])
	if fileMagic != magicDataHeader {
		return nil, fmt.Errorf("bad magic number on data file %s (%x) -- not bit dataio file or corrupted", path, fileMagic)
	}

	fileFormatVersion := binary.LittleEndian.Uint32(m.Data()[4:8])
	if fileFormatVersion != 1 {
		return nil, fmt.Errorf("this version of the bit library can only read v1 data files; found v%d", fileFormatVersion)
	}

	r := &Reader{
		mmap: m,
	}
	return r, nil
}

func (r *Reader) Read(off uint64) (key, value []byte, err error) {
	m := r.mmap.Data()
	mLen := len(m)
	if off+recordHeaderSize > uint64(len(m)) {
		return nil, nil, fmt.Errorf("off %d beyond bounds (%d)", off, mLen)
	}
	header := m[off : off+recordHeaderSize]
	// bounds check elimination
	_ = header[recordHeaderSize-1]
	expectedChecksum := binary.LittleEndian.Uint32(header[:4])
	packedLen := uint64(binary.LittleEndian.Uint32(header[4:]))
	valueLen := packedLen >> 8
	keyLen := packedLen & 0xff
	if off+recordHeaderSize+valueLen+keyLen > uint64(mLen) {
		return nil, nil, fmt.Errorf("off %d + keyLen %d + valueLen %d beyond bounds (%d)", off, keyLen, valueLen, mLen)
	}
	key = m[off+recordHeaderSize : off+recordHeaderSize+keyLen]
	paddedKeyLen := uint64((len(key) + 7) & (-8))
	value = m[off+recordHeaderSize+paddedKeyLen : off+recordHeaderSize+paddedKeyLen+valueLen]
	// bounds check elimination
	_ = value[valueLen-1]
	checksum := uint32(farm.Hash64(value))
	if expectedChecksum != checksum {
		return nil, nil, fmt.Errorf("off %d checksum failed (%d != %d): data file corrupted", off, expectedChecksum, checksum)
	}
	return key, value, nil
}
