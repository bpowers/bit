// Copyright 2021 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package datafile

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/dgryski/go-farm"

	"github.com/bpowers/bit/internal/exp/mmap"
)

const (
	magicDataHeader    = 0xC0FFEE0D
	defaultBufferSize  = 4 * 1024 * 1024
	recordHeaderSize   = 4 + 4 // 32-bit record length + 32-bit checksum of the value
	fileHeaderSize     = 128
	maximumKeyLength   = 256
	maximumValueLength = 1 << 24
)

type nopWriter struct{}

func (*nopWriter) Write([]byte) (int, error) {
	return 0, io.EOF
}

type Writer struct {
	f         *os.File
	w         *bufio.Writer
	headerBuf []byte
	off       uint64
	count     uint64
}

func NewWriter(f *os.File) (*Writer, error) {
	w := &Writer{
		f:         f,
		w:         bufio.NewWriterSize(f, defaultBufferSize),
		headerBuf: make([]byte, recordHeaderSize),
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
	binary.LittleEndian.PutUint32(headerBuf[4:8], 1)

	_, err := w.w.Write(headerBuf[:])
	if err != nil {
		return fmt.Errorf("bufio.Write: %e", err)
	}
	w.off += uint64(fileHeaderSize)
	return nil
}

func (w *Writer) writeRecordHeader(key, value []byte) (int, error) {
	checksum := uint32(farm.Hash64(value))
	header := w.headerBuf
	packedSize := (uint32(len(value)) << 8) | (uint32(len(key)) & 0xff)
	binary.LittleEndian.PutUint32(header[:4], checksum)
	binary.LittleEndian.PutUint32(header[4:], packedSize)
	return w.w.Write(header[:])
}

func (w *Writer) Write(key, value []byte) (off uint64, err error) {
	w.count += 1
	off = w.off
	if len(key) > maximumKeyLength {
		return 0, fmt.Errorf("key length %d greater than %d", len(key), maximumKeyLength)
	}
	if len(value) > maximumValueLength {
		return 0, fmt.Errorf("value length %d greater than %d", len(value), maximumValueLength)
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
	err = nil
	return
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

type Reader struct {
	mmap       *mmap.ReaderAt
	entryCount int64
}

func NewMMapReaderWithPath(path string) (*Reader, error) {
	m, err := mmap.Open(path)
	if err != nil {
		return nil, fmt.Errorf("mmap.Open(%s): %e", path, err)
	}

	if m.Len() < fileHeaderSize {
		return nil, fmt.Errorf("data file too short: %d < %d", m.Len(), fileHeaderSize)
	}

	fileMagic := binary.LittleEndian.Uint32(m.Data()[:4])
	if fileMagic != magicDataHeader {
		return nil, fmt.Errorf("bad magic number on data file %s (%x) -- not bit datafile or corrupted", path, fileMagic)
	}

	fileFormatVersion := binary.LittleEndian.Uint32(m.Data()[4:8])
	if fileFormatVersion != 1 {
		return nil, fmt.Errorf("this version of the bit library can only read v1 data files; found v%d", fileFormatVersion)
	}

	entryCount := int64(binary.LittleEndian.Uint64(m.Data()[8:16]))

	r := &Reader{
		mmap:       m,
		entryCount: entryCount,
	}
	return r, nil
}

func (r *Reader) Len() int64 {
	return r.entryCount
}

func (r *Reader) ReadAt(off int64) (key, value []byte, err error) {
	m := r.mmap.Data()
	mLen := len(m)
	if off+recordHeaderSize > int64(len(m)) {
		return nil, nil, fmt.Errorf("off %d beyond bounds (%d)", off, mLen)
	}
	header := m[off : off+recordHeaderSize]
	// bounds check elimination
	_ = header[recordHeaderSize-1]
	expectedChecksum := binary.LittleEndian.Uint32(header[:4])
	packedLen := int64(binary.LittleEndian.Uint32(header[4:]))
	valueLen := packedLen >> 8
	keyLen := packedLen & 0xff
	if off+recordHeaderSize+valueLen+keyLen > int64(mLen) {
		return nil, nil, fmt.Errorf("off %d + keyLen %d + valueLen %d beyond bounds (%d)", off, keyLen, valueLen, mLen)
	}
	key = m[off+recordHeaderSize : off+recordHeaderSize+keyLen]
	value = m[off+recordHeaderSize+keyLen : off+recordHeaderSize+keyLen+valueLen]
	// bounds check elimination
	_ = value[valueLen-1]
	checksum := uint32(farm.Hash64(value))
	if expectedChecksum != checksum {
		return nil, nil, fmt.Errorf("off %d checksum failed (%d != %d): data file corrupted", off, expectedChecksum, checksum)
	}
	return key, value, nil
}

func (r *Reader) Iter() Iter {
	return &iter{r: r}
}

type IterItem struct {
	Key    []byte
	Value  []byte
	Offset int64
}

// Iter iterates over the contents in a logfile.  Make sure to `defer it.Close()`.
type Iter interface {
	Close()
	Iter() <-chan IterItem
	Len() int64
	ReadAt(off int64) (key []byte, value []byte, err error)
}

type iter struct {
	r     *Reader
	mu    sync.Mutex
	chans []struct {
		cancel func()
		ch     chan IterItem
	}
}

// Close cleans up the iterator, closing the iteration channel and freeing resources.
func (i *iter) Close() {
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, ch := range i.chans {
		ch.cancel()
	}
	i.chans = nil
}

func (i *iter) Iter() <-chan IterItem {
	i.mu.Lock()
	defer i.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	// unbuffered
	ch := make(chan IterItem, 0)
	i.chans = append(i.chans, struct {
		cancel func()
		ch     chan IterItem
	}{
		cancel: cancel,
		ch:     ch,
	})
	go i.producer(ctx, ch)
	return ch
}

func (i *iter) producer(ctx context.Context, ch chan<- IterItem) {
	defer close(ch)

	off := int64(fileHeaderSize)
	for {
		k, v, err := i.r.ReadAt(off)
		if err != nil {
			return
		}
		item := IterItem{
			Key:    k,
			Value:  v,
			Offset: off,
		}
		select {
		case ch <- item:
		case <-ctx.Done():
			break
		}
		off += recordHeaderSize + int64(len(k)) + int64(len(v))
	}
}

func (i *iter) Len() int64 {
	return i.r.Len()
}

func (i *iter) ReadAt(off int64) (key []byte, value []byte, err error) {
	return i.r.ReadAt(off)
}
