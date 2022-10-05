// Copyright 2021 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package datafile

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/dgryski/go-farm"

	"github.com/bpowers/bit/internal/exp/mmap"
)

const (
	magicDataHeader   = 0xC0FFEE0D
	fileFormatVersion = 2
	defaultBufferSize = 4 * 1024 * 1024
	recordHeaderSize  = 4 + 1 + 1 // 32-bit checksum of the value + 8-bit key length + 8-bit value length
	fileHeaderSize    = 128

	maximumOffset      = (1 << 48) - 1
	maximumKeyLength   = (1 << 8) - 1
	maximumValueLength = (1 << 8) - 1

	headerKeyLenOff   = 4
	headerValueLenOff = 5
)

var (
	InvalidOffset = errors.New("invalid offset")
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

// PackedOffset packs datafile offset + record length into a 64-bit value
type PackedOffset uint64

func (po PackedOffset) Unpack() (off, recordLen uint64) {
	packed := uint64(po)
	off = packed >> 16
	keyLen := (packed >> 8) & 0xff
	valueLen := (packed) & 0xff

	return off, recordHeaderSize + keyLen + valueLen
}

func NewPackedOffset(off uint64, keyLen, valueLen uint8) PackedOffset {
	return PackedOffset((off << 48) | uint64(keyLen)<<8 | uint64(valueLen))

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
	checksum := uint32(farm.Hash64(value))
	header := w.headerBuf
	binary.LittleEndian.PutUint32(header[:4], checksum)
	header[headerKeyLenOff] = uint8(len(key))
	header[headerValueLenOff] = uint8(len(value))
	return w.w.Write(header[:])
}

func (w *Writer) Write(key, value []byte) (off uint64, err error) {
	off = w.off

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

	if off == 0 {
		// This can be removed in prod, but documents our expectation here
		// (elsewhere we use an offset of '0' as a sentinel representing
		// 'invalid/bad index lookup')
		panic("invariant broken: always expect off to be non-negative")
	}

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

	data := m.Data()
	if err := unix.Madvise(data, syscall.MADV_RANDOM); err != nil {
		return nil, fmt.Errorf("madvise: %s", err)
	}

	fileMagic := binary.LittleEndian.Uint32(data[:4])
	if fileMagic != magicDataHeader {
		return nil, fmt.Errorf("bad magic number on data file %s (%x) -- not bit datafile or corrupted", path, fileMagic)
	}

	formatVersion := binary.LittleEndian.Uint32(data[4:8])
	if formatVersion != fileFormatVersion {
		return nil, fmt.Errorf("this version of the bit library can only read v1 data files; found v%d", fileFormatVersion)
	}

	entryCount := int64(binary.LittleEndian.Uint64(data[8:16]))

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
	// an offset of 0 is never valid -- offsets are absolute from the
	// start of the datafile, and datafiles _always_ have a 128-byte
	// header.  This doesn't indicate corruption -- if someone looks
	// up a non-existent key they could find a 0 in the index.
	if off == 0 {
		return nil, nil, InvalidOffset
	}

	m := r.mmap.Data()
	mLen := len(m)
	if off+recordHeaderSize > int64(len(m)) {
		return nil, nil, fmt.Errorf("off %d beyond bounds (%d)", off, mLen)
	}
	header := m[off : off+recordHeaderSize]
	// bounds check elimination
	_ = header[recordHeaderSize-1]
	expectedChecksum := binary.LittleEndian.Uint32(header[:4])
	keyLen := int64(header[headerKeyLenOff])
	valueLen := int64(header[headerValueLenOff])

	if off+recordHeaderSize+valueLen+keyLen > int64(mLen) {
		return nil, nil, fmt.Errorf("off %d + keyLen %d + valueLen %d beyond bounds (%d)", off, keyLen, valueLen, mLen)
	}
	key = m[off+recordHeaderSize : off+recordHeaderSize+keyLen]
	value = m[off+recordHeaderSize+keyLen : off+recordHeaderSize+keyLen+valueLen]
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
	Next() (IterItem, bool)
}

type iter struct {
	r     *Reader
	mu    sync.Mutex
	chans []struct {
		cancel func()
		ch     chan IterItem
	}
	off int64
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

func (i *iter) Next() (IterItem, bool) {
	if i.off == 0 {
		i.off = int64(fileHeaderSize)
	}

	k, v, err := i.r.ReadAt(i.off)
	if err != nil {
		return IterItem{}, false
	}

	item := IterItem{
		Key:    k,
		Value:  v,
		Offset: i.off,
	}

	i.off += recordHeaderSize + int64(len(k)) + int64(len(v))

	return item, true
}

func (i *iter) producer(_ context.Context, ch chan<- IterItem) {
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
		ch <- item
		off += recordHeaderSize + int64(len(k)) + int64(len(v))
	}
}

func (i *iter) Len() int64 {
	return i.r.Len()
}

func (i *iter) ReadAt(off int64) (key []byte, value []byte, err error) {
	return i.r.ReadAt(off)
}
