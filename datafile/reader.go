// Copyright 2023 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package datafile

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"syscall"

	"github.com/bpowers/bit/internal/exp/mmap"
	"github.com/dgryski/go-farm"
	"golang.org/x/sys/unix"
)

// PackedOffset packs datafile offset + record length into a 64-bit value
type PackedOffset uint64

func (po PackedOffset) Unpack() (off int64, recordLen uint64) {
	packed := uint64(po)
	off = int64(packed >> 24)
	keyLen := (packed >> 16) & 0xff
	valueLen := packed & 0xffff

	return off, recordHeaderSize + keyLen + valueLen
}

func NewPackedOffset(off uint64, keyLen uint8, valueLen uint16) PackedOffset {
	return PackedOffset((off << 24) | uint64(keyLen)<<16 | uint64(valueLen))
}

// unsyncReaderAt is the "backend" for reader -- it could be provided by e.g. an
// mmap backend, or one that reads from disk using the pread(2) syscall.
type unsyncReaderAt interface {
	// ReadAt returns the key and value bytes at a given offset -- key
	// and value MUST NOT be written to, and may be overwritten by the
	// next call to ReadAt.
	ReadAt(pOff PackedOffset) (key, value []byte, err error)
	Close() error
}

type Reader struct {
	h    fileHeader
	mmap *mmap.ReaderAt
}

func NewReader(path string) (*Reader, error) {
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

	var header fileHeader
	if err := header.UnmarshalBytes(data); err != nil {
		return nil, fmt.Errorf("fileHeader.UnmarshalBytes: %w", err)
	}

	r := &Reader{
		h:    header,
		mmap: m,
	}
	return r, nil
}

func (r *Reader) Len() int64 {
	return int64(r.h.recordCount)
}

func (r *Reader) ReadAt(poff PackedOffset) (key, value []byte, err error) {
	off, _ := poff.Unpack()
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

func (ii IterItem) PackedOffset() PackedOffset {
	if ii.Offset < 0 || len(ii.Key) > maximumKeyLength || len(ii.Value) > maximumValueLength {
		panic("PackedOffset invariants broken!")
	}
	return NewPackedOffset(uint64(ii.Offset), uint8(len(ii.Key)), uint16(len(ii.Value)))
}

// Iter iterates over the contents in a logfile.  Make sure to `defer it.Close()`.
type Iter interface {
	Close()
	Iter() <-chan IterItem
	Len() int64
	ReadAt(off PackedOffset) (key, value []byte, err error)
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

	k, v, err := i.r.ReadAt(NewPackedOffset(uint64(i.off), 0, 0))
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
		k, v, err := i.r.ReadAt(NewPackedOffset(uint64(off), 0, 0))
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

func (i *iter) ReadAt(off PackedOffset) (key []byte, value []byte, err error) {
	return i.r.ReadAt(off)
}
