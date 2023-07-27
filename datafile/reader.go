// Copyright 2023 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package datafile

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/dgryski/go-farm"
)

// PackedOffset packs datafile offset + record length into a 64-bit value
type PackedOffset uint64

func NewPackedOffset(off uint64, keyLen uint8, valueLen uint16) PackedOffset {
	return PackedOffset((off << 24) | uint64(keyLen)<<16 | uint64(valueLen))
}

func (po PackedOffset) Unpack() (off int64, recordLen uint64) {
	packed := uint64(po)
	off = int64(packed >> 24)
	keyLen := (packed >> 16) & 0xff
	valueLen := packed & 0xffff

	return off, recordHeaderSize + keyLen + valueLen
}

type Reader struct {
	h   fileHeader
	f   *os.File
	buf []byte
}

func NewReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("os.Open(%q): %w", path, err)
	}

	headerBytes := make([]byte, fileHeaderSize)
	if n, err := f.Read(headerBytes); err != nil || n != fileHeaderSize {
		_ = f.Close()
		return nil, fmt.Errorf("error reading file header: %s", err)
	}

	var header fileHeader
	if err := header.UnmarshalBytes(headerBytes); err != nil {
		return nil, fmt.Errorf("fileHeader.UnmarshalBytes: %w", err)
	}

	r := &Reader{
		h:   header,
		f:   f,
		buf: make([]byte, recordHeaderSize+maximumKeyLength+maximumValueLength),
	}
	return r, nil
}

func (r *Reader) Len() int64 {
	return int64(r.h.recordCount)
}

func (r *Reader) ReadAt(poff PackedOffset) (key, value []byte, err error) {
	off, recordLen := poff.Unpack()
	// an offset of 0 is never valid -- offsets are absolute from the
	// start of the datafile, and datafiles _always_ have a 128-byte
	// header.  This doesn't indicate corruption -- if someone looks
	// up a non-existent key they could find a 0 in the index.
	if off == 0 {
		return nil, nil, InvalidOffset
	}

	header := r.buf[:recordLen]
	n, err := r.f.ReadAt(header, off)
	if err != nil || uint64(n) != recordLen {
		return nil, nil, fmt.Errorf("short read; %d != expected %d", n, recordLen)
	}

	// bounds check elimination
	_ = header[recordHeaderSize-1]
	expectedChecksum := binary.LittleEndian.Uint32(header[:4])
	keyLen := int64(header[headerKeyLenOff])
	valueLen := int64(binary.LittleEndian.Uint16(header[headerValueLenOff : headerValueLenOff+2]))

	if uint64(recordHeaderSize+keyLen+valueLen) != recordLen {
		return nil, nil, fmt.Errorf("keyLen %d + valueLen %d beyond bounds (%d)", keyLen, valueLen, recordLen)
	}

	key = header[recordHeaderSize : recordHeaderSize+keyLen]
	value = header[recordHeaderSize+keyLen : recordHeaderSize+keyLen+valueLen]
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
	r *Reader

	mu    sync.Mutex
	f     *os.File
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
