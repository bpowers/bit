// Copyright 2023 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package datafile

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/bpowers/go-rapidhash"
	"golang.org/x/sys/unix"

	"github.com/bpowers/bit/internal/exp/mmap"
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

type MmapReader struct {
	h    fileHeader
	mmap *mmap.ReaderAt
}

func NewMMapReaderWithPath(path string) (*MmapReader, error) {
	m, err := mmap.Open(path)
	if err != nil {
		return nil, fmt.Errorf("mmap.Open(%s): %w", path, err)
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

	r := &MmapReader{
		h:    header,
		mmap: m,
	}
	return r, nil
}

func (r *MmapReader) Index() (level0Count, levellCount uint64, indexBytes []byte) {
	return r.h.indexLevel0Count, r.h.indexLevel1Count, r.mmap.Data()[r.h.indexStart:]
}

func (r *MmapReader) Len() int64 {
	return int64(r.h.recordCount)
}

func readRecordHeader(header []byte) (expectedChecksum uint32, keyLen, valueLen int64) {
	_ = header[recordHeaderSize-1]

	expectedChecksum = binary.LittleEndian.Uint32(header[:4])
	keyLen = int64(header[headerKeyLenOff])
	valueLen = int64(binary.LittleEndian.Uint16(header[headerValueLenOff : headerValueLenOff+2]))
	return
}

func (r *MmapReader) ReadAt(poff PackedOffset) (key, value []byte, err error) {
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
	expectedChecksum, keyLen, valueLen := readRecordHeader(header[:recordHeaderSize])

	if off+recordHeaderSize+valueLen+keyLen > int64(mLen) {
		return nil, nil, fmt.Errorf("off %d + keyLen %d + valueLen %d beyond bounds (%d)", off, keyLen, valueLen, mLen)
	}
	key = m[off+recordHeaderSize : off+recordHeaderSize+keyLen]
	value = m[off+recordHeaderSize+keyLen : off+recordHeaderSize+keyLen+valueLen]
	checksum := uint32(rapidhash.HashMicro(value, 0))
	if expectedChecksum != checksum {
		return nil, nil, fmt.Errorf("off %d checksum failed (%d != %d): data file corrupted", off, expectedChecksum, checksum)
	}
	return key, value, nil
}

func (r *MmapReader) Iter() Iter {
	return &iter{r: r}
}

type IterItem struct {
	Key    []byte
	Value  []byte
	Offset int64
}

func (ii IterItem) PackedOffset() PackedOffset {
	if ii.Offset < 0 || len(ii.Key) > MaxKeyLen || len(ii.Value) > maxValueLen {
		panic("PackedOffset invariants broken!")
	}
	return NewPackedOffset(uint64(ii.Offset), uint8(len(ii.Key)), uint16(len(ii.Value)))
}

// Iter iterates over the contents in a logfile.  Make sure to `defer it.Close()`.
type Iter interface {
	Close()
	Len() int64
	ReadAt(off PackedOffset) (key []byte, value []byte, err error)
	Next() (IterItem, bool)
}

type iter struct {
	r   *MmapReader
	mu  sync.Mutex
	off int64
}

// Close cleans up the iterator, closing the iteration channel and freeing resources.
func (i *iter) Close() {
	i.mu.Lock()
	defer i.mu.Unlock()
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

func (i *iter) Len() int64 {
	return i.r.Len()
}

func (i *iter) ReadAt(off PackedOffset) (key, value []byte, err error) {
	return i.r.ReadAt(off)
}

type OsFileReader struct {
	h        fileHeader
	f        *os.File
	size     int64
	isClosed atomic.Bool
}

func NewOsFileReader(path string) (*OsFileReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("os.Open(%s): %w", path, err)
	}

	stats, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("f.Stat: %w", err)
	}
	if stats.Size() < fileHeaderSize {
		return nil, fmt.Errorf("data file too short: %d < %d", stats.Size(), fileHeaderSize)
	}

	data := make([]byte, fileHeaderSize)
	_, err = io.ReadFull(f, data)
	if err != nil {
		return nil, fmt.Errorf("io.ReadFull: %w", err)
	}

	var header fileHeader
	if err := header.UnmarshalBytes(data); err != nil {
		return nil, fmt.Errorf("fileHeader.UnmarshalBytes: %w", err)
	}

	r := &OsFileReader{
		h:    header,
		f:    f,
		size: stats.Size(),
	}
	return r, nil
}

// Index will return index metadata and the index bytes on-heap (not mmap'ed).
func (r *OsFileReader) Index() (level0Count, levellCount uint64, indexBytes []byte, err error) {
	defer func() {
		// seek back to where we were, no matter what and ignoring errors
		_, _ = r.f.Seek(fileHeaderSize, io.SeekStart)
	}()

	indexLen := r.size - int64(r.h.indexStart)
	if indexLen <= 0 {
		err = fmt.Errorf("couldn't read index at offset %d in file of length %d", r.h.indexStart, r.size)
		return

	}

	if _, err = r.f.Seek(int64(r.h.indexStart), io.SeekStart); err != nil {
		return
	}

	indexBytes = make([]byte, indexLen)
	if _, err = io.ReadFull(r.f, indexBytes); err != nil {
		return
	}

	return r.h.indexLevel0Count, r.h.indexLevel1Count, indexBytes, nil
}

func (r *OsFileReader) ReadAt(poff PackedOffset) (key, value []byte, err error) {
	off, rLen := poff.Unpack()
	// an offset of 0 is never valid -- offsets are absolute from the
	// start of the datafile, and datafiles _always_ have a 128-byte
	// header.  This doesn't indicate corruption -- if someone looks
	// up a non-existent key they could find a 0 in the index.
	if off == 0 {
		return nil, nil, InvalidOffset
	}

	// avoid an allocation for reasonably-sized records
	buf := make([]byte, rLen)
	mLen := len(buf)

	n, err := r.f.ReadAt(buf, off)
	if err != nil {
		return nil, nil, fmt.Errorf("f.ReadAt(%d, len: %d): %w", off, rLen, err)
	} else if n != int(rLen) {
		return nil, nil, fmt.Errorf("short read of %d ReadAt(%d, len: %d)", n, off, rLen)
	}

	header := buf[:recordHeaderSize]
	// bounds check elimination
	_ = header[recordHeaderSize-1]
	expectedChecksum, keyLen, valueLen := readRecordHeader(header[:recordHeaderSize])

	if recordHeaderSize+valueLen+keyLen > int64(mLen) {
		return nil, nil, fmt.Errorf("off %d + keyLen %d + valueLen %d beyond bounds (%d)", off, keyLen, valueLen, mLen)
	}
	key = buf[recordHeaderSize : recordHeaderSize+keyLen]
	value = buf[recordHeaderSize+keyLen : recordHeaderSize+keyLen+valueLen]
	checksum := uint32(rapidhash.HashMicro(value, 0))
	if expectedChecksum != checksum {
		return nil, nil, fmt.Errorf("off %d checksum failed (%d != %d): data file corrupted", off, expectedChecksum, checksum)
	}
	return key, value, nil
}

func (r *OsFileReader) Close() error {
	if r.isClosed.Swap(true) {
		return nil
	}
	return r.f.Close()
}

/*

type reader struct {
	f   FileReader
	off int64
}

func (r *reader) Read(p []byte) (n int, err error) {
	n, err = r.f.ReadAt(p, r.off)
	if n > 0 {
		r.off += int64(n)
	}
	return
}

var _ io.MmapReader = &reader{}

func (i *iter) producer(_ context.Context, ch chan<- IterItem) {
	defer close(ch)

	offsetReader := reader{
		f:   i.r.f,
		off: fileHeaderSize,
	}
	r := bufio.NewReaderSize(&offsetReader, defaultBufferSize)

	off := int64(fileHeaderSize)
	for j := 0; int64(j) < i.Len(); j++ {
		buf := make([]byte, recordHeaderSize)
		n, err := r.Read(buf)
		if err != nil {
			// TODO: handle this more gracefully
			panic(err)
		}
		if n != recordHeaderSize {
			panic(fmt.Errorf("short read of %d bytes", n))
		}

		expectedChecksum, keyLen, valueLen := readRecordHeader(buf)
		if keyLen == 0 {
			panic("invariant broken: encountered 0 length key")
		}
		k := make([]byte, keyLen)
		v := make([]byte, valueLen)

		if n, err := io.ReadFull(r, k); int64(n) != keyLen || err != nil {
			panic(err)
		}
		if n, err := io.ReadFull(r, v); int64(n) != valueLen || err != nil {
			panic(err)
		}

		checksum := uint32(rapidhash.HashMicro(v, 0))
		if expectedChecksum != checksum {
			panic(fmt.Errorf("off %d checksum failed (%d != %d): data file corrupted", off, expectedChecksum, checksum))
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
*/
