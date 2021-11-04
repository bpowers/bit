// Copyright 2021 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package ondisk

import (
	"encoding/binary"
	"fmt"
	"os"
)

type U32Slice struct {
	f   *os.File
	len int   // length in number of elements
	off int64 // offset in bytes of the start of this slice
}

func NewU32Slice(f *os.File, len int, off int64) *U32Slice {
	return &U32Slice{
		f:   f,
		len: len,
		off: off,
	}
}

func (s *U32Slice) Set(i int, value uint32) error {
	if i < 0 || i >= s.len {
		return fmt.Errorf("offset (%d) out of range (len %d)", i, s.len)
	}
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], value)
	_, err := s.f.WriteAt(buf[:], s.off+int64(4*i))
	return err
}

func (s *U32Slice) Get(i int) (uint32, error) {
	if i < 0 || i >= s.len {
		return 0, fmt.Errorf("offset (%d) out of range (len %d)", i, s.len)
	}
	var buf [4]byte
	_, err := s.f.ReadAt(buf[:], s.off+int64(4*i))
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf[:]), nil
}

type U64Slice struct {
	f   *os.File
	len int   // length in number of elements
	off int64 // offset in bytes of the start of this slice
}

func NewU64Slice(f *os.File, len int, off int64) *U64Slice {
	return &U64Slice{
		f:   f,
		len: len,
		off: off,
	}
}

func (s *U64Slice) Set(i int, value uint64) error {
	if i < 0 || i >= s.len {
		return fmt.Errorf("offset (%d) out of range (len %d)", i, s.len)
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	_, err := s.f.WriteAt(buf[:], s.off+int64(8*i))
	return err
}

func (s *U64Slice) Get(i int) (uint64, error) {
	if i < 0 || i >= s.len {
		return 0, fmt.Errorf("offset (%d) out of range (len %d)", i, s.len)
	}
	var buf [8]byte
	_, err := s.f.ReadAt(buf[:], s.off+int64(8*i))
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf[:]), nil
}

type BucketSlice struct {
	f         *os.File
	len       int // length in number of elements
	bucketCap int // offset in bytes of the start of this slice
	bucketBuf []byte
}

const (
	initialBucketSize = 4
)

func bucketLen(bucketCap int) int {
	// uint32 `n`
	// uint32 `bucketLen`
	// uint32 * bucketSize items
	return 4 + 4 + 4*bucketCap
}

func NewBucketSlice(f *os.File, len int) (*BucketSlice, error) {
	if err := f.Truncate(0); err != nil {
		return nil, fmt.Errorf("f.Truncate: %e", err)
	}
	b := &BucketSlice{
		f:   f,
		len: len,
	}
	if err := b.setBucketCapacity(initialBucketSize); err != nil {
		return nil, err
	}
	return b, nil
}

func zero(b []byte) {
	for i := 0; i < len(b); i++ {
		b[i] = 0
	}
}

func readBucket(f *os.File, b []byte, off, bucketCap int) error {
	bucketSize := bucketLen(bucketCap)
	if len(b) < bucketSize {
		return fmt.Errorf("readBucket(f, b, %d, %d): b too short for bucketCap", off, bucketCap)
	}
	_, err := f.ReadAt(b, int64(off*bucketSize))
	return err
}

func (s *BucketSlice) setBucketCapacity(newBucketCap int) error {
	oldBucketCap := s.bucketCap
	if oldBucketCap > newBucketCap {
		return fmt.Errorf("newBucketCap %d needs to be greater than old cap %d", newBucketCap, oldBucketCap)
	} else if oldBucketCap == newBucketCap {
		// nothing to do
		return nil
	}

	oldBucketSize := bucketLen(oldBucketCap)
	newBucketSize := bucketLen(newBucketCap)
	if err := s.f.Truncate(int64(s.len * newBucketSize)); err != nil {
		return fmt.Errorf("f.Truncate: %e", err)
	}

	// previously we were an empty file; no buckets to update/resize
	if oldBucketCap == 0 {
		return nil
	}

	bucketBuf := make([]byte, newBucketSize)
	// iterate in reverse order so that we are always moving a bucket into a new (or no longer
	// needed) part of the file
	for i := s.len - 1; i >= 0; i-- {
		zero(bucketBuf)
		if err := readBucket(s.f, bucketBuf[0:oldBucketSize], i, oldBucketCap); err != nil {
			return fmt.Errorf("readBucket: %e", err)
		}
		_, err := s.f.WriteAt(bucketBuf, int64(i*newBucketSize))
		if err != nil {
			return fmt.Errorf("writeBucket: %e", err)
		}
	}
	s.bucketBuf = bucketBuf

	return nil
}

func (s *BucketSlice) AddToBucket(off int64, n int32) error {
	if off < 0 || off >= int64(s.len) {
		return fmt.Errorf("off %d out of range", off)
	}
	bucketBuf := s.bucketBuf
	zero(bucketBuf)
	if err := readBucket(s.f, bucketBuf, int(off), s.bucketCap); err != nil {
		return fmt.Errorf("readBucket: %e", err)
	}
	bucketOff := binary.LittleEndian.Uint32(bucketBuf[0:4])
	if bucketOff == 0 && off != 0 {
		binary.LittleEndian.PutUint32(bucketBuf[0:4], uint32(off))
	}
	bucketLen := int(binary.LittleEndian.Uint32(bucketBuf[4:8]))
	if bucketLen >= s.bucketCap {
		if err := s.setBucketCapacity(s.bucketCap * 2); err != nil {
			return err
		}
		bucketBuf := s.bucketBuf
		zero(bucketBuf)
		if err := readBucket(s.f, bucketBuf, int(off), s.bucketCap); err != nil {
			return fmt.Errorf("readBucket: %e", err)
		}
	}
	binary.LittleEndian.PutUint32(bucketBuf[4:8], uint32(bucketLen+1))
	binary.LittleEndian.PutUint32(bucketBuf[4*bucketLen:4*bucketLen+4], uint32(n))
	_, err := s.f.WriteAt(bucketBuf, off*int64(len(bucketBuf)))
	return err
}
