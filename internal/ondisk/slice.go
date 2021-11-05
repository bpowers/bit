// Copyright 2021 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package ondisk

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/bpowers/bit/internal/zero"
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
	valuesBuf []uint32
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

func readBucket(f *os.File, b []byte, off, bucketCap int) error {
	bucketSize := bucketLen(bucketCap)
	if len(b) < bucketSize {
		return fmt.Errorf("readBucket(f, b, %d, %d): len(b) (%d) too short for bucketCap", off, bucketCap, len(b))
	}
	_, err := f.ReadAt(b, int64(off*bucketSize))
	return err
}

func (s *BucketSlice) setBucketCapacity(newBucketCap int) error {
	oldBucketCap := s.bucketCap
	if oldBucketCap > newBucketCap {
		return fmt.Errorf("newBucketCap %d needs to be greater than old cap %d", newBucketCap, oldBucketCap)
	}
	newBucketSize := bucketLen(newBucketCap)
	bucketBuf := make([]byte, newBucketSize)
	s.bucketBuf = bucketBuf

	if oldBucketCap == newBucketCap {
		// nothing to do
		return nil
	}

	s.bucketCap = newBucketCap

	if err := s.f.Truncate(int64(s.len * newBucketSize)); err != nil {
		return fmt.Errorf("f.Truncate: %e", err)
	}

	// previously we were an empty file; no buckets to update/resize
	if oldBucketCap == 0 {
		return nil
	}

	oldBucketSize := bucketLen(oldBucketCap)

	// iterate in reverse order so that we are always moving a bucket into a new (or no longer
	// needed) part of the file
	for i := s.len - 1; i >= 0; i-- {
		zero.Bytes(bucketBuf)
		if err := readBucket(s.f, bucketBuf[0:oldBucketSize], i, oldBucketCap); err != nil {
			return fmt.Errorf("readBucket: %e", err)
		}
		_, err := s.f.WriteAt(bucketBuf, int64(i*newBucketSize))
		if err != nil {
			return fmt.Errorf("writeBucket: %e", err)
		}
	}

	return nil
}

func (s *BucketSlice) AddToBucket(off int64, n int32) error {
	if off < 0 || off >= int64(s.len) {
		return fmt.Errorf("off %d out of range", off)
	}
	bucketBuf := s.bucketBuf[0:bucketLen(s.bucketCap)]
	zero.Bytes(bucketBuf)
	if err := readBucket(s.f, bucketBuf, int(off), s.bucketCap); err != nil {
		return fmt.Errorf("readBucket: %e", err)
	}
	bucketOff := binary.LittleEndian.Uint32(bucketBuf[0:4])
	if bucketOff == 0 && off != 0 {
		binary.LittleEndian.PutUint32(bucketBuf[0:4], uint32(off))
	}
	valuesLen := int(binary.LittleEndian.Uint32(bucketBuf[4:8]))
	if valuesLen >= s.bucketCap {
		if err := s.setBucketCapacity(s.bucketCap * 2); err != nil {
			return err
		}
		bucketBuf = s.bucketBuf[0:bucketLen(s.bucketCap)]
		zero.Bytes(bucketBuf)
		if err := readBucket(s.f, bucketBuf, int(off), s.bucketCap); err != nil {
			return fmt.Errorf("readBucket: %e", err)
		}
	}
	binary.LittleEndian.PutUint32(bucketBuf[4:8], uint32(valuesLen+1))
	binary.LittleEndian.PutUint32(bucketBuf[8+4*valuesLen:8+4*valuesLen+4], uint32(n))
	_, err := s.f.WriteAt(bucketBuf, off*int64(len(bucketBuf)))
	return err
}

type Bucket struct {
	N      int
	Values []uint32
}

func (s *BucketSlice) Bucket(off int) (Bucket, error) {
	bucketSize := bucketLen(s.bucketCap)
	if len(s.bucketBuf) < bucketSize {
		s.bucketBuf = make([]byte, bucketSize)
	}
	buf := s.bucketBuf[:bucketSize]
	zero.Bytes(buf)
	if err := readBucket(s.f, buf, off, s.bucketCap); err != nil {
		return Bucket{}, err
	}

	n := int(binary.LittleEndian.Uint32(buf[0:4]))
	valuesLen := int(binary.LittleEndian.Uint32(buf[4:8]))
	if len(s.valuesBuf) < valuesLen {
		s.valuesBuf = make([]uint32, valuesLen)
	}
	if valuesLen >= s.bucketCap {
		panic(fmt.Errorf("invariant broken: bucket %d overflowed", off))
	}
	values := s.valuesBuf[0:valuesLen]
	zero.U32(values)
	for i := 0; i < valuesLen; i++ {
		v := binary.LittleEndian.Uint32(buf[8+4*i : 8+4*i+4])
		values[i] = v
	}

	return Bucket{
		N:      n,
		Values: values,
	}, nil
}

func (s *BucketSlice) Len() int {
	return s.len
}

func (s *BucketSlice) Less(i, j int) bool {
	l := bucketLen(s.bucketCap)
	if len(s.bucketBuf) < l*2 {
		s.bucketBuf = make([]byte, l*2)
	}
	iBuf := s.bucketBuf[:l]
	jBuf := s.bucketBuf[l:]
	zero.Bytes(iBuf)
	zero.Bytes(jBuf)
	// capacity of zero -- we only care about the headers
	if err := readBucket(s.f, iBuf[:], i, s.bucketCap); err != nil {
		panic(err)
	}
	if err := readBucket(s.f, jBuf[:], j, s.bucketCap); err != nil {
		panic(err)
	}
	iLen := int(binary.LittleEndian.Uint32(iBuf[4:8]))
	jLen := int(binary.LittleEndian.Uint32(jBuf[4:8]))
	return iLen > jLen
}
func (s *BucketSlice) Swap(i, j int) {
	l := bucketLen(s.bucketCap)
	if len(s.bucketBuf) < l*2 {
		s.bucketBuf = make([]byte, l*2)
	}
	iBuf := s.bucketBuf[:l]
	jBuf := s.bucketBuf[l:]

	if err := readBucket(s.f, iBuf, i, s.bucketCap); err != nil {
		panic(err)
	}
	if err := readBucket(s.f, jBuf, j, s.bucketCap); err != nil {
		panic(err)
	}
	if _, err := s.f.WriteAt(jBuf, int64(i*l)); err != nil {
		panic(err)
	}
	if _, err := s.f.WriteAt(iBuf, int64(j*l)); err != nil {
		panic(err)
	}
}
