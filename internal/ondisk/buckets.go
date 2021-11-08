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

type BucketSlice struct {
	f         *os.File
	nBuckets  int64 // length in number of elements
	bucketCap int64 // offset in bytes of the start of this slice
	bucketBuf []byte
	valuesBuf []uint32
}

const (
	initialBucketSize = 4
)

func bucketLen(bucketCap int64) int64 {
	// uint32 `n`
	// uint32 `bucketLen`
	// uint32 * bucketSize items
	return 4 + 4 + 4*bucketCap
}

func NewBucketSlice(f *os.File, nBuckets int64) (*BucketSlice, error) {
	if err := f.Truncate(0); err != nil {
		return nil, fmt.Errorf("f.Truncate: %e", err)
	}
	b := &BucketSlice{
		f:        f,
		nBuckets: nBuckets,
	}
	if err := b.setBucketCapacity(initialBucketSize); err != nil {
		return nil, err
	}
	return b, nil
}

func readBucket(f *os.File, b []byte, off, bucketCap int64) error {
	bucketSize := bucketLen(bucketCap)
	if int64(len(b)) < bucketSize {
		return fmt.Errorf("readBucket(f, b, %d, %d): elemLen(b) (%d) too short for bucketCap", off, bucketCap, len(b))
	}
	_, err := f.ReadAt(b, off*bucketSize)
	return err
}

func (s *BucketSlice) setBucketCapacity(newBucketCap int64) error {
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

	fileByteLen := s.nBuckets * newBucketSize
	if err := s.f.Truncate(fileByteLen); err != nil {
		return fmt.Errorf("f.Truncate: %e", err)
	}

	// previously we were an empty file; no buckets to update/resize
	if oldBucketCap == 0 {
		// page in the file now, which in practice slightly improves performance
		for i := int64(0); i < fileByteLen; i += 1024 {
			if _, err := s.f.WriteAt(bucketBuf[:1], i); err != nil {
				return err
			}
		}
		return nil
	}

	oldBucketSize := bucketLen(oldBucketCap)

	// iterate in reverse order so that we are always moving a bucket into a new (or no longer
	// needed) part of the file
	for i := s.nBuckets - 1; i >= 0; i-- {
		zero.Bytes(bucketBuf)
		if err := readBucket(s.f, bucketBuf[0:oldBucketSize], i, oldBucketCap); err != nil {
			return fmt.Errorf("readBucket: %e", err)
		}
		_, err := s.f.WriteAt(bucketBuf, i*newBucketSize)
		if err != nil {
			return fmt.Errorf("writeBucket: %e", err)
		}
	}

	return nil
}

func (s *BucketSlice) AddToBucket(off int64, n int32) error {
	if off < 0 || off >= s.nBuckets {
		return fmt.Errorf("byteOff %d out of range", off)
	}
	// reuse a buffer here to avoid an allocation
	bucketBuf := s.bucketBuf[0:bucketLen(s.bucketCap)]
	zero.Bytes(bucketBuf)
	if err := readBucket(s.f, bucketBuf, off, s.bucketCap); err != nil {
		return fmt.Errorf("readBucket: %e", err)
	}

	bucketOff := binary.LittleEndian.Uint32(bucketBuf[0:4])
	if bucketOff == 0 && off != 0 {
		binary.LittleEndian.PutUint32(bucketBuf[0:4], uint32(off))
	}

	// if we would overflow the bucket capacity, resize the whole array of buckets
	valuesLen := int64(binary.LittleEndian.Uint32(bucketBuf[4:8]))
	if valuesLen >= s.bucketCap {
		if err := s.setBucketCapacity(s.bucketCap * 2); err != nil {
			return err
		}
		bucketBuf = s.bucketBuf[0:bucketLen(s.bucketCap)]
		zero.Bytes(bucketBuf)
		if err := readBucket(s.f, bucketBuf, off, s.bucketCap); err != nil {
			return fmt.Errorf("readBucket: %e", err)
		}
	}
	// finally update the length of items we have, append the value, and write the bucket back out
	binary.LittleEndian.PutUint32(bucketBuf[4:8], uint32(valuesLen+1))
	binary.LittleEndian.PutUint32(bucketBuf[8+4*valuesLen:8+4*valuesLen+4], uint32(n))
	_, err := s.f.WriteAt(bucketBuf, off*int64(len(bucketBuf)))
	return err
}

type Bucket struct {
	N      int64
	Values []uint32
}

func (s *BucketSlice) Bucket(off int) (Bucket, error) {
	bucketSize := bucketLen(s.bucketCap)
	if len(s.bucketBuf) < int(bucketSize) {
		s.bucketBuf = make([]byte, bucketSize)
	}
	buf := s.bucketBuf[:bucketSize]
	zero.Bytes(buf)
	if err := readBucket(s.f, buf, int64(off), s.bucketCap); err != nil {
		return Bucket{}, err
	}

	n := int(binary.LittleEndian.Uint32(buf[0:4]))
	valuesLen := int64(binary.LittleEndian.Uint32(buf[4:8]))
	if int64(len(s.valuesBuf)) < valuesLen {
		s.valuesBuf = make([]uint32, valuesLen)
	}
	if valuesLen >= s.bucketCap {
		panic(fmt.Errorf("invariant broken: bucket %d overflowed", off))
	}
	values := s.valuesBuf[0:valuesLen]
	zero.Uint32(values)
	for i := int64(0); i < valuesLen; i++ {
		v := binary.LittleEndian.Uint32(buf[8+4*i : 8+4*i+4])
		values[i] = v
	}

	return Bucket{
		N:      int64(n),
		Values: values,
	}, nil
}

// Len is used to fulfil the sort.Interface.
func (s *BucketSlice) Len() int {
	return int(s.nBuckets)
}

// Less is used to fulfil the sort.Interface.
func (s *BucketSlice) Less(i, j int) bool {
	l := bucketLen(s.bucketCap)
	if len(s.bucketBuf) < int(l*2) {
		s.bucketBuf = make([]byte, l*2)
	}
	iBuf := s.bucketBuf[:l]
	jBuf := s.bucketBuf[l:]
	zero.Bytes(iBuf)
	zero.Bytes(jBuf)
	// capacity of zero -- we only care about the headers
	if err := readBucket(s.f, iBuf[:], int64(i), s.bucketCap); err != nil {
		panic(err)
	}
	if err := readBucket(s.f, jBuf[:], int64(j), s.bucketCap); err != nil {
		panic(err)
	}
	iLen := int(binary.LittleEndian.Uint32(iBuf[4:8]))
	jLen := int(binary.LittleEndian.Uint32(jBuf[4:8]))
	return iLen > jLen
}

// Swap is used to fulfil the sort.Interface.
func (s *BucketSlice) Swap(i, j int) {
	l := bucketLen(s.bucketCap)
	// use a single []byte buffer, cleaved in two
	if len(s.bucketBuf) < int(l*2) {
		s.bucketBuf = make([]byte, l*2)
	}
	iBuf := s.bucketBuf[:l]
	jBuf := s.bucketBuf[l:]

	if err := readBucket(s.f, iBuf, int64(i), s.bucketCap); err != nil {
		panic(err)
	}
	if err := readBucket(s.f, jBuf, int64(j), s.bucketCap); err != nil {
		panic(err)
	}
	if _, err := s.f.WriteAt(jBuf, int64(i)*l); err != nil {
		panic(err)
	}
	if _, err := s.f.WriteAt(iBuf, int64(j)*l); err != nil {
		panic(err)
	}
}
