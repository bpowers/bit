// Copyright 2022 The bit Authors and Caleb Spare. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package indexfile

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"

	"github.com/dgryski/go-farm"

	"github.com/bpowers/bit/datafile"
	"github.com/bpowers/bit/internal/bitset"
	"github.com/bpowers/bit/internal/ondisk"
	"github.com/bpowers/bit/internal/zero"
)

// Build builds a inMemoryBuilder from keys using the "Hash, displace, and compress"
// algorithm described in http://cmph.sourceforge.net/papers/esa09.pdf.
func Build(f *os.File, it datafile.Iter, buildType BuildType) error {
	if it.Len() > maxIndexEntries {
		return fmt.Errorf("too many elements -- we only support %d items in a bit index (%d asked for)", maxIndexEntries, it.Len())
	}

	switch buildType {
	case FastHighMem:
		return buildInCore(f, it)
	case SlowLowMem:
		return buildOutOfCore(f, it)
	default:
		return errors.New("unknown buildType argument")
	}
}

func buildInCore(f *os.File, it datafile.Iter) error {
	t, err := newInMemoryBuilder(it)
	if err != nil {
		return err
	}

	return t.Write(f)
}

func buildOutOfCore(f *os.File, it datafile.Iter) error {
	var (
		entryLen  = it.Len()
		level0Len = nextPow2(entryLen / 4)
		level1Len = nextPow2(entryLen)
	)

	if level1Len >= (1<<32)-1 {
		return fmt.Errorf("level1Len too big %d (too many entries)", level1Len)
	}

	var (
		level0Mask = uint64(level0Len - 1)
		level1Mask = uint64(level1Len - 1)
	)

	// the file header is used when we open the table in the future
	if err := writeFileHeader(f, entryLen, level0Len, level1Len); err != nil {
		return fmt.Errorf("writeFileHeader: %e", err)
	}

	// I think this isn't strictly necessary, but can't hurt
	if err := f.Truncate(fileHeaderSize + entryLen*8 + level0Len*4 + level1Len*4); err != nil {
		return fmt.Errorf("truncate: %e", err)
	}

	var (
		offsets = ondisk.NewUint64Array(f, entryLen, fileHeaderSize)
		level0  = ondisk.NewUint32Array(f, level0Len, fileHeaderSize+entryLen*8)
		level1  = ondisk.NewUint32Array(f, level1Len, fileHeaderSize+entryLen*8+level0Len*4)
	)

	bw := bufio.NewWriterSize(f, 4*1024*1024)

	bucketFile, err := os.CreateTemp(path.Dir(f.Name()), "bit-buildindex.*.buckets")
	if err != nil {
		return fmt.Errorf("os.CreateTemp: %e", err)
	}
	defer func() {
		_ = os.Remove(bucketFile.Name())
		_ = bucketFile.Close()
	}()
	buckets, err := ondisk.NewBucketSlice(bucketFile, level0Len)
	if err != nil {
		return fmt.Errorf("ondisk.NewBucketSlice: %e", err)
	}

	valueBuf := make([]byte, 8)
	i := 0
	for e, ok := it.Next(); ok; e, ok = it.Next() {
		n := farm.Hash64WithSeed(e.Key, 0) & level0Mask
		if err := buckets.AddToBucket(int64(n), int32(i)); err != nil {
			return err
		}
		binary.LittleEndian.PutUint64(valueBuf, uint64(e.Offset))
		if _, err := bw.Write(valueBuf); err != nil {
			return err
		}
		i++
	}

	if err := bw.Flush(); err != nil {
		return err
	}

	// sort the buckets in order of occupancy: we want to start with the higher-occupancy
	// buckets first, as it will be easier to satisfy the `trySeed` loop below for them
	// early on.
	sort.Sort(buckets)

	// the first bucket is the most full after our sort, so use that in sizing our buffers
	firstBucket, err := buckets.Bucket(0)
	if err != nil {
		return err
	}
	keys := make([][]byte, len(firstBucket.Values))
	results := make([]uint32, len(firstBucket.Values))

	occ := bitset.New(level1Len)
	var tmpOcc []uint32
	for i := 0; i < buckets.Len(); i++ {
		bucket, err := buckets.Bucket(i)
		if err != nil {
			return fmt.Errorf("buckets.Bucket(%d): %e", i, err)
		}
		// this is likely our exit condition: we will have some empty buckets
		// because our hash function isn't perfect
		if len(bucket.Values) == 0 {
			break
		}
		seed := uint64(1)
		// we may retry the `trySeed` loop below multiple times -- ensure we
		// only have to read the keys off disk once
		keys = keys[:len(bucket.Values)]
		zero.ByteSlices(keys)
		results = results[:len(bucket.Values)]
		zero.Uint32(results)

		for i, n := range bucket.Values {
			off, err := offsets.Get(int64(n))
			if err != nil {
				return err
			}
			key, _, err := it.ReadAt(int64(off))
			if err != nil {
				return err
			}
			keys[i] = key
		}
	trySeed:
		tmpOcc = tmpOcc[:0]
		for i := range bucket.Values {
			key := keys[i]
			n := uint32(farm.Hash64WithSeed(key, seed) & level1Mask)
			if occ.IsSet(int64(n)) {
				for _, n := range tmpOcc {
					occ.Clear(int64(n))
				}
				seed++
				goto trySeed
			}
			occ.Set(int64(n))
			tmpOcc = append(tmpOcc, n)
			results[i] = n
		}
		for i, n := range results {
			if err := level1.Set(int64(n), bucket.Values[i]); err != nil {
				return err
			}
		}
		if err := level0.Set(bucket.N, uint32(seed)); err != nil {
			return err
		}
	}

	return nil
}

func writeFileHeader(w io.Writer, offsetsLen, level0Len, level1Len int64) error {
	var buf [fileHeaderSize]byte

	// TODO: ensure lengths fit in uint32s

	binary.LittleEndian.PutUint32(buf[0:4], magicIndexHeader)
	binary.LittleEndian.PutUint32(buf[4:8], 1) // file version
	binary.LittleEndian.PutUint32(buf[8:12], uint32(offsetsLen))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(level0Len))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(level1Len))

	_, err := w.Write(buf[:])
	return err
}
