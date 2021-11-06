// Copyright 2021 The bit Authors and Caleb Spare. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package indexfile

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path"
	"sort"

	"github.com/dgryski/go-farm"

	"github.com/bpowers/bit/bitset"
	"github.com/bpowers/bit/datafile"
	"github.com/bpowers/bit/internal/exp/mmap"
	"github.com/bpowers/bit/internal/ondisk"
	"github.com/bpowers/bit/internal/unsafestring"
	"github.com/bpowers/bit/internal/zero"
)

const (
	magicIndexHeader = uint32(0xC0FFEE01)
	fileHeaderSize   = 128
)

var (
	errorDuplicateKey = errors.New("duplicate keys aren't supported")
)

// Table is an immutable hash table that provides constant-time lookups of key
// indices using a minimal perfect hash.
type Table struct {
	offsets    []uint64
	level0     []uint32 // power of 2 size
	level0Mask uint32   // len(Level0) - 1
	level1     []uint32 // power of 2 size >= len(keys)
	level1Mask uint32   // len(Level1) - 1
}

// Build builds a Table from keys using the "Hash, displace, and compress"
// algorithm described in http://cmph.sourceforge.net/papers/esa09.pdf.
func Build(it datafile.Iter) *Table {
	entryLen := int(it.Len())
	var (
		level0        = make([]uint32, nextPow2(entryLen/4))
		level0Mask    = uint32(len(level0) - 1)
		level1        = make([]uint32, nextPow2(entryLen))
		level1Mask    = uint32(len(level1) - 1)
		sparseBuckets = make([][]int, len(level0))
	)

	offsets := make([]uint64, entryLen)

	i := 0
	for e := range it.Iter() {
		n := uint32(farm.Hash64WithSeed(e.Key, 0)) & level0Mask
		sparseBuckets[n] = append(sparseBuckets[n], i)
		offsets[i] = e.Offset
		i++
	}
	var buckets []indexBucket
	for n, vals := range sparseBuckets {
		if len(vals) > 0 {
			buckets = append(buckets, indexBucket{n, vals})
		}
	}
	sort.Sort(bySize(buckets))

	occ := bitset.New(len(level1))
	var tmpOcc []uint32
	for _, bucket := range buckets {
		seed := uint64(1)
	trySeed:
		tmpOcc = tmpOcc[:0]
		for _, i := range bucket.vals {
			key, _, err := it.ReadAt(offsets[i])
			if err != nil {
				// TODO: fixme
				panic(err)
			}
			n := uint32(farm.Hash64WithSeed(key, seed)) & level1Mask
			if occ.IsSet(int(n)) {
				for _, n := range tmpOcc {
					occ.Clear(int(n))
				}
				seed++
				goto trySeed
			}
			occ.Set(int(n))
			tmpOcc = append(tmpOcc, n)
			level1[n] = uint32(i)
		}
		level0[bucket.n] = uint32(seed)
	}

	return &Table{
		offsets:    offsets,
		level0:     level0,
		level0Mask: level0Mask,
		level1:     level1,
		level1Mask: level1Mask,
	}
}

// BuildFlat builds a Table from keys using the "Hash, displace, and compress"
// algorithm described in http://cmph.sourceforge.net/papers/esa09.pdf.
func BuildFlat(f *os.File, it datafile.Iter) error {
	var (
		entryLen  = int(it.Len())
		level0Len = nextPow2(entryLen / 4)
		level1Len = nextPow2(entryLen)

		level0Mask = uint32(level0Len - 1)
		level1Mask = uint32(level1Len - 1)
	)

	if err := writeFileHeader(f, entryLen, level0Len, level1Len); err != nil {
		return fmt.Errorf("writeFileHeader: %e", err)
	}

	if err := f.Truncate(int64(fileHeaderSize + entryLen*8 + level0Len*4 + level1Len*4)); err != nil {
		return fmt.Errorf("truncate: %e", err)
	}
	var (
		offsets = ondisk.NewU64Slice(f, entryLen, fileHeaderSize)
		level0  = ondisk.NewU32Slice(f, level0Len, int64(fileHeaderSize+entryLen*8))
		level1  = ondisk.NewU32Slice(f, level1Len, int64(fileHeaderSize+entryLen*8+level0Len*4))
	)

	bw := bufio.NewWriterSize(f, 4*1024*1024)

	bucketFile, err := os.CreateTemp(path.Dir(f.Name()), "bit-buildindex.*.buckets")
	if err != nil {
		return fmt.Errorf("os.CreateTemp: %e", err)
	}
	defer func() {
		_ = bucketFile.Close()
	}()
	buckets, err := ondisk.NewBucketSlice(bucketFile, level0Len)
	if err != nil {
		return fmt.Errorf("ondisk.NewBucketSlice: %e", err)
	}

	i := 0
	for e := range it.Iter() {
		n := uint32(farm.Hash64WithSeed(e.Key, 0)) & level0Mask
		if err := buckets.AddToBucket(int64(n), int32(i)); err != nil {
			return err
		}
		var valueBuf [8]byte
		binary.LittleEndian.PutUint64(valueBuf[:], e.Offset)
		if _, err := bw.Write(valueBuf[:]); err != nil {
			return err
		}
		i++
	}

	if err := bw.Flush(); err != nil {
		return err
	}

	sort.Sort(buckets)

	// the first bucket is the most full after our sort
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
			off, err := offsets.Get(int(n))
			if err != nil {
				return err
			}
			key, _, err := it.ReadAt(off)
			if err != nil {
				return err
			}
			keys[i] = key
		}
	trySeed:
		tmpOcc = tmpOcc[:0]
		for i := range bucket.Values {
			key := keys[i]
			n := uint32(farm.Hash64WithSeed(key, seed)) & level1Mask
			if occ.IsSet(int(n)) {
				for _, n := range tmpOcc {
					occ.Clear(int(n))
				}
				seed++
				goto trySeed
			}
			occ.Set(int(n))
			tmpOcc = append(tmpOcc, n)
			results[i] = n
		}
		for i, n := range results {
			if err := level1.Set(int(n), bucket.Values[i]); err != nil {
				return err
			}
		}
		if err := level0.Set(bucket.N, uint32(seed)); err != nil {
			return err
		}
	}

	return nil
}

func nextPow2(n int) int {
	return 1 << (32 - bits.LeadingZeros32(uint32(n)))
}

// MaybeLookupString searches for s in t and returns its potential index.
func (t *Table) MaybeLookupString(s string) uint64 {
	return t.MaybeLookup(unsafestring.ToBytes(s))
}

// MaybeLookup searches for b in t and returns its potential index.
func (t *Table) MaybeLookup(b []byte) uint64 {
	i0 := uint32(farm.Hash64WithSeed(b, 0)) & t.level0Mask
	seed := uint64(t.level0[i0])
	i1 := uint32(farm.Hash64WithSeed(b, seed)) & t.level1Mask
	n := t.level1[i1]
	return t.offsets[int(n)]
}

type indexBucket struct {
	n    int
	vals []int
}

type bySize []indexBucket

func (s bySize) Len() int           { return len(s) }
func (s bySize) Less(i, j int) bool { return len(s[i].vals) > len(s[j].vals) }
func (s bySize) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func writeFileHeader(w io.Writer, offsetsLen, level0Len, level1Len int) error {
	var buf [fileHeaderSize]byte

	binary.LittleEndian.PutUint32(buf[0:4], magicIndexHeader)
	binary.LittleEndian.PutUint32(buf[4:8], 1) // file version
	binary.LittleEndian.PutUint32(buf[8:12], uint32(offsetsLen))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(level0Len))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(level1Len))

	_, err := w.Write(buf[:])
	return err
}

// Write writes the table out to the given file
func (t *Table) Write(w io.Writer) error {
	bw := bufio.NewWriterSize(w, 4*1024*1024)
	defer func() {
		_ = bw.Flush()
	}()

	if err := writeFileHeader(bw, len(t.offsets), len(t.level0), len(t.level1)); err != nil {
		return fmt.Errorf("writeFileHeader: %e", err)
	}

	// we should be 8-byte aligned at this point (file header is 128-bytes wide)

	// write offsets first, while we're sure we're 8-byte aligned
	for _, i := range t.offsets {
		if err := binary.Write(bw, binary.LittleEndian, i); err != nil {
			return err
		}
	}

	for _, i := range t.level0 {
		if err := binary.Write(bw, binary.LittleEndian, i); err != nil {
			return err
		}
	}

	for _, i := range t.level1 {
		if err := binary.Write(bw, binary.LittleEndian, i); err != nil {
			return err
		}
	}

	return nil
}

type FlatTable struct {
	mm         *mmap.ReaderAt
	offsets    []byte
	level0     []byte
	level0Mask uint32
	level1     []byte
	level1Mask uint32
}

func NewFlatTable(path string) (*FlatTable, error) {
	mm, err := mmap.Open(path)
	if err != nil {
		return nil, fmt.Errorf("mmap.Open(%s): %e", path, err)
	}

	m := mm.Data()
	fileMagic := binary.LittleEndian.Uint32(m[:4])
	if fileMagic != magicIndexHeader {
		return nil, fmt.Errorf("bad magic number on index file %s (%x) -- not bit indexfile or corrupted", path, fileMagic)
	}

	fileFormatVersion := binary.LittleEndian.Uint32(m[4:8])
	if fileFormatVersion != 1 {
		return nil, fmt.Errorf("this version of the bit library can only read v1 data files; found v%d", fileFormatVersion)
	}

	offsetLen := binary.LittleEndian.Uint32(m[8:12])
	level0Len := binary.LittleEndian.Uint32(m[12:16])
	level1Len := binary.LittleEndian.Uint32(m[16:20])

	rest := m[fileHeaderSize:]
	offsets := rest[:offsetLen*8]
	level0 := rest[offsetLen*8 : offsetLen*8+level0Len*4]
	level1 := rest[offsetLen*8+level0Len*4:]

	if uint32(len(level1)) != level1Len*4 {
		return nil, fmt.Errorf("bad len for level1: %d (expected %d)", len(level1), level1Len)
	}

	return &FlatTable{
		mm:         mm,
		offsets:    offsets,
		level0:     level0,
		level0Mask: level0Len - 1,
		level1:     level1,
		level1Mask: level1Len - 1,
	}, nil
}

// MaybeLookupString searches for b in t and returns its potential index.
func (t *FlatTable) MaybeLookupString(s string) uint64 {
	return t.MaybeLookup(unsafestring.ToBytes(s))
}

// MaybeLookup searches for b in t and returns its potential index.
func (t *FlatTable) MaybeLookup(b []byte) uint64 {
	i0 := uint64(uint32(farm.Hash64WithSeed(b, 0)) & t.level0Mask)
	seed := uint64(binary.LittleEndian.Uint32(t.level0[i0*4 : i0*4+4]))
	i1 := uint64(uint32(farm.Hash64WithSeed(b, seed)) & t.level1Mask)
	n := binary.LittleEndian.Uint32(t.level1[i1*4 : i1*4+4])
	return binary.LittleEndian.Uint64(t.offsets[n*8 : n*8+8])
}
