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
	"log"
	"math/bits"
	"os"
	"path"
	"sort"
	"syscall"

	"github.com/dgryski/go-farm"
	"golang.org/x/sys/unix"

	"github.com/bpowers/bit/datafile"
	"github.com/bpowers/bit/internal/bitset"
	"github.com/bpowers/bit/internal/exp/mmap"
	"github.com/bpowers/bit/internal/ondisk"
	"github.com/bpowers/bit/internal/unsafestring"
	"github.com/bpowers/bit/internal/zero"
)

const (
	magicIndexHeader = uint32(0xC0FFEE01)
	fileHeaderSize   = 128
	maxIndexEntries  = (1 << 31) - 1
	maxUint32        = ^uint32(0)
)

type BuildType int

const (
	FastHighMem BuildType = iota
	SlowLowMem
)

var (
	errorDuplicateKey = errors.New("duplicate keys aren't supported")
)

// Build builds a inMemoryTable from keys using the "Hash, displace, and compress"
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
	t, err := newInMemoryTable(it)
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
		level0Mask = uint32(level0Len - 1)
		level1Mask = uint32(level1Len - 1)
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
	for e := range it.Iter() {
		n := uint32(farm.Hash64WithSeed(e.Key, 0)) & level0Mask
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
			n := uint32(farm.Hash64WithSeed(key, seed)) & level1Mask
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

// nextPow2 returns the next highest power of two above a given number.
func nextPow2(n int64) int64 {
	return 1 << (64 - bits.LeadingZeros64(uint64(n)))
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

// Table is an index into a datafile, backed by an mmap'd file.
type Table struct {
	mm         *mmap.ReaderAt
	offsets    []byte
	level0     []byte
	level0Mask uint32
	level1     []byte
	level1Mask uint32
}

// NewTable returns a new `*indexfile.Table` based on the on-disk table at `path`.
func NewTable(path string) (*Table, error) {
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

	if err := unix.Madvise(m, syscall.MADV_RANDOM); err != nil {
		return nil, fmt.Errorf("madvise: %s", err)
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

	return &Table{
		mm:         mm,
		offsets:    offsets,
		level0:     level0,
		level0Mask: level0Len - 1,
		level1:     level1,
		level1Mask: level1Len - 1,
	}, nil
}

// MaybeLookupString searches for b in t and returns its potential index.
func (t *Table) MaybeLookupString(s string) uint64 {
	return t.MaybeLookup(unsafestring.ToBytes(s))
}

// MaybeLookup searches for b in t and returns its potential index.
func (t *Table) MaybeLookup(b []byte) uint64 {
	i0 := uint64(uint32(farm.Hash64WithSeed(b, 0)) & t.level0Mask)
	seed := uint64(binary.LittleEndian.Uint32(t.level0[i0*4 : i0*4+4]))
	i1 := uint64(uint32(farm.Hash64WithSeed(b, seed)) & t.level1Mask)
	n := binary.LittleEndian.Uint32(t.level1[i1*4 : i1*4+4])
	return binary.LittleEndian.Uint64(t.offsets[n*8 : n*8+8])
}

type bySize []ondisk.Bucket

func (s bySize) Len() int           { return len(s) }
func (s bySize) Less(i, j int) bool { return len(s[i].Values) > len(s[j].Values) }
func (s bySize) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// inMemoryTable is an immutable hash table that provides constant-time lookups of key
// indices using a minimal perfect hash.
type inMemoryTable struct {
	offsets    []int64
	level0     []uint32 // power of 2 size
	level0Mask uint32   // len(Level0) - 1
	level1     []uint32 // power of 2 size >= len(keys)
	level1Mask uint32   // len(Level1) - 1
}

// newInMemoryTable builds a inMemoryTable from keys using the "Hash, displace, and compress"
// algorithm described in http://cmph.sourceforge.net/papers/esa09.pdf.
func newInMemoryTable(it datafile.Iter) (*inMemoryTable, error) {
	var (
		entryLen  = it.Len()
		level0Len = nextPow2(entryLen / 4)
		level1Len = nextPow2(entryLen)
	)

	if level1Len >= (1<<32)-1 {
		return nil, fmt.Errorf("level1Len too big %d (too many entries)", level1Len)
	}

	var (
		level0Mask = uint32(level0Len - 1)
		level1Mask = uint32(level1Len - 1)
	)

	var (
		offsets       = make([]int64, entryLen)
		level0        = make([]uint32, level0Len)
		level1        = make([]uint32, level1Len)
		sparseBuckets = make([][]uint32, level0Len)
	)

	log.Printf("building sparse buckets\n")

	{
		i := 0
		for e := range it.Iter() {
			n := uint32(farm.Hash64WithSeed(e.Key, 0)) & level0Mask
			sparseBuckets[n] = append(sparseBuckets[n], uint32(i))
			offsets[i] = e.Offset
			i++
		}
	}

	log.Printf("collating sparse buckets\n")

	var buckets []ondisk.Bucket
	for n, vals := range sparseBuckets {
		if len(vals) > 0 {
			buckets = append(buckets, ondisk.Bucket{N: int64(n), Values: vals})
		}
	}

	log.Printf("sorting sparse buckets\n")
	sort.Sort(bySize(buckets))
	log.Printf("done sorting sparse buckets\n")

	log.Printf("iterating over %d buckets\n", len(buckets))
	occ := bitset.New(int64(len(level1)))
	var tmpOcc []uint32
	for j, bucket := range buckets {
		if j%1000000 == 0 {
			log.Printf("at bucket %d\n", j)
		}
		seed := uint64(1)
	trySeed:
		if seed >= uint64(maxUint32) {
			return nil, errors.New("couldn't find 32-bit seed")
		}
		tmpOcc = tmpOcc[:0]
		for _, i := range bucket.Values {
			key, _, err := it.ReadAt(offsets[i])
			if err != nil {
				return nil, err
			}
			n := uint32(farm.Hash64WithSeed(key, seed)) & level1Mask
			if occ.IsSet(int64(n)) {
				for _, n := range tmpOcc {
					occ.Clear(int64(n))
				}
				seed++
				goto trySeed
			}
			occ.Set(int64(n))
			tmpOcc = append(tmpOcc, n)
			level1[n] = i
		}
		level0[bucket.N] = uint32(seed)
	}

	return &inMemoryTable{
		offsets:    offsets,
		level0:     level0,
		level0Mask: level0Mask,
		level1:     level1,
		level1Mask: level1Mask,
	}, nil
}

// MaybeLookupString searches for s in t and returns its potential index.
func (t *inMemoryTable) MaybeLookupString(s string) uint64 {
	return t.MaybeLookup(unsafestring.ToBytes(s))
}

// MaybeLookup searches for b in t and returns its potential index.
func (t *inMemoryTable) MaybeLookup(b []byte) uint64 {
	i0 := uint32(farm.Hash64WithSeed(b, 0)) & t.level0Mask
	seed := uint64(t.level0[i0])
	i1 := uint32(farm.Hash64WithSeed(b, seed)) & t.level1Mask
	n := t.level1[i1]
	return uint64(t.offsets[int(n)])
}

// Write writes the table out to the given file
func (t *inMemoryTable) Write(w io.Writer) error {
	bw := bufio.NewWriterSize(w, 4*1024*1024)
	defer func() {
		_ = bw.Flush()
	}()

	if err := writeFileHeader(bw, int64(len(t.offsets)), int64(len(t.level0)), int64(len(t.level1))); err != nil {
		return fmt.Errorf("writeFileHeader: %e", err)
	}

	// we should be 8-byte aligned at this point (file header is 128-bytes wide)

	// write offsets first, while we're sure we're 8-byte aligned
	log.Printf("writing offsets\n")
	for _, i := range t.offsets {
		if err := binary.Write(bw, binary.LittleEndian, i); err != nil {
			return err
		}
	}

	log.Printf("writing level 0\n")
	for _, i := range t.level0 {
		if err := binary.Write(bw, binary.LittleEndian, i); err != nil {
			return err
		}
	}

	log.Printf("writing level 1\n")
	for _, i := range t.level1 {
		if err := binary.Write(bw, binary.LittleEndian, i); err != nil {
			return err
		}
	}

	return nil
}
