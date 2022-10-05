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
	"log"
	"sort"

	"github.com/dgryski/go-farm"

	"github.com/bpowers/bit/datafile"
	"github.com/bpowers/bit/internal/bitset"
	"github.com/bpowers/bit/internal/ondisk"
	"github.com/bpowers/bit/internal/unsafestring"
)

// bySize is used to sort our buckets from most full to least full
type bySize []ondisk.Bucket

func (s bySize) Len() int           { return len(s) }
func (s bySize) Less(i, j int) bool { return len(s[i].Values) > len(s[j].Values) }
func (s bySize) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// inMemoryBuilder is an immutable hash table that provides constant-time lookups of key
// indices using a minimal perfect hash.
type inMemoryBuilder struct {
	offsets    []int64
	level0     []uint32 // power of 2 size
	level0Mask uint64   // len(Level0) - 1
	level1     []uint32 // power of 2 size >= len(keys)
	level1Mask uint64   // len(Level1) - 1
}

// newInMemoryBuilder builds a inMemoryBuilder from keys using the "Hash, displace, and compress"
// algorithm described in http://cmph.sourceforge.net/papers/esa09.pdf.
func newInMemoryBuilder(it datafile.Iter) (*inMemoryBuilder, error) {
	var (
		entryLen  = it.Len()
		level0Len = nextPow2(entryLen / 4)
		level1Len = nextPow2(entryLen)
	)

	if level1Len >= (1<<32)-1 {
		return nil, fmt.Errorf("level1Len too big %d (too many entries)", level1Len)
	}

	var (
		level0Mask = uint64(level0Len - 1)
		level1Mask = uint64(level1Len - 1)
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
		for e, ok := it.Next(); ok; e, ok = it.Next() {
			n := farm.Hash64WithSeed(e.Key, 0) & level0Mask
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
			level1[n] = i
		}
		level0[bucket.N] = uint32(seed)
	}

	return &inMemoryBuilder{
		offsets:    offsets,
		level0:     level0,
		level0Mask: level0Mask,
		level1:     level1,
		level1Mask: level1Mask,
	}, nil
}

// MaybeLookupString searches for s in t and returns its potential index.
func (t *inMemoryBuilder) MaybeLookupString(s string) uint64 {
	return t.MaybeLookup(unsafestring.ToBytes(s))
}

// MaybeLookup searches for b in t and returns its potential index.
func (t *inMemoryBuilder) MaybeLookup(b []byte) uint64 {
	i0 := farm.Hash64WithSeed(b, 0) & t.level0Mask
	seed := uint64(t.level0[i0])
	i1 := farm.Hash64WithSeed(b, seed) & t.level1Mask
	n := t.level1[i1]
	return uint64(t.offsets[int(n)])
}

// Write writes the table out to the given file
func (t *inMemoryBuilder) Write(w io.Writer) error {
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
