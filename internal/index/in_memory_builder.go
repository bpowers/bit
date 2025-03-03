// Copyright 2022 The bit Authors and Caleb Spare. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sort"

	"github.com/orisano/wyhash"

	"github.com/bpowers/bit/internal/bitset"
	"github.com/bpowers/bit/internal/datafile"
	"github.com/bpowers/bit/internal/unsafestring"
)

var errNoSeedFound = errors.New("couldn't find 32-bit seed")

type set[T ~string] map[T]struct{}

func (s set[T]) Add(k T) {
	s[k] = struct{}{}
}

func (s set[T]) ContainsBytes(k []byte) bool {
	_, ok := s[T(k)]
	return ok
}

type Built struct {
	Table     []byte
	Level0Len uint64
	Level1Len uint64
}

// Build builds a inMemoryBuilder from keys using the "Hash, displace, and compress"
// algorithm described in http://cmph.sourceforge.net/papers/esa09.pdf.
func Build(it datafile.Iter) (Built, error) {
	if it.Len() > maxIndexEntries {
		return Built{}, fmt.Errorf("too many elements -- we only support %d items in a bit index (%d asked for)", maxIndexEntries, it.Len())
	}

	t, err := buildInMemory(it)
	if err != nil {
		return Built{}, err
	}

	return t.Write()
}

type Bucket struct {
	N      int64
	Values []uint32
}

// bySize is used to sort our buckets from most full to least full
type bySize []Bucket

func (s bySize) Len() int           { return len(s) }
func (s bySize) Less(i, j int) bool { return len(s[i].Values) > len(s[j].Values) }
func (s bySize) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// inMemoryBuilder is an immutable hash table that provides constant-time lookups of key
// indices using a minimal perfect hash.
type inMemoryBuilder struct {
	level0     []uint32                // power of 2 size
	level0Mask uint64                  // len(Level0) - 1
	level1     []datafile.PackedOffset // power of 2 size >= len(keys)
	level1Mask uint64                  // len(Level1) - 1
}

// buildInMemory builds a inMemoryBuilder from keys using the "Hash, displace, and compress"
// algorithm described in http://cmph.sourceforge.net/papers/esa09.pdf.
func buildInMemory(it datafile.Iter) (*inMemoryBuilder, error) {
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
		offsets = make([]datafile.PackedOffset, entryLen)
		level0  = make([]uint32, level0Len)
		// level1 is allocated below, to reduce our required max RSS
		sparseBuckets = make([][]uint32, level0Len)
	)

	// maintain a set of keys to check for duplicates - this is memory expensive,
	// so only do it if asked.
	keys := make(set[string], entryLen)

	log.Printf("building sparse buckets\n")

	{
		i := 0
		for e, ok := it.Next(); ok; e, ok = it.Next() {
			if keys != nil {
				if keys.ContainsBytes(e.Key) {
					return nil, fmt.Errorf("duplicate key: %q", e.Key)
				} else {
					keys.Add(string(e.Key))
				}
			}
			n := wyhash.Sum64(0, e.Key) & level0Mask
			sparseBuckets[n] = append(sparseBuckets[n], uint32(i))
			offsets[i] = e.PackedOffset()
			i++
		}
	}

	// done with keys, so we can free them here
	keys = nil

	log.Printf("collating sparse buckets\n")

	var buckets []Bucket
	for n, vals := range sparseBuckets {
		if len(vals) > 0 {
			buckets = append(buckets, Bucket{N: int64(n), Values: vals})
		}
	}

	// done with sparseBuckets, so free it too
	sparseBuckets = nil

	log.Printf("sorting sparse buckets\n")
	sort.Sort(bySize(buckets))
	log.Printf("done sorting sparse buckets\n")

	level1 := make([]datafile.PackedOffset, level1Len)

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
			return nil, errNoSeedFound
		}
		tmpOcc = tmpOcc[:0]
		for _, i := range bucket.Values {
			key, _, err := it.ReadAt(offsets[i])
			if err != nil {
				return nil, err
			}
			n := uint32(wyhash.Sum64(seed, key) & level1Mask)
			if occ.IsSet(int64(n)) {
				for _, n := range tmpOcc {
					occ.Clear(int64(n))
					level1[n] = 0
				}
				seed++
				goto trySeed
			}
			tmpOcc = append(tmpOcc, n)
			occ.Set(int64(n))
			level1[n] = offsets[i]
		}
		level0[bucket.N] = uint32(seed)
	}

	return &inMemoryBuilder{
		level0:     level0,
		level0Mask: level0Mask,
		level1:     level1,
		level1Mask: level1Mask,
	}, nil
}

// MaybeLookupString searches for s in t and returns its potential index.
func (t *inMemoryBuilder) MaybeLookupString(s string) datafile.PackedOffset {
	return t.MaybeLookup(unsafestring.ToBytes(s))
}

// MaybeLookup searches for b in t and returns its potential index.
func (t *inMemoryBuilder) MaybeLookup(b []byte) datafile.PackedOffset {
	i0 := wyhash.Sum64(0, b) & t.level0Mask
	seed := uint64(t.level0[i0])
	i1 := wyhash.Sum64(seed, b) & t.level1Mask
	return t.level1[i1]
}

// Write writes the table out to the given file
func (t *inMemoryBuilder) Write() (Built, error) {
	var buf bytes.Buffer

	tbl := Built{
		Level0Len: uint64(len(t.level0)),
		Level1Len: uint64(len(t.level1)),
	}

	// write offsets first, which has stricter alignment requirements
	log.Printf("writing level 1\n")
	for _, i := range t.level1 {
		if err := binary.Write(&buf, binary.LittleEndian, i); err != nil {
			return Built{}, err
		}
	}

	log.Printf("writing level 0\n")
	for _, i := range t.level0 {
		if err := binary.Write(&buf, binary.LittleEndian, i); err != nil {
			return Built{}, err
		}
	}

	tbl.Table = buf.Bytes()

	return tbl, nil
}
