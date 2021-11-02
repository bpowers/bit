// Copyright 2021 The bit Authors and Caleb Spare. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package index

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"reflect"
	"sort"
	"unsafe"

	"github.com/bpowers/bit/internal/exp/mmap"

	"github.com/dgryski/go-farm"
)

const (
	magicIndexHeader = uint32(0xC0FFEE01)
)

type Entry struct {
	Key    string
	Offset uint64
}

// Table is an immutable hash table that provides constant-time lookups of key
// indices using a minimal perfect hash.
type Table struct {
	offsets    []uint64
	level0     []uint32 // power of 2 size
	level0Mask uint32   // len(Level0) - 1
	level1     []uint32 // power of 2 size >= len(keys)
	level1Mask uint32   // len(Level1) - 1
}

func s2b(s string) (b []byte) {
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := *(*reflect.StringHeader)(unsafe.Pointer(&s))
	bh.Data = sh.Data
	bh.Len = sh.Len
	bh.Cap = sh.Len
	return b
}

type BuildInputer interface {
	Next() bool
	Entry() *Entry
	KeyAt(off uint64) ([]byte, []byte, error) // random access
	Len() uint64
}

// Build builds a Table from keys using the "Hash, displace, and compress"
// algorithm described in http://cmph.sourceforge.net/papers/esa09.pdf.
func Build(in BuildInputer) *Table {
	entryLen := int(in.Len())
	var (
		level0        = make([]uint32, nextPow2(entryLen/4))
		level0Mask    = uint32(len(level0) - 1)
		level1        = make([]uint32, nextPow2(entryLen))
		level1Mask    = uint32(len(level1) - 1)
		sparseBuckets = make([][]int, len(level0))
	)

	offsets := make([]uint64, entryLen)

	i := 0
	for in.Next() {
		e := in.Entry()
		n := uint32(farm.Hash64WithSeed(s2b(e.Key), 0)) & level0Mask
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

	// could be bitset
	occ := make([]bool, len(level1))
	var tmpOcc []uint32
	for _, bucket := range buckets {
		seed := uint64(1)
	trySeed:
		tmpOcc = tmpOcc[:0]
		for _, i := range bucket.vals {
			key, _, err := in.KeyAt(offsets[i])
			if err != nil {
				// TODO: fixme
				panic(err)
			}
			n := uint32(farm.Hash64WithSeed(key, seed)) & level1Mask
			if occ[n] {
				for _, n := range tmpOcc {
					occ[n] = false
				}
				seed++
				goto trySeed
			}
			occ[n] = true
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

func nextPow2(n int) int {
	return 1 << (32 - bits.LeadingZeros32(uint32(n)))
}

// MaybeLookupString searches for s in t and returns its potential index.
func (t *Table) MaybeLookupString(s string) uint64 {
	return t.MaybeLookup(s2b(s))
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

// Write writes the table out to the given file
func (t *Table) Write(w io.Writer) error {
	bw := bufio.NewWriterSize(w, 4*1024*1024)
	defer func() {
		_ = bw.Flush()
	}()

	if err := binary.Write(bw, binary.LittleEndian, magicIndexHeader); err != nil {
		return err
	}
	// version
	if err := binary.Write(bw, binary.LittleEndian, uint32(1)); err != nil {
		return err
	}
	if err := binary.Write(bw, binary.LittleEndian, uint32(len(t.offsets))); err != nil {
		return err
	}
	if err := binary.Write(bw, binary.LittleEndian, uint32(len(t.level0))); err != nil {
		return err
	}
	if err := binary.Write(bw, binary.LittleEndian, uint32(len(t.level1))); err != nil {
		return err
	}
	const padding = uint32(0)
	if err := binary.Write(bw, binary.LittleEndian, padding); err != nil {
		return err
	}

	// we should be 8-byte aligned at this point (an even number of 4-byte writes above)

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
		return nil, fmt.Errorf("bad magic number on data file %s (%x) -- not bit dataio file or corrupted", path, fileMagic)
	}

	fileFormatVersion := binary.LittleEndian.Uint32(m[4:8])
	if fileFormatVersion != 1 {
		return nil, fmt.Errorf("this version of the bit library can only read v1 data files; found v%d", fileFormatVersion)
	}

	offsetLen := binary.LittleEndian.Uint32(m[8:12])
	level0Len := binary.LittleEndian.Uint32(m[12:16])
	level1Len := binary.LittleEndian.Uint32(m[16:20])

	rest := m[24:]
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
	return t.MaybeLookup(s2b(s))
}

// MaybeLookup searches for b in t and returns its potential index.
func (t *FlatTable) MaybeLookup(b []byte) uint64 {
	i0 := uint64(uint32(farm.Hash64WithSeed(b, 0)) & t.level0Mask)
	seed := uint64(binary.LittleEndian.Uint32(t.level0[i0*4 : i0*4+4]))
	i1 := uint64(uint32(farm.Hash64WithSeed(b, seed)) & t.level1Mask)
	n := binary.LittleEndian.Uint32(t.level1[i1*4 : i1*4+4])
	return binary.LittleEndian.Uint64(t.offsets[n*8 : n*8+8])
}
