// Copyright 2022 The bit Authors and Caleb Spare. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package indexfile

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/bits"
	"syscall"

	"github.com/dgryski/go-farm"
	"golang.org/x/sys/unix"

	"github.com/bpowers/bit/datafile"
	"github.com/bpowers/bit/internal/exp/mmap"
	"github.com/bpowers/bit/internal/unsafestring"
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
)

var (
	errorDuplicateKey = errors.New("duplicate keys aren't supported")
)

// nextPow2 returns the next highest power of two above a given number.
func nextPow2(n int64) int64 {
	return 1 << (64 - bits.LeadingZeros64(uint64(n)))
}

// uint64Slice is a read-only view into a byte array as if it was []uint64
type uint64Slice []byte

// uint32Slice is a read-only view into a byte array as if it was []uint32
type uint32Slice []byte

func (s uint32Slice) Get(off uint64) uint32 {
	return binary.LittleEndian.Uint32(s[off*4 : off*4+4])
}

func (s uint64Slice) Get(off uint64) uint64 {
	return binary.LittleEndian.Uint64(s[off*8 : off*8+8])
}

// Table is an index into a datafile, backed by an mmap'd file.
type Table struct {
	mm          *mmap.ReaderAt
	seeds       uint32Slice
	seedsMask   uint64
	offsets     uint64Slice
	offsetsMask uint64
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
	if fileFormatVersion != 2 {
		return nil, fmt.Errorf("this version of the bit library can only read v1 data files; found v%d", fileFormatVersion)
	}

	if err := unix.Madvise(m, syscall.MADV_RANDOM); err != nil {
		return nil, fmt.Errorf("madvise: %s", err)
	}

	log.Printf("mlocking the index into memory\n")
	if err := unix.Mlock(m); err != nil {
		log.Printf("failed to mlock the index, continuing anyway: %s\n", err)
	} else {
		log.Printf("finished mlocking the index into memory\n")
	}

	level0Len := uint64(binary.LittleEndian.Uint32(m[12:16]))
	level1Len := uint64(binary.LittleEndian.Uint32(m[16:20]))

	rest := m[fileHeaderSize:]
	level1 := rest[:level1Len*8]
	level0 := rest[level1Len*8 : level1Len*8+level0Len*4]

	if uint64(len(level1)) != level1Len*8 {
		return nil, fmt.Errorf("bad len for offsets: %d (expected %d)", len(level1), level1Len)
	}

	return &Table{
		mm:          mm,
		seeds:       level0,
		seedsMask:   level0Len - 1,
		offsets:     level1,
		offsetsMask: level1Len - 1,
	}, nil
}

// MaybeLookupString searches for b in t and returns its potential index.
func (t *Table) MaybeLookupString(s string) datafile.PackedOffset {
	return t.MaybeLookup(unsafestring.ToBytes(s))
}

// MaybeLookup searches for b in t and returns its potential index.
func (t *Table) MaybeLookup(b []byte) datafile.PackedOffset {
	// first we hash the key with a fixed seed, giving us the offset
	// of a seed that perfectly hashes into our second-level table
	seed := t.seeds.Get(farm.Hash64WithSeed(b, 0) & t.seedsMask)
	// next, we use that more-specific seed to re-hash the key, giving
	// us the offset into our array of 'values' (which in this case
	// are 64-bit indexes into the datafile, where the variable-sized
	// value actually lives).
	off := t.offsets.Get(farm.Hash64WithSeed(b, uint64(seed)) & t.offsetsMask)

	return datafile.PackedOffset(off)
}
