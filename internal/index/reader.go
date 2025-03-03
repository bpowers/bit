// Copyright 2022 The bit Authors and Caleb Spare. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package index

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/bits"
	"syscall"

	"github.com/orisano/wyhash"
	"golang.org/x/sys/unix"

	"github.com/bpowers/bit/internal/datafile"
	"github.com/bpowers/bit/internal/unsafestring"
)

const (
	maxIndexEntries = (1 << 31) - 1
	maxUint32       = ^uint32(0)
)

var ErrDuplicateKey = errors.New("duplicate keys aren't supported")

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
	seeds       uint32Slice
	seedsMask   uint64
	offsets     uint64Slice
	offsetsMask uint64
}

// NewTable returns a new `*index.Table` based on the on-disk table at `path`.
func NewTable(tbl Built) (*Table, error) {
	m := tbl.Table

	if err := unix.Madvise(m, syscall.MADV_RANDOM); err != nil {
		return nil, fmt.Errorf("madvise: %s", err)
	}

	log.Printf("mlocking the index into memory\n")
	if err := unix.Mlock(m); err != nil {
		log.Printf("failed to mlock the index, continuing anyway: %s\n", err)
	} else {
		log.Printf("finished mlocking the index into memory\n")
	}

	level0Len := tbl.Level0Len
	level1Len := tbl.Level1Len

	rest := m
	level1 := rest[:level1Len*8]
	level0 := rest[level1Len*8 : level1Len*8+level0Len*4]

	if uint64(len(level1)) != level1Len*8 {
		return nil, fmt.Errorf("bad len for offsets: %d (expected %d)", len(level1), level1Len)
	}

	return &Table{
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
	seed := t.seeds.Get(wyhash.Sum64(0, b) & t.seedsMask)
	// next, we use that more-specific seed to re-hash the key, giving
	// us the offset into our array of 'values' (which in this case
	// are 64-bit indexes into the datafile, where the variable-sized
	// value actually lives).
	off := t.offsets.Get(wyhash.Sum64(uint64(seed), b) & t.offsetsMask)

	return datafile.PackedOffset(off)
}
