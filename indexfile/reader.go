// Copyright 2022 The bit Authors and Caleb Spare. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package indexfile

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"syscall"

	"github.com/dgryski/go-farm"
	"golang.org/x/sys/unix"

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
	SlowLowMem
)

var (
	errorDuplicateKey = errors.New("duplicate keys aren't supported")
)

// nextPow2 returns the next highest power of two above a given number.
func nextPow2(n int64) int64 {
	return 1 << (64 - bits.LeadingZeros64(uint64(n)))
}

// Table is an index into a datafile, backed by an mmap'd file.
type Table struct {
	mm         *mmap.ReaderAt
	offsets    []byte
	level0     []byte
	level0Mask uint64
	level1     []byte
	level1Mask uint64
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

	offsetLen := uint64(binary.LittleEndian.Uint32(m[8:12]))
	level0Len := uint64(binary.LittleEndian.Uint32(m[12:16]))
	level1Len := uint64(binary.LittleEndian.Uint32(m[16:20]))

	rest := m[fileHeaderSize:]
	offsets := rest[:offsetLen*8]
	level0 := rest[offsetLen*8 : offsetLen*8+level0Len*4]
	level1 := rest[offsetLen*8+level0Len*4:]

	if uint64(len(level1)) != level1Len*4 {
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
	i0 := farm.Hash64WithSeed(b, 0) & t.level0Mask
	seed := uint64(binary.LittleEndian.Uint32(t.level0[i0*4 : i0*4+4]))
	i1 := farm.Hash64WithSeed(b, seed) & t.level1Mask
	n := binary.LittleEndian.Uint32(t.level1[i1*4 : i1*4+4])
	return binary.LittleEndian.Uint64(t.offsets[n*8 : n*8+8])
}
