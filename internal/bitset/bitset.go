// Copyright 2021 The bit Authors and Caleb Spare. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package bitset

// Bitset is an in-memory bitmap that is conceptually similar to []bool, but more memory efficient.
type Bitset struct {
	bits   []uint64
	length int64
}

func getOffsets(off int64) (sliceOff int64, bitOff uint64) {
	sliceOff = off / 64
	bitOff = uint64(off) % 64
	return
}

// Set sets the bit at position `off` to 1.
func (b *Bitset) Set(off int64) {
	if off >= b.length {
		return
	}
	sliceOff, bitOff := getOffsets(off)
	u64 := &b.bits[sliceOff]
	*u64 |= 1 << bitOff
}

// Clear sets the bit at position `off` to 0.
func (b *Bitset) Clear(off int64) {
	if off >= b.length {
		return
	}
	sliceOff, bitOff := getOffsets(off)
	u64 := &b.bits[sliceOff]
	*u64 &= ^(1 << bitOff)
}

// IsSet returns true if the bit at position `off` is 1.
func (b *Bitset) IsSet(off int64) bool {
	if off >= b.length {
		return false
	}
	sliceOff, bitOff := getOffsets(off)
	u64 := &b.bits[sliceOff]
	return *u64&(1<<bitOff) != 0
}

// New returns a new in-memory bitset where you can set, clear and test for individual bits.
func New(length int64) *Bitset {
	sliceLen := (length + 63) / 64
	return &Bitset{
		bits:   make([]uint64, sliceLen),
		length: length,
	}
}
