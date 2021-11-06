package bitset

type Bitset struct {
	bits   []uint64
	length int64
}

func (b *Bitset) Set(off int64) {
	if off >= b.length {
		return
	}
	sliceOff := off / 64
	bitOff := uint64(off) % 64
	u64 := &b.bits[sliceOff]
	*u64 |= 1 << bitOff
}

func (b *Bitset) Clear(off int64) {
	if off >= b.length {
		return
	}
	sliceOff := off / 64
	bitOff := uint64(off) % 64
	u64 := &b.bits[sliceOff]
	*u64 &= ^(1 << bitOff)
}

func (b *Bitset) IsSet(off int64) bool {
	if off >= b.length {
		return false
	}
	sliceOff := off / 64
	bitOff := uint64(off) % 64
	u64 := &b.bits[sliceOff]
	return *u64&(1<<bitOff) != 0
}

func New(length int64) *Bitset {
	sliceLen := (length + 63) / 64
	return &Bitset{
		bits:   make([]uint64, sliceLen),
		length: length,
	}
}
