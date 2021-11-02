package bitset

type Bitset struct {
	bits   []uint64
	length int
}

func (b *Bitset) Set(off int) {
	if off >= b.length {
		return
	}
	sliceOff := off / 64
	bitOff := uint64(off) % 64
	u64 := &b.bits[sliceOff]
	*u64 |= 1 << bitOff
}

func (b *Bitset) Clear(off int) {
	if off >= b.length {
		return
	}
	sliceOff := off / 64
	bitOff := uint64(off) % 64
	u64 := &b.bits[sliceOff]
	*u64 &= ^(1 << bitOff)
}

func (b *Bitset) IsSet(off int) bool {
	if off >= b.length {
		return false
	}
	sliceOff := off / 64
	bitOff := uint64(off) % 64
	u64 := &b.bits[sliceOff]
	return *u64&(1<<bitOff) != 0
}

func New(length int) *Bitset {
	sliceLen := (length + 63) / 64
	return &Bitset{
		bits:   make([]uint64, sliceLen),
		length: length,
	}
}
