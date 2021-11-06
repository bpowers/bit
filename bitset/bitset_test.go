package bitset

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBitset(t *testing.T) {
	b := New(128)

	require.Equal(t, 2, len(b.bits))
	require.Equal(t, int64(128), b.length)

	// should do nothing
	b.Set(132)

	zero := []uint64{0, 0}
	require.Equal(t, zero, b.bits)

	require.False(t, b.IsSet(7))
	b.Set(7)
	require.True(t, b.IsSet(7))
	b.Set(8)
	require.True(t, b.IsSet(8))
	b.Clear(7)
	require.False(t, b.IsSet(7))
	require.True(t, b.IsSet(8))
	b.Clear(8)
	require.Equal(t, zero, b.bits)

	for i := int64(0); i < 128; i++ {
		b.Set(i)
	}

	full := []uint64{^uint64(0), ^uint64(0)}
	require.Equal(t, full, b.bits)

	// should do nothing
	b.Clear(137)
	require.Equal(t, full, b.bits)
}
