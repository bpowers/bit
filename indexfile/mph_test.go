// Copyright 2021 The bit Authors and Caleb Spare. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package indexfile

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/bpowers/bit/internal/ondisk"

	"github.com/stretchr/testify/require"

	"github.com/dgryski/go-farm"

	"github.com/bpowers/bit/datafile"
	"github.com/bpowers/bit/internal/bitset"
	"github.com/bpowers/bit/internal/unsafestring"
)

type testEntry struct {
	Key    string
	Value  string
	Offset uint64
}

type testIter struct {
	items  []testEntry
	cancel func()
	ch     chan datafile.IterItem
}

// Close cleans up the iterator, closing the iteration channel and freeing resources.
func (i *testIter) Close() {
	i.cancel()
}

func (i *testIter) Iter() <-chan datafile.IterItem {
	// unbuffered
	ctx, cancel := context.WithCancel(context.Background())
	i.cancel = cancel
	i.ch = make(chan datafile.IterItem, 0)
	go i.producer(ctx, i.ch)
	return i.ch
}

func (i *testIter) producer(ctx context.Context, ch chan<- datafile.IterItem) {
	defer close(ch)

	for off := int64(0); off < int64(len(i.items)); off++ {
		item := i.items[off]
		iitem := datafile.IterItem{
			Key:    []byte(item.Key),
			Value:  []byte(item.Value),
			Offset: off,
		}
		select {
		case ch <- iitem:
		case <-ctx.Done():
			break
		}
	}
}

func (i *testIter) Len() int64 {
	return int64(len(i.items))
}

func (i *testIter) ReadAt(off int64) (key []byte, value []byte, err error) {
	if off >= int64(len(i.items)) {
		return nil, nil, errors.New("off too big")
	}
	item := i.items[off]
	return []byte(item.Key), []byte(item.Value), nil
}

func TestNextPow2(t *testing.T) {
	for _, testcase := range []struct {
		input    int64
		expected int64
	}{
		{1, 2},
		{2, 4},
		{3, 4},
		{31, 32},
	} {
		actual := nextPow2(testcase.input)
		require.Equal(t, testcase.expected, actual)
	}
}

func TestBuild_simple(t *testing.T) {
	testTable(t, []string{"foo", "foo2", "bar", "baz"}, []string{"quux"})
}

func TestBuild_stress(t *testing.T) {
	var keys, extra []string
	for i := 0; i < 20000; i++ {
		s := strconv.Itoa(i)
		if i < 10000 {
			keys = append(keys, s)
		} else {
			extra = append(extra, s)
		}
	}
	testTable(t, keys, extra)
}

func testTable(t *testing.T, keys []string, extra []string) {
	entries := make([]testEntry, len(keys))
	for i, key := range keys {
		entries[i] = testEntry{Key: key, Offset: uint64(i)}
	}
	it := &testIter{items: entries}
	defer it.Close()
	f := tmpTestfile()
	defer f.Close()
	if err := Build(f, it); err != nil {
		panic(err)
	}
	table, err := NewTable(f.Name())
	if err != nil {
		panic(err)
	}
	for i, key := range keys {
		n := table.MaybeLookupString(key)
		if int(n) != i {
			t.Errorf("Lookup(%s): got n=%d; want %d", key, n, i)
		}
	}
}

var (
	words          []testEntry
	wordsOnce      sync.Once
	benchTable     *InMemoryTable
	benchFlatTable *Table
)

func tmpTestfile() *os.File {
	f, err := os.CreateTemp("", "bit-test.*.index")
	if err != nil {
		panic(err)
	}
	return f
}

func BenchmarkBuild(b *testing.B) {
	wordsOnce.Do(loadBenchTable)
	if len(words) == 0 {
		b.Skip("unable to load dictionary file")
	}
	f := tmpTestfile()
	defer f.Close()
	for i := 0; i < b.N; i++ {
		if err := Build(f, &testIter{items: words}); err != nil {
			panic(err)
		}
	}
}

func BenchmarkTable(b *testing.B) {
	wordsOnce.Do(loadBenchTable)
	if len(words) == 0 {
		b.Skip("unable to load dictionary file")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := i % len(words)
		n := benchTable.MaybeLookupString(string(words[j].Key))
		if n != uint64(j) {
			b.Fatal("bad result index")
		}
	}
}

func BenchmarkFlatTable(b *testing.B) {
	wordsOnce.Do(loadBenchTable)
	if len(words) == 0 {
		b.Skip("unable to load dictionary file")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := i % len(words)
		n := benchFlatTable.MaybeLookupString(string(words[j].Key))
		if n != uint64(j) {
			b.Fatal("bad result index")
		}
	}
}

// For comparison against BenchmarkTable.
func BenchmarkTableMap(b *testing.B) {
	wordsOnce.Do(loadBenchTable)
	if len(words) == 0 {
		b.Skip("unable to load dictionary file")
	}
	m := make(map[string]uint64)
	for _, word := range words {
		m[string(word.Key)] = word.Offset
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := i % len(words)
		n, ok := m[string(words[j].Key)]
		if !ok {
			b.Fatal("missing key")
		}
		if n != uint64(j) {
			b.Fatal("bad result index")
		}
	}
}

func loadBenchTable() {
	for _, dict := range []string{"/usr/share/dict/words", "/usr/dict/words"} {
		var err error
		words, err = loadDict(dict)
		if err == nil {
			break
		}
	}
	if len(words) > 0 {
		it := &testIter{items: words}
		defer it.Close()
		benchTable = BuildInMemory(it)

		f := tmpTestfile()
		defer f.Close()
		if err := benchTable.Write(f); err != nil {
			panic(err)
		}
		var err error
		benchFlatTable, err = NewTable(f.Name())
		if err != nil {
			panic(err)
		}
	}
}

func loadDict(dict string) ([]testEntry, error) {
	f, err := os.Open(dict)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	var words []testEntry
	i := uint64(0)
	for scanner.Scan() {
		words = append(words, testEntry{Key: string(scanner.Bytes()), Offset: i})
		i++
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return words, nil
}

// InMemoryTable is an immutable hash table that provides constant-time lookups of key
// indices using a minimal perfect hash.
type InMemoryTable struct {
	offsets    []int64
	level0     []uint32 // power of 2 size
	level0Mask uint32   // len(Level0) - 1
	level1     []uint32 // power of 2 size >= len(keys)
	level1Mask uint32   // len(Level1) - 1
}

// BuildInMemory builds a InMemoryTable from keys using the "Hash, displace, and compress"
// algorithm described in http://cmph.sourceforge.net/papers/esa09.pdf.
func BuildInMemory(it datafile.Iter) *InMemoryTable {
	entryLen := it.Len()
	var (
		level0        = make([]uint32, nextPow2(entryLen/4))
		level0Mask    = uint32(len(level0) - 1)
		level1        = make([]uint32, nextPow2(entryLen))
		level1Mask    = uint32(len(level1) - 1)
		sparseBuckets = make([][]uint32, len(level0))
	)

	offsets := make([]int64, entryLen)

	i := 0
	for e := range it.Iter() {
		n := uint32(farm.Hash64WithSeed(e.Key, 0)) & level0Mask
		sparseBuckets[n] = append(sparseBuckets[n], uint32(i))
		offsets[i] = e.Offset
		i++
	}
	var buckets []ondisk.Bucket
	for n, vals := range sparseBuckets {
		if len(vals) > 0 {
			buckets = append(buckets, ondisk.Bucket{N: int64(n), Values: vals})
		}
	}
	sort.Sort(bySize(buckets))

	occ := bitset.New(int64(len(level1)))
	var tmpOcc []uint32
	for _, bucket := range buckets {
		seed := uint64(1)
	trySeed:
		tmpOcc = tmpOcc[:0]
		for _, i := range bucket.Values {
			key, _, err := it.ReadAt(offsets[i])
			if err != nil {
				// TODO: fixme
				panic(err)
			}
			n := uint32(farm.Hash64WithSeed(key, seed)) & level1Mask
			if occ.IsSet(int64(n)) {
				for _, n := range tmpOcc {
					occ.Clear(int64(n))
				}
				seed++
				goto trySeed
			}
			occ.Set(int64(n))
			tmpOcc = append(tmpOcc, n)
			level1[n] = uint32(i)
		}
		level0[bucket.N] = uint32(seed)
	}

	return &InMemoryTable{
		offsets:    offsets,
		level0:     level0,
		level0Mask: level0Mask,
		level1:     level1,
		level1Mask: level1Mask,
	}
}

// MaybeLookupString searches for s in t and returns its potential index.
func (t *InMemoryTable) MaybeLookupString(s string) uint64 {
	return t.MaybeLookup(unsafestring.ToBytes(s))
}

// MaybeLookup searches for b in t and returns its potential index.
func (t *InMemoryTable) MaybeLookup(b []byte) uint64 {
	i0 := uint32(farm.Hash64WithSeed(b, 0)) & t.level0Mask
	seed := uint64(t.level0[i0])
	i1 := uint32(farm.Hash64WithSeed(b, seed)) & t.level1Mask
	n := t.level1[i1]
	return uint64(t.offsets[int(n)])
}

// Write writes the table out to the given file
func (t *InMemoryTable) Write(w io.Writer) error {
	bw := bufio.NewWriterSize(w, 4*1024*1024)
	defer func() {
		_ = bw.Flush()
	}()

	if err := writeFileHeader(bw, int64(len(t.offsets)), int64(len(t.level0)), int64(len(t.level1))); err != nil {
		return fmt.Errorf("writeFileHeader: %e", err)
	}

	// we should be 8-byte aligned at this point (file header is 128-bytes wide)

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
