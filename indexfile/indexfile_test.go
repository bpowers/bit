// Copyright 2021 The bit Authors and Caleb Spare. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package indexfile

import (
	"bufio"
	"context"
	"errors"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bpowers/bit/datafile"
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
	off    int64
}

func (i *testIter) Next() (datafile.IterItem, bool) {
	if i.off >= int64(len(i.items)) {
		return datafile.IterItem{}, false
	}

	item := i.items[i.off]
	iitem := datafile.IterItem{
		Key:    []byte(item.Key),
		Value:  []byte(item.Value),
		Offset: i.off,
	}

	i.off++

	return iitem, true
}

// Close cleans up the iterator, closing the iteration channel and freeing resources.
func (i *testIter) Close() {
	if i.cancel != nil {
		i.cancel()
	}
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

func (i *testIter) ReadAt(poff datafile.PackedOffset) (key []byte, value []byte, err error) {
	off, _ := poff.Unpack()
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
	if err := Build(f, it, FastHighMem); err != nil {
		panic(err)
	}
	table, err := NewTable(f.Name())
	if err != nil {
		panic(err)
	}
	for i, key := range keys {
		n := table.MaybeLookupString(key)
		off, _ := n.Unpack()
		if off != int64(i) {
			t.Errorf("Lookup(%s): got n=%d; want %d", key, n, i)
		}
	}
}

var (
	words          []testEntry
	wordsOnce      sync.Once
	benchTable     *inMemoryBuilder
	benchFlatTable *Table
)

func tmpTestfile() *os.File {
	f, err := os.CreateTemp("", "bit-test.*.index")
	if err != nil {
		panic(err)
	}
	return f
}

func BenchmarkMemoryBasedBuild(b *testing.B) {
	wordsOnce.Do(loadBenchTable)
	if len(words) == 0 {
		b.Skip("unable to load dictionary file")
	}
	f := tmpTestfile()
	defer f.Close()
	for i := 0; i < b.N; i++ {
		if err := Build(f, &testIter{items: words}, FastHighMem); err != nil {
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
		n := benchTable.MaybeLookupString(words[j].Key)
		off, _ := n.Unpack()
		if off != int64(j) {
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
		n := benchFlatTable.MaybeLookupString(words[j].Key)
		off, _ := n.Unpack()
		if off != int64(j) {
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
		var err error
		benchTable, err = newInMemoryBuilder(it)
		if err != nil {
			panic(err)
		}

		f := tmpTestfile()
		defer f.Close()
		if err := benchTable.Write(f); err != nil {
			panic(err)
		}
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
