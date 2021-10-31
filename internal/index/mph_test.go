// Copyright 2021 The bit Authors and Caleb Spare. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package index

import (
	"bufio"
	"os"
	"strconv"
	"sync"
	"testing"
)

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
	entries := make([]Entry, len(keys))
	for i, key := range keys {
		entries[i] = Entry{Key: key, Offset: uint64(i)}
	}
	table := Build(entries)
	for i, key := range keys {
		n := table.MaybeLookup(key)
		if int(n) != i {
			t.Errorf("Lookup(%s): got n=%d; want %d", key, n, i)
		}
	}
}

var (
	words          []Entry
	wordsOnce      sync.Once
	benchTable     *Table
	benchFlatTable *FlatTable
)

func BenchmarkBuild(b *testing.B) {
	wordsOnce.Do(loadBenchTable)
	if len(words) == 0 {
		b.Skip("unable to load dictionary file")
	}
	for i := 0; i < b.N; i++ {
		Build(words)
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
		n := benchTable.MaybeLookup(string(words[j].Key))
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
		n := benchFlatTable.MaybeLookup(string(words[j].Key))
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
		benchTable = Build(words)
		f, err := os.CreateTemp("", "bit-test.*.idx")
		if err != nil {
			panic(err)
		}
		defer func() {
			_ = f.Close()
		}()
		if err := benchTable.Write(f); err != nil {
			panic(err)
		}
		benchFlatTable, err = NewFlatTable(f.Name())
		if err != nil {
			panic(err)
		}
	}
}

func loadDict(dict string) ([]Entry, error) {
	f, err := os.Open(dict)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	var words []Entry
	i := uint64(0)
	for scanner.Scan() {
		words = append(words, Entry{Key: string(scanner.Bytes()), Offset: i})
		i++
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return words, nil
}
