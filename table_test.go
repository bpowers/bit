// Copyright 2021 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package bit

import (
	"bufio"
	"bytes"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	benchTable     *Table
	benchTableMmap *Table
	benchTableOnce sync.Once
	benchHashmap   map[string]string
	benchEntries   []benchEntry
)

type benchEntry struct {
	Key   string
	Value string
}

var keyBuf []byte
var valueBuf []byte

func loadBenchTable() {
	var err error
	var expected map[string]string
	benchTable, expected, err = openTestFile("testdata.large")
	if err != nil {
		panic(err)
	}
	benchTableMmap, expected, err = openTestFile("testdata.large", WithMMap(true))
	if err != nil {
		panic(err)
	}

	for k, v := range expected {
		benchEntries = append(benchEntries, benchEntry{Key: k, Value: v})
	}

	benchHashmap = make(map[string]string)
	for _, entry := range benchEntries {
		keyBuf = make([]byte, len(entry.Key))
		copy(keyBuf, entry.Key)
		valueBuf = make([]byte, len(entry.Value))
		copy(valueBuf, entry.Value)
		// attempt to ensure the hashmap doesn't share memory with our test oracle
		benchHashmap[string(keyBuf)] = string(valueBuf)
	}
}

func openTestFile(path string, opts ...Option) (*Table, map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = f.Close()
	}()

	dataFile, err := os.CreateTemp("", "bit-test.*.data")
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		_ = os.Remove(dataFile.Name())
	}()
	if err = dataFile.Close(); err != nil {
		return nil, nil, err
	}
	if err = os.Remove(dataFile.Name()); err != nil {
		return nil, nil, err
	}

	builder, err := NewBuilder(dataFile.Name())
	if err != nil {
		return nil, nil, err
	}

	known := make(map[string]string)

	s := bufio.NewScanner(bufio.NewReaderSize(f, 16*1024))
	for s.Scan() {
		line := s.Bytes()
		k, v, ok := bytes.Cut(line, []byte{':'})
		if !ok {
			panic("input file unexpected shape")
		}
		err := builder.Put(k, v)
		if err != nil {
			return nil, nil, err
		}
		known[string(k)] = string(v)
	}

	if err := builder.Finalize(); err != nil {
		return nil, nil, err
	}

	table, err := New(dataFile.Name(), opts...)
	if err != nil {
		return nil, nil, err
	}

	return table, known, nil
}

func testFile(t testing.TB, path string, opts ...Option) {
	table, known, err := openTestFile(path, opts...)
	require.NoError(t, err)

	for k, expected := range known {
		v, ok := table.GetString(k)
		require.True(t, ok)
		require.Equal(t, expected, string(v))
		v, ok = table.Get([]byte(k))
		require.True(t, ok)
		require.Equal(t, expected, string(v))
	}

	for _, negative := range []string{
		"", "doesn't exist",
	} {
		// we shouldn't find keys that don't exist
		v, ok := table.GetString(negative)
		require.False(t, ok)
		require.Nil(t, v)
		v, ok = table.Get([]byte(negative))
		require.False(t, ok)
		require.Nil(t, v)
	}
}

func TestTableSmall(t *testing.T) {
	testFile(t, "testdata.small")
}

func TestTableSmallWithMmap(t *testing.T) {
	testFile(t, "testdata.small", WithMMap(true))
}

func TestTableLarge(t *testing.T) {
	dataFile := "testdata.large"
	if _, err := os.Stat(dataFile); err != nil {
		t.Skip("testdata.large doesn't exist, skipping large test")
		return
	}
	testFile(t, dataFile)
}

func BenchmarkTable(b *testing.B) {
	benchTableOnce.Do(loadBenchTable)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := i % len(benchEntries)
		entry := benchEntries[j]
		value, ok := benchTable.GetString(entry.Key)
		if !ok || string(value) != entry.Value {
			b.Fatal("bad data or lookup")
		}
	}
}

func BenchmarkTableWithMMap(b *testing.B) {
	benchTableOnce.Do(loadBenchTable)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := i % len(benchEntries)
		entry := benchEntries[j]
		value, ok := benchTableMmap.GetString(entry.Key)
		if !ok || string(value) != entry.Value {
			b.Fatal("bad data or lookup")
		}
	}
}

func BenchmarkHashmap(b *testing.B) {
	benchTableOnce.Do(loadBenchTable)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := i % len(benchEntries)
		entry := benchEntries[j]
		value, ok := benchHashmap[entry.Key]
		if !ok || value != entry.Value {
			b.Fatal("bad data or lookup")
		}
	}
}

func init() {
}
