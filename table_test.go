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

	benchHashmap = make(map[string]string)
	benchEntries = make([]benchEntry, 0, len(expected))
	for k, v := range expected {
		benchEntries = append(benchEntries, benchEntry{Key: k, Value: v})
		keyBuf = make([]byte, len(k))
		copy(keyBuf, k)
		valueBuf = make([]byte, len(v))
		copy(valueBuf, v)
		// attempt to ensure the hashmap doesn't share memory with our test oracle
		benchHashmap[string(keyBuf)] = string(valueBuf)
	}
}

func TestSplit2(t *testing.T) {
	sep := byte(',')
	for _, testcase := range []string{
		"",
		"a,b",
		",a,b,",
		"a,b,",
	} {
		input := []byte(testcase)
		expected := bytes.SplitN(input, []byte{sep}, 2)
		var actualL, actualR []byte
		var ok bool
		allocs := testing.AllocsPerRun(1, func() {
			actualL, actualR, ok = split2(input, sep)
		})
		require.Zero(t, allocs)
		require.True(t, len(expected) <= 2)
		if len(expected) < 2 {
			require.False(t, ok)
		} else {
			expectedL := expected[0]
			expectedR := expected[1]
			require.Equal(t, expectedL, actualL)
			require.Equal(t, expectedR, actualR)
		}
	}
}

func openTestFile(path string) (*Table, map[string]string, error) {
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
		_ = os.Remove(dataFile.Name() + ".index")
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
		k, v, ok := split2(line, ':')
		if !ok {
			panic("input file unexpected shape")
		}
		err := builder.Put(k, v)
		if err != nil {
			return nil, nil, err
		}
		known[string(k)] = string(v)
	}

	table, err := builder.Finalize()
	if err != nil {
		return nil, nil, err
	}
	return table, known, nil
}

func testFile(t testing.TB, path string) {
	table, known, err := openTestFile(path)
	require.NoError(t, err)

	for k, expected := range known {
		v, ok := table.GetString(k)
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
	}
}

func TestTableSmall(t *testing.T) {
	testFile(t, "testdata.small")
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
