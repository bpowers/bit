// Copyright 2021 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

// Package bit (Big Immutable Table) provides a way to build a constant hashmap/table,
// and then use that hashmap without reading/hydrating it into your program's heap.
package bit

import (
	"bytes"
	"errors"
	"fmt"
	"log"

	"github.com/bpowers/bit/internal/datafile"
	"github.com/bpowers/bit/internal/index"
)

// WithMMap configures the returned table to use either mmap or a readat(2) syscall
// to access the data table.  Mmap is significantly faster in the p50 case, but may
// result in worse tail latency under high load with large tables.
func WithMMap(enabled bool) func(opts *options) {
	return func(opts *options) {
		opts.withMmap = enabled
	}
}

type options struct {
	withMmap bool
}

// Option configures the table returned by New.
type Option func(*options)

// Table represents an on-disk big immutable table.  The contents of the table are `mmap(2)`'d
// into your process's address space, which (1) lets the OS manage keeping hot parts of the
// table in its LRU-based page cache, and (2) enables you to access big immutable tables whose
// contents are larger than you have RAM+swap for.  It is robust to corruption -- we validate
// that (a) keys passed to Get exactly match the key in the entry, and (b) the value's checksum
// matches one we stored at table build time.
type Table struct {
	data     *datafile.MmapReader
	slowData *datafile.OsFileReader
	idx      *index.Table
}

// New opens a bit table for reading, returning an error if things go wrong.
func New(dataPath string, opts ...Option) (*Table, error) {
	var options options
	for _, opt := range opts {
		opt(&options)
	}

	tbl := new(Table)

	var level0Count, level1Count uint64
	var indexBytes []byte

	if options.withMmap {
		mmapData, err := datafile.NewMMapReaderWithPath(dataPath)
		if err != nil {
			return nil, fmt.Errorf("datafile.NewMMapReaderAtPath(%s): %w", dataPath, err)
		}
		level0Count, level1Count, indexBytes = mmapData.Index()
		tbl.data = mmapData
	} else {
		fileData, err := datafile.NewOsFileReader(dataPath)
		if err != nil {
			return nil, fmt.Errorf("datafile.NewOsFileReader(%s): %w", dataPath, err)
		}
		level0Count, level1Count, indexBytes, err = fileData.Index()
		if err != nil {
			return nil, fmt.Errorf("datafile.Index: %w", err)
		}
		tbl.slowData = fileData
	}

	idx, err := index.NewTable(index.Built{
		Level0Len: level0Count,
		Level1Len: level1Count,
		Table:     indexBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("index.NewTable: %e", err)
	}
	tbl.idx = idx

	return tbl, nil
}

// GetString returns a value for the given string key, if it exists in the table.
// The value is a []byte, but MUST NOT be written to.
func (t *Table) GetString(key string) ([]byte, bool) {
	off := t.idx.MaybeLookupString(key)

	var expectedKey, value []byte
	var err error

	if t.slowData != nil {
		expectedKey, value, err = t.slowData.ReadAt(off)
	} else {
		expectedKey, value, err = t.data.ReadAt(off)
	}
	if err != nil {
		if errors.Is(err, datafile.InvalidOffset) {
			// TODO: remove this before deploying to prod probably
			log.Printf("bit.Table.GetString(%q): %s\n", key, err)
		}
		return nil, false
	}
	if string(expectedKey) != key {
		// this is expected: if we call GetString with a key that doesn't exist
		// we hit this
		return nil, false
	}
	return value, true
}

// Get returns a value for the given []byte key, if it exists in the table.
// The value is a []byte, but MUST NOT be written to.
func (t *Table) Get(key []byte) ([]byte, bool) {
	off := t.idx.MaybeLookup(key)

	var expectedKey, value []byte
	var err error

	if t.slowData != nil {
		expectedKey, value, err = t.slowData.ReadAt(off)
	} else {
		expectedKey, value, err = t.data.ReadAt(off)
	}
	if err != nil {
		if errors.Is(err, datafile.InvalidOffset) {
			// TODO: remove this before deploying to prod probably
			log.Printf("bit.Table.Get(%q): %s\n", key, err)
		}
		return nil, false
	}
	if !bytes.Equal(expectedKey, key) {
		// this is expected: if we call GetString with a key that doesn't exist
		// we hit this
		return nil, false
	}
	return value, true
}
