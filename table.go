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
	"os"
	"path/filepath"

	"github.com/bpowers/bit/datafile"
	"github.com/bpowers/bit/indexfile"
)

// Builder is used to construct a big immutable table from key/value pairs.
type Builder struct {
	resultPath string
	dataFile   *os.File
	dioWriter  *datafile.Writer
}

var (
	errorKeyTooBig = errors.New("we only support keys < 256 bytes in length")
)

// NewBuilder creates a Builder that can be used to construct a Table.  Building should happen
// once, and the table
func NewBuilder(dataFilePath string) (*Builder, error) {
	// we want to write to a new file and do an atomic rename when we're done on disk
	dataFilePath, err := filepath.Abs(dataFilePath)
	if err != nil {
		return nil, fmt.Errorf("filepath.Abs: %e", err)
	}
	dir := filepath.Dir(dataFilePath)
	dataFile, err := os.CreateTemp(dir, "bit-builder.*.data")
	if err != nil {
		return nil, fmt.Errorf("CreateTemp failed (may need permissions for dir containing dataFile): %e", err)
	}
	w, err := datafile.NewWriter(dataFile)
	if err != nil {
		return nil, fmt.Errorf("datafile.NewWriter: %e", err)
	}
	return &Builder{
		resultPath: dataFilePath,
		dataFile:   dataFile,
		dioWriter:  w,
	}, nil
}

// Put adds a key/value pair to the table.  Duplicate keys result in an error at Finalize time.
func (b *Builder) Put(k, v []byte) error {
	kLen := len(k)
	if kLen >= 256 {
		return errorKeyTooBig
	}
	_, err := b.dioWriter.Write(k, v)
	if err != nil {
		return err
	}
	return nil
}

func (b *Builder) Finalize() (*Table, error) {
	return b.finalize(indexfile.FastHighMem)
}

func (b *Builder) FinalizeLowMem() (*Table, error) {
	return b.finalize(indexfile.SlowLowMem)
}

// Finalize flushes the table to disk and builds an index to efficiently randomly access entries.
func (b *Builder) finalize(indexBuildType indexfile.BuildType) (*Table, error) {
	if err := b.dioWriter.Close(); err != nil {
		return nil, fmt.Errorf("recordio.Close: %e", err)
	}
	// make the file read-only
	if err := os.Chmod(b.dataFile.Name(), 0444); err != nil {
		return nil, fmt.Errorf("os.Chmod(0444): %e", err)
	}
	if err := os.Rename(b.dataFile.Name(), b.resultPath); err != nil {
		return nil, fmt.Errorf("os.Rename: %e", err)
	}
	// make the file read-only
	if err := os.Chmod(b.resultPath, 0444); err != nil {
		return nil, fmt.Errorf("os.Chmod(0444): %e", err)
	}
	b.dataFile = nil
	dataPath := b.resultPath

	r, err := datafile.NewMMapReaderWithPath(dataPath)
	if err != nil {
		return nil, fmt.Errorf("datafile.NewMMapReaderWithPath(%s): %e", dataPath, err)
	}

	finalIndexPath := dataPath + ".index"
	f, err := os.CreateTemp(filepath.Dir(dataPath), "bit-builder.*.index")

	it := r.Iter()
	defer it.Close()
	if err := indexfile.Build(f, it, indexBuildType); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, fmt.Errorf("idx.Write: %e", err)
	}

	if err = f.Sync(); err != nil {
		return nil, err
	}
	if err = f.Close(); err != nil {
		return nil, err
	}

	if err = os.Chmod(f.Name(), 0444); err != nil {
		return nil, fmt.Errorf("os.Chmod(0444): %e", err)
	}
	if err = os.Rename(f.Name(), finalIndexPath); err != nil {
		return nil, fmt.Errorf("os.Rename: %e", err)
	}
	if err = os.Chmod(finalIndexPath, 0444); err != nil {
		return nil, fmt.Errorf("os.Chmod(0444): %e", err)
	}

	return New(dataPath)
}

// Table represents an on-disk big immutable table.  The contents of the table are `mmap(2)`'d
// into your process's address space, which (1) lets the OS manage keeping hot parts of the
// table in its LRU-based page cache, and (2) enables you to access big immutable tables whose
// contents are larger than you have RAM+swap for.  It is robust to corruption -- we validate
// that (a) keys passed to Get exactly match the key in the entry, and (b) the value's checksum
// matches one we stored at table build time.
type Table struct {
	data *datafile.Reader
	idx  *indexfile.Table
}

// New opens a bit table for reading, returning an error if things go wrong.
func New(dataPath string) (*Table, error) {
	r, err := datafile.NewMMapReaderWithPath(dataPath)
	if err != nil {
		return nil, fmt.Errorf("datafile.NewMMapReaderWithPath(%s): %e", dataPath, err)
	}
	idx, err := indexfile.NewTable(dataPath + ".index")
	if err != nil {
		return nil, fmt.Errorf("indexfile.NewTable: %e", err)
	}
	return &Table{
		data: r,
		idx:  idx,
	}, nil
}

// GetString returns a value for the given string key, if it exists in the table.
// The value is a []byte, but MUST NOT be written to.
func (t *Table) GetString(key string) ([]byte, bool) {
	off := t.idx.MaybeLookupString(key)
	expectedKey, value, err := t.data.ReadAt(int64(off))
	if err != nil {
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
	expectedKey, value, err := t.data.ReadAt(int64(off))
	if err != nil {
		return nil, false
	}
	if !bytes.Equal(expectedKey, key) {
		// this is expected: if we call GetString with a key that doesn't exist
		// we hit this
		return nil, false
	}
	return value, true
}
