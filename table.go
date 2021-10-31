// Copyright 2021 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package bit

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bpowers/bit/internal/dataio"
	"github.com/bpowers/bit/internal/index"
)

type Builder struct {
	resultPath   string
	dataFile     *os.File
	dioWriter    *dataio.Writer
	indexEntries []index.Entry
	indexKeys    stringSet
}

var (
	errorKeyTooBig    = errors.New("we only support keys < 256 bytes in length")
	errorDuplicateKey = errors.New("duplicate keys aren't supported")
)

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
	w, err := dataio.NewWriter(dataFile)
	if err != nil {
		return nil, fmt.Errorf("dataio.NewWriter: %e", err)
	}
	return &Builder{
		resultPath: dataFilePath,
		dataFile:   dataFile,
		dioWriter:  w,
		indexKeys:  make(stringSet),
	}, nil
}

func (b *Builder) Put(k, v []byte) error {
	kLen := len(k)
	if kLen >= 256 {
		return errorKeyTooBig
	}
	// copy the key (by allocating a string), because it could point into
	// e.g. a bufio buffer
	key := string(k)
	if b.indexKeys.Contains(key) {
		return errorDuplicateKey
	}
	off, err := b.dioWriter.Write(k, v)
	b.indexEntries = append(b.indexEntries, index.Entry{Key: key, Offset: off})
	if err != nil {
		return err
	}
	return nil
}

func (b *Builder) Finalize() (*Table, error) {
	// we're done with this -- nil it so it can be GC'd earlier
	b.indexKeys = nil
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

	idx := index.Build(b.indexEntries)
	finalIndexPath := dataPath + ".index"
	f, err := os.CreateTemp(filepath.Dir(dataPath), "bit-builder.*.index")
	if err != nil {
		return nil, fmt.Errorf("os.CreateTemp: %e", err)
	}

	if err := idx.Write(f); err != nil {
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

type Table struct {
	data *dataio.Reader
	idx  *index.FlatTable
}

func New(dataPath string) (*Table, error) {
	r, err := dataio.NewMMapReaderWithPath(dataPath)
	if err != nil {
		return nil, fmt.Errorf("dataio.NewMMapReaderWithPath(%s): %e", dataPath, err)
	}
	idx, err := index.NewFlatTable(dataPath + ".index")
	if err != nil {
		return nil, fmt.Errorf("index.NewFlatTable: %e", err)
	}
	return &Table{
		data: r,
		idx:  idx,
	}, nil
}

func (t *Table) GetString(key string) ([]byte, bool) {
	off := t.idx.MaybeLookupString(key)
	expectedKey, value, err := t.data.Read(off)
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

func (t *Table) Get(key []byte) ([]byte, bool) {
	off := t.idx.MaybeLookup(key)
	expectedKey, value, err := t.data.Read(off)
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
