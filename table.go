// Copyright 2021 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package bit

import (
	"errors"
	"fmt"
	"os"

	"github.com/bpowers/bit/internal/dataio"
	"github.com/bpowers/bit/internal/index"
)

type Builder struct {
	dataFile     *os.File
	dioWriter    *dataio.Writer
	indexEntries []index.Entry
	indexKeys    stringSet
}

var (
	errorKeyTooBig    = errors.New("we only support keys < 256 bytes in length")
	errorDuplicateKey = errors.New("duplicate keys aren't supported")
)

func NewBuilder(dataFile *os.File) (*Builder, error) {
	w, err := dataio.NewWriter(dataFile)
	if err != nil {
		return nil, fmt.Errorf("dataio.NewWriter: %e", err)
	}
	return &Builder{
		dataFile:  dataFile,
		dioWriter: w,
		indexKeys: make(stringSet),
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
	dataPath := b.dataFile.Name()
	b.dataFile = nil

	idx := index.Build(b.indexEntries)
	idxFilePath := dataPath + ".idx"

	f, err := os.Create(idxFilePath)
	if err != nil {
		return nil, fmt.Errorf("os.Create(%s): %e", idxFilePath, err)
	}
	defer func() {
		_ = f.Sync()
		_ = f.Close()
	}()

	if err := idx.Write(f); err != nil {
		return nil, fmt.Errorf("idx.Write: %e", err)
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
	idx, err := index.NewFlatTable(dataPath + ".idx")
	if err != nil {
		return nil, fmt.Errorf("index.NewFlatTable: %e", err)
	}
	return &Table{
		data: r,
		idx:  idx,
	}, nil
}

func (t *Table) GetString(key string) ([]byte, bool) {
	off := t.idx.MaybeLookup(key)
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
