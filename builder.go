// Copyright 2021 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package bit

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/bpowers/bit/internal/datafile"
	"github.com/bpowers/bit/internal/index"
)

var (
	errKeyTooBig = errors.New("we only support keys < 256 bytes in length")
)

// BuilderOption configures the Builder.
type BuilderOption func(*builderOptions)

type builderOptions struct {
	logger *slog.Logger
}

// WithBuilderLogger sets an optional logger for the builder to use for progress updates.
// If not provided, no logging output will be produced.
func WithBuilderLogger(logger *slog.Logger) BuilderOption {
	return func(opts *builderOptions) {
		opts.logger = logger
	}
}

// Builder is used to construct a big immutable table from key/value pairs.
type Builder struct {
	resultPath string
	dataFile   *os.File
	dioWriter  *datafile.Writer
	logger     *slog.Logger
}

// NewBuilder creates a Builder that can be used to construct a Table.  Building should happen
// once, and the table
func NewBuilder(dataFilePath string, opts ...BuilderOption) (*Builder, error) {
	var options builderOptions
	options.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, opt := range opts {
		opt(&options)
	}
	// we want to write to a new file and do an atomic rename when we're done on disk
	dataFilePath, err := filepath.Abs(dataFilePath)
	if err != nil {
		return nil, fmt.Errorf("filepath.Abs: %w", err)
	}
	dir := filepath.Dir(dataFilePath)
	dataFile, err := os.CreateTemp(dir, "bit-builder.*.data")
	if err != nil {
		return nil, fmt.Errorf("CreateTemp failed (may need permissions for dir %q containing dataFile): %w", dir, err)
	}
	w, err := datafile.NewWriter(dataFile)
	if err != nil {
		return nil, fmt.Errorf("datafile.NewWriter: %w", err)
	}
	return &Builder{
		resultPath: dataFilePath,
		dataFile:   dataFile,
		dioWriter:  w,
		logger:     options.logger,
	}, nil
}

// Put adds a key/value pair to the table.  Duplicate keys result in an error at Finalize time.
func (b *Builder) Put(k, v []byte) error {
	kLen := len(k)
	if kLen > datafile.MaxKeyLen {
		return errKeyTooBig
	}
	_, err := b.dioWriter.Write(k, v)
	if err != nil {
		return err
	}
	return nil
}

func (b *Builder) Finalize() error {
	return b.finalize()
}

// Finalize flushes the table to disk and builds an index to efficiently randomly access entries.
func (b *Builder) finalize() error {
	if err := b.dioWriter.Finish(); err != nil {
		return fmt.Errorf("recordio.Close: %w", err)
	}

	if err := appendIndexFor(b.dataFile, b.dioWriter, b.logger); err != nil {
		return fmt.Errorf("appendIndexFor: %w\n", err)
	}

	// make the file read-only
	if err := os.Chmod(b.dataFile.Name(), 0444); err != nil {
		return fmt.Errorf("os.Chmod(0444): %w", err)
	}
	if err := os.Rename(b.dataFile.Name(), b.resultPath); err != nil {
		return fmt.Errorf("os.Rename: %w", err)
	}
	// make the file read-only
	if err := os.Chmod(b.resultPath, 0444); err != nil {
		return fmt.Errorf("os.Chmod(0444): %w", err)
	}
	_ = b.dataFile.Close()
	b.dataFile = nil

	return nil
}

func appendIndexFor(f *os.File, dioWriter *datafile.Writer, logger *slog.Logger) error {
	dataPath := f.Name()
	r, err := datafile.NewMMapReaderWithPath(dataPath)
	if err != nil {
		return fmt.Errorf("datafile.NewMMapReaderAtPath(%s): %w", dataPath, err)
	}

	it := r.Iter()
	defer it.Close()
	if built, err := index.Build(it, logger); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return fmt.Errorf("idx.Build: %w", err)
	} else {
		if err := dioWriter.SetIndexMetadata(built.Level0Len, built.Level1Len); err != nil {
			return fmt.Errorf("dioWriter.SetIndexMetadata: %w", err)
		}
		if n, err := f.Write(built.Table); err != nil {
			return fmt.Errorf("idx.Write: %w", err)
		} else if n != len(built.Table) {
			return fmt.Errorf("idx.Write: short write of %d (wanted %d)", n, len(built.Table))
		}
	}

	return nil
}
