// Copyright 2023 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package datafile

import (
	"encoding/binary"
	"fmt"
	"io"
)

type fileHeader struct {
	magic         uint32
	formatVersion uint32
	recordCount   uint64
}

func newFileHeader() *fileHeader {
	return &fileHeader{
		magic:         magicDataHeader,
		formatVersion: fileFormatVersion,
	}
}

func (h *fileHeader) WriteTo(w io.Writer) (n int64, err error) {
	// make the header the minimum cache-width we expect to see
	var headerBuf [fileHeaderSize]byte
	binary.LittleEndian.PutUint32(headerBuf[:4], h.magic)
	// current file format version
	binary.LittleEndian.PutUint32(headerBuf[4:8], h.formatVersion)

	if _, err = w.Write(headerBuf[:]); err != nil {
		return 0, fmt.Errorf("write: %w", err)
	}
	return int64(fileHeaderSize), nil
}

func (h *fileHeader) UpdateRecordCount(n uint64, w io.WriterAt) error {
	h.recordCount = n

	var recordCountBuf [8]byte
	binary.LittleEndian.PutUint64(recordCountBuf[:], h.recordCount)
	if _, err := w.WriteAt(recordCountBuf[:], 8); err != nil {
		return fmt.Errorf("f.WriteAt: %w", err)
	}

	return nil
}

func (h *fileHeader) UnmarshalBytes(headerBytes []byte) error {
	if len(headerBytes) < fileHeaderSize {
		return fmt.Errorf("headerBytes too short: %d < %d", headerBytes, fileHeaderSize)
	}

	headerBytes = headerBytes[:fileHeaderSize]

	h.magic = binary.LittleEndian.Uint32(headerBytes[:4])
	if h.magic != magicDataHeader {
		return fmt.Errorf("bad magic number on data file (%x) -- not bit datafile or corrupted", h.magic)
	}

	h.formatVersion = binary.LittleEndian.Uint32(headerBytes[4:8])
	if h.formatVersion != fileFormatVersion {
		return fmt.Errorf("this version of the bit library can only read v%d data files; found v%d", fileFormatVersion, h.formatVersion)
	}

	h.recordCount = binary.LittleEndian.Uint64(headerBytes[8:16])

	return nil
}
