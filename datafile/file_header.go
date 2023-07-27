// Copyright 2023 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package datafile

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	magicDataHeader   = 0xC0FFEE3D
	fileFormatVersion = 4
	fileHeaderSize    = 128
)

// fileHeader contains metadata about a bit file.  On disk it looks like:
//
//	 0    1    2    3    4    5    6    7    8    9    A    B    C    D    E    F
//	+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
//	| magic             | format version    | record count                          |
//	+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
//	| file ID...                                                                    |
//	+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
//	| file ID...                                                                    |
//	+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
//	| [RESERVED]                                                                    |
//	+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
//	| index start offset                    | index level 0 count                   |
//	+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
//	| index level 1 count                   | [RESERVED]...                         |
//	+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
//	| [RESERVED]...                                                                 |
//	+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
//	| [RESERVED]...                                                                 |
//	+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
//	| [RESERVED]...                                                                 |
//	+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
type fileHeader struct {
	magic            uint32
	formatVersion    uint32
	recordCount      uint64
	fileID           [32]byte
	indexStart       uint64
	indexLevel0Count uint64
	indexLevel1Count uint64
}

func newFileHeader() (*fileHeader, error) {
	h := &fileHeader{
		magic:         magicDataHeader,
		formatVersion: fileFormatVersion,
	}

	_, err := rand.Read(h.fileID[:])
	if err != nil {
		return nil, fmt.Errorf("crypto/rand.Read: %w", err)
	}

	return h, nil
}

func (h *fileHeader) MarshalTo(headerBytes []byte) (err error) {
	if len(headerBytes) < fileHeaderSize {
		return fmt.Errorf("headerBytes too short: %d < %d", headerBytes, fileHeaderSize)
	}

	headerBytes = headerBytes[:fileHeaderSize]

	binary.LittleEndian.PutUint32(headerBytes[0:], h.magic)
	binary.LittleEndian.PutUint32(headerBytes[4:], h.formatVersion)
	binary.LittleEndian.PutUint64(headerBytes[8:], h.recordCount)
	copy(headerBytes[16:48], h.fileID[:])

	binary.LittleEndian.PutUint64(headerBytes[64:], h.indexStart)
	binary.LittleEndian.PutUint64(headerBytes[72:], h.indexLevel0Count)
	binary.LittleEndian.PutUint64(headerBytes[80:], h.indexLevel1Count)

	return nil
}

func (h *fileHeader) WriteTo(w io.Writer) (n int64, err error) {
	var headerBuf [fileHeaderSize]byte
	if err := h.MarshalTo(headerBuf[:]); err != nil {
		return 0, fmt.Errorf("marshalTo: %w", err)
	}

	if _, err = w.Write(headerBuf[:]); err != nil {
		return 0, fmt.Errorf("write: %w", err)
	}
	return int64(fileHeaderSize), nil
}

func (h *fileHeader) UpdateRecordCount(n uint64, w io.WriterAt) error {
	h.recordCount = n

	var headerBuf [fileHeaderSize]byte
	if err := h.MarshalTo(headerBuf[:]); err != nil {
		return fmt.Errorf("marshalTo: %w", err)
	}

	if _, err := w.WriteAt(headerBuf[:], 0); err != nil {
		return fmt.Errorf("f.WriteAt: %w", err)
	}

	return nil
}

func (h *fileHeader) UnmarshalBytes(headerBytes []byte) error {
	if len(headerBytes) < fileHeaderSize {
		return fmt.Errorf("headerBytes too short: %d < %d", headerBytes, fileHeaderSize)
	}

	headerBytes = headerBytes[:fileHeaderSize]

	h.magic = binary.LittleEndian.Uint32(headerBytes[0:])
	h.formatVersion = binary.LittleEndian.Uint32(headerBytes[4:])
	h.recordCount = binary.LittleEndian.Uint64(headerBytes[8:])
	copy(h.fileID[:], headerBytes[16:48])

	h.indexStart = binary.LittleEndian.Uint64(headerBytes[64:])
	h.indexLevel0Count = binary.LittleEndian.Uint64(headerBytes[72:])
	h.indexLevel1Count = binary.LittleEndian.Uint64(headerBytes[80:])

	if h.magic != magicDataHeader {
		return fmt.Errorf("bad magic number on data file (%x) -- not bit datafile or corrupted", h.magic)
	}

	if h.formatVersion != fileFormatVersion {
		return fmt.Errorf("this version of the bit library can only read v%d data files; found v%d", fileFormatVersion, h.formatVersion)
	}

	return nil
}
