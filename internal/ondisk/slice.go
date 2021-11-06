// Copyright 2021 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

package ondisk

import (
	"encoding/binary"
	"fmt"
	"os"
)

type Uint32Array struct {
	f       *os.File
	elemLen int64 // length in number of elements
	byteOff int64 // offset in bytes of the start of this slice
}

func NewUint32Array(f *os.File, len, fileOff int64) *Uint32Array {
	return &Uint32Array{
		f:       f,
		elemLen: len,
		byteOff: fileOff,
	}
}

func (s *Uint32Array) Set(i int64, value uint32) error {
	if i < 0 || i >= s.elemLen {
		return fmt.Errorf("offset (%d) out of range (lenen %d)", i, s.elemLen)
	}
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], value)
	_, err := s.f.WriteAt(buf[:], s.byteOff+4*i)
	return err
}

func (s *Uint32Array) Get(i int64) (uint32, error) {
	if i < 0 || i >= s.elemLen {
		return 0, fmt.Errorf("offset (%d) out of range (len %d)", i, s.elemLen)
	}
	var buf [4]byte
	_, err := s.f.ReadAt(buf[:], s.byteOff+4*i)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf[:]), nil
}

type Uint64Array struct {
	f       *os.File
	elemLen int64 // length in number of elements
	byteOff int64 // offset in bytes of the start of this slice
}

func NewUint64Array(f *os.File, len int64, fileOff int64) *Uint64Array {
	return &Uint64Array{
		f:       f,
		elemLen: len,
		byteOff: fileOff,
	}
}

func (s *Uint64Array) Set(i int64, value uint64) error {
	if i < 0 || i >= s.elemLen {
		return fmt.Errorf("offset (%d) out of range (elemLen %d)", i, s.elemLen)
	}
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	_, err := s.f.WriteAt(buf[:], s.byteOff+8*i)
	return err
}

func (s *Uint64Array) Get(i int64) (uint64, error) {
	if i < 0 || i >= s.elemLen {
		return 0, fmt.Errorf("offset (%d) out of range (elemLen %d)", i, s.elemLen)
	}
	var buf [8]byte
	_, err := s.f.ReadAt(buf[:], s.byteOff+8*i)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf[:]), nil
}
