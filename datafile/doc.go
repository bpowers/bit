// Copyright 2023 The bit Authors. All rights reserved.
// Use of this source code is governed by the MIT License
// that can be found in the LICENSE file.

// Package datafile contains structures for building and reading
// static mappings from keys to data that may be too big to fit
// in memory or too expensive to compute on each process start.
//
// A datafile generally looks like:
//
//	┌───────────────────┐
//	│ file header       │
//	├───────────────────┤
//	│ repeated KV pairs │
//	│                   │
//	│                   │
//	│                   │
//	│                   │
//	│                   │
//	│                   │
//	├───────────────────┤
//	│ padding           │
//	├───────────────────┤
//	│ minimal perfect   │
//	│ hashmap           │
//	│                   │
//	└───────────────────┘
//
// Individual KV pairs stat with a fixed 7-byte header and are variable length,
// and look like:
//
//	 0    1    2    3    4    5    6    7
//	+----+----+----+----+----+----+----+----+
//	| value checksum    |klen| vlen    |key.|
//	+----+----+----+----+----+----+----+----+
//	| key...       | value...               |
//	+----+----+----+----+----+----+----+----+
//	| value...                              |
//	+----+----+----+----+----+----+----+----+
//
// This gives us a 255-byte max length for keys, and a 65-KB max length for values.
// The checksum is calculated from the bytes of the value, and is used to ensure we
// don't have un-detected on-disk corruption (with high probability).
package datafile
