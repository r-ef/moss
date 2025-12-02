//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build !safe
// +build !safe

package moss

import (
	"unsafe"
)

// Uint64SliceToByteSlice gives access to []uint64 as []byte.
func Uint64SliceToByteSlice(in []uint64) ([]byte, error) {
	if len(in) == 0 {
		return nil, nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&in[0])), len(in)*8), nil
}

// ByteSliceToUint64Slice gives access to []byte as []uint64.
func ByteSliceToUint64Slice(in []byte) ([]uint64, error) {
	if len(in) == 0 {
		return nil, nil
	}
	return unsafe.Slice((*uint64)(unsafe.Pointer(&in[0])), len(in)/8), nil
}

// --------------------------------------------------------------

func endian() string { // See golang-nuts / how-to-tell-endian-ness-of-machine,
	var x uint32 = 0x01020304
	if *(*byte)(unsafe.Pointer(&x)) == 0x01 {
		return "big"
	}
	return "little"
}
