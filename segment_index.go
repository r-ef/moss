//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package moss

import (
	"bytes"
)

type segmentKeysIndex struct {
	// Number of keys that can be indexed.
	numIndexableKeys int

	// Keys that have been added so far.
	numKeys int

	// Start offsets of keys in the segment's buf.
	keyOffsets []uint32

	// Lengths of keys.
	keyLengths []uint32

	// Number of skips over keys in the segment kvs to arrive at the
	// next adjacent key in the keyOffsets array.
	hop int

	// Total number of keys in the source segment.
	srcKeyCount int

	// Reference to segment buf for key access.
	buf []byte
}

// newSegmentKeysIndex preallocates the keyOffsets array
// based on a calculated hop.
func newSegmentKeysIndex(quota int, srcKeyCount int,
	keyAvgSize int) *segmentKeysIndex {
	numIndexableKeys := quota / 8 /* 4 for offset + 4 for length */
	if numIndexableKeys == 0 {
		return nil
	}

	hop := (srcKeyCount / numIndexableKeys) + 1

	keyOffsets := make([]uint32, numIndexableKeys)
	keyLengths := make([]uint32, numIndexableKeys)

	return &segmentKeysIndex{
		numIndexableKeys: numIndexableKeys,
		numKeys:          0,
		keyOffsets:       keyOffsets,
		keyLengths:       keyLengths,
		hop:              hop,
		srcKeyCount:      srcKeyCount,
	}
}

// Adds a qualified entry to the index. Returns true if space
// still available, false otherwise.
func (s *segmentKeysIndex) add(keyIdx int, keyOffset uint32, keyLen uint32) bool {
	if s.numKeys >= s.numIndexableKeys {
		// All keys that can be indexed already have been,
		// return false indicating that there's no room for
		// anymore.
		return false
	}

	if keyIdx%(s.hop) != 0 {
		// Key does not satisfy the hop condition.
		return true
	}

	s.keyOffsets[s.numKeys] = keyOffset
	s.keyLengths[s.numKeys] = keyLen
	s.numKeys++

	return true
}

// Fetches the range of offsets between which the key exists,
// if present at all. The returned leftPos and rightPos can
// directly be used as the left and right extreme cursors
// while binary searching over the source segment.
func (s *segmentKeysIndex) lookup(key []byte) (leftPos int, rightPos int) {
	i, j := 0, s.numKeys

	if i == j || s.numKeys < 2 {
		// The index either wasn't used or isn't of any use.
		rightPos = s.srcKeyCount
		return
	}

	// If key smaller than the first key, return early.
	keyStart := s.keyOffsets[0]
	keyLen := s.keyLengths[0]
	indexedKey := s.buf[keyStart : keyStart+keyLen]
	cmp := bytes.Compare(key, indexedKey)
	if cmp < 0 {
		return
	}

	indexOfLastKey := s.numKeys - 1

	// If key larger than last key, return early.
	keyStart = s.keyOffsets[indexOfLastKey]
	keyLen = s.keyLengths[indexOfLastKey]
	indexedKey = s.buf[keyStart : keyStart+keyLen]
	cmp = bytes.Compare(indexedKey, key)
	if cmp < 0 {
		leftPos = (indexOfLastKey) * s.hop
		rightPos = s.srcKeyCount
		return
	}

	for i < j {
		h := i + (j-i)/2

		keyStart = s.keyOffsets[h]
		keyLen = s.keyLengths[h]
		indexedKey = s.buf[keyStart : keyStart+keyLen]

		cmp = bytes.Compare(indexedKey, key)
		if cmp == 0 {
			leftPos = h * s.hop
			rightPos = leftPos + 1
			return // Direct hit.
		} else if cmp < 0 {
			if i == h {
				break
			}
			i = h
		} else {
			j = h
		}
	}

	// The key is between i and j.
	leftPos = i * s.hop
	rightPos = j * s.hop
	return
}
