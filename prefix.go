/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package ristretto

import (
	"sync"

	"github.com/dgraph-io/ristretto/v2/z"
)

// hashPrefix reduces a prefix to the same 64-bit representation used for
// string keys. The plaintext prefix is discarded after this call.
func hashPrefix(prefix string) uint64 {
	prefixHash, _ := z.KeyToHash[string](prefix)
	return prefixHash
}

// prefixIndex is an inverted index between prefix hashes and the key hashes
// stored in the cache. It lets the cache invalidate every entry sharing a
// prefix without retaining any plaintext keys or prefixes: both sides of the
// index only hold uint64 hashes, the same material the cache already keeps.
//
// The index is safe for concurrent use. Every tracked key hash is associated
// with at most one prefix hash, mirroring the single entry a key occupies in
// the store, so removing a key reclaims its index slot as well.
type prefixIndex struct {
	mu sync.RWMutex
	// byPrefix maps a prefix hash to the set of key hashes written under it.
	byPrefix map[uint64]map[uint64]struct{}
	// keyPrefix maps a tracked key hash back to its prefix hash. It keeps the
	// index consistent when a key is deleted, evicted, expires, or is rewritten
	// under a different prefix.
	keyPrefix map[uint64]uint64
}

func newPrefixIndex() *prefixIndex {
	return &prefixIndex{
		byPrefix:  make(map[uint64]map[uint64]struct{}),
		keyPrefix: make(map[uint64]uint64),
	}
}

// add associates the key hash with the prefix hash. Re-associating a key with
// a different prefix removes it from the previous prefix's set.
func (idx *prefixIndex) add(prefixHash, keyHash uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if prev, ok := idx.keyPrefix[keyHash]; ok && prev == prefixHash {
		return
	} else if ok {
		idx.deleteFromPrefix(prev, keyHash)
	}

	keys, ok := idx.byPrefix[prefixHash]
	if !ok {
		keys = make(map[uint64]struct{})
		idx.byPrefix[prefixHash] = keys
	}
	keys[keyHash] = struct{}{}
	idx.keyPrefix[keyHash] = prefixHash
}

// removeKey drops every index reference to the key hash. It is a no-op for
// keys that were never written with a prefix.
func (idx *prefixIndex) removeKey(keyHash uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	prefixHash, ok := idx.keyPrefix[keyHash]
	if !ok {
		return
	}
	idx.deleteFromPrefix(prefixHash, keyHash)
	delete(idx.keyPrefix, keyHash)
}

// invalidate removes and returns the key hashes currently associated with the
// prefix hash. The cost is proportional to the number of matching entries,
// not to the total cache size, so invalidating a prefix never scans the whole
// cache.
func (idx *prefixIndex) invalidate(prefixHash uint64) []uint64 {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	keys := idx.byPrefix[prefixHash]
	if len(keys) == 0 {
		return nil
	}

	removed := make([]uint64, 0, len(keys))
	for keyHash := range keys {
		removed = append(removed, keyHash)
		delete(idx.keyPrefix, keyHash)
	}
	delete(idx.byPrefix, prefixHash)
	return removed
}

// clear drops every prefix association.
func (idx *prefixIndex) clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.byPrefix = make(map[uint64]map[uint64]struct{})
	idx.keyPrefix = make(map[uint64]uint64)
}

// count returns the number of key hashes tracked by the index.
func (idx *prefixIndex) count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.keyPrefix)
}

// deleteFromPrefix removes a key hash from a prefix set and drops the set when
// it becomes empty. The caller must hold idx.mu.
func (idx *prefixIndex) deleteFromPrefix(prefixHash, keyHash uint64) {
	keys := idx.byPrefix[prefixHash]
	delete(keys, keyHash)
	if len(keys) == 0 {
		delete(idx.byPrefix, prefixHash)
	}
}
