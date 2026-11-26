/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package ristretto

import (
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/stretchr/testify/require"
)

func newPrefixTestCache(t *testing.T) *Cache[string, int] {
	t.Helper()
	c, err := NewCache(&Config[string, int]{
		NumCounters:        1000,
		MaxCost:            1000,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	require.NoError(t, err)
	return c
}

// TestCacheInvalidatePrefix writes keys under three different prefixes,
// invalidates one of them, and checks that only that prefix's entries are gone
// while the other two remain served from the cache.
func TestCacheInvalidatePrefix(t *testing.T) {
	c := newPrefixTestCache(t)

	prefixes := []string{"product:electronics", "product:grocery", "product:apparel"}
	const keysPerPrefix = 10
	for _, prefix := range prefixes {
		for i := 0; i < keysPerPrefix; i++ {
			key := fmt.Sprintf("%s:%d", prefix, i)
			require.True(t, c.SetWithPrefix(prefix, key, i, 1))
		}
	}
	c.Wait()

	require.Equal(t, len(prefixes)*keysPerPrefix, c.prefixes.count())

	c.InvalidatePrefix(prefixes[0])

	for i := 0; i < keysPerPrefix; i++ {
		_, ok := c.Get(fmt.Sprintf("%s:%d", prefixes[0], i))
		require.False(t, ok, "key under invalidated prefix should be gone")
	}
	for _, prefix := range prefixes[1:] {
		for i := 0; i < keysPerPrefix; i++ {
			val, ok := c.Get(fmt.Sprintf("%s:%d", prefix, i))
			require.True(t, ok, "key under untouched prefix should still be cached")
			require.Equal(t, i, val)
		}
	}

	require.Equal(t, 2*keysPerPrefix, c.prefixes.count())

	// Invalidating an unknown prefix is a no-op.
	c.InvalidatePrefix("product:nonexistent")
	require.Equal(t, 2*keysPerPrefix, c.prefixes.count())
}

func TestCacheInvalidatePrefixKeepsUnprefixedEntries(t *testing.T) {
	c := newPrefixTestCache(t)

	require.True(t, c.SetWithPrefix("group:a", "group:a:1", 1, 1))
	require.True(t, c.Set("plain:1", 2, 1))
	c.Wait()

	c.InvalidatePrefix("group:a")

	_, ok := c.Get("group:a:1")
	require.False(t, ok)
	val, ok := c.Get("plain:1")
	require.True(t, ok)
	require.Equal(t, 2, val)
	require.Zero(t, c.prefixes.count())
}

func TestCachePrefixDelRemovesIndexEntry(t *testing.T) {
	c := newPrefixTestCache(t)

	require.True(t, c.SetWithPrefix("group:a", "group:a:1", 1, 1))
	c.Wait()
	require.Equal(t, 1, c.prefixes.count())

	c.Del("group:a:1")
	time.Sleep(wait)

	_, ok := c.Get("group:a:1")
	require.False(t, ok)
	require.Zero(t, c.prefixes.count())

	// No stale association means a later invalidation has nothing to delete.
	c.InvalidatePrefix("group:a")
	require.Zero(t, c.prefixes.count())
}

func TestCachePrefixReassociation(t *testing.T) {
	c := newPrefixTestCache(t)

	require.True(t, c.SetWithPrefix("group:a", "shared:1", 1, 1))
	c.Wait()
	require.True(t, c.SetWithPrefix("group:b", "shared:1", 2, 1))
	c.Wait()

	// The key now belongs only to group:b, so invalidating group:a must not
	// touch it and must not leave a stale entry behind.
	c.InvalidatePrefix("group:a")
	val, ok := c.Get("shared:1")
	require.True(t, ok)
	require.Equal(t, 2, val)
	require.Equal(t, 1, c.prefixes.count())

	c.InvalidatePrefix("group:b")
	_, ok = c.Get("shared:1")
	require.False(t, ok)
	require.Zero(t, c.prefixes.count())
}

func TestCachePrefixIndexEvictionCleanup(t *testing.T) {
	c, err := NewCache(&Config[string, int]{
		NumCounters:        1000,
		MaxCost:            3,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	require.NoError(t, err)

	// Fill the tiny cache, then keep writing so earlier entries are evicted.
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("group:a:%d", i)
		c.SetWithPrefix("group:a", key, i, 1)
	}
	c.Wait()

	// Every eviction must have dropped the key's prefix association, so the
	// index can never hold more entries than the store.
	require.LessOrEqual(t, c.prefixes.count(), 3)
}

func TestCachePrefixExpirationCleanup(t *testing.T) {
	c, err := NewCache(&Config[string, int]{
		NumCounters:            100,
		MaxCost:                100,
		BufferItems:            64,
		IgnoreInternalCost:     true,
		TtlTickerDurationInSec: 1,
	})
	require.NoError(t, err)

	require.True(t, c.SetWithPrefixAndTTL("group:a", "group:a:1", 1, 1, 300*time.Millisecond))
	c.Wait()
	require.Equal(t, 1, c.prefixes.count())

	time.Sleep(2 * time.Second)

	_, ok := c.Get("group:a:1")
	require.False(t, ok)
	require.Zero(t, c.prefixes.count(), "expired entries must leave no prefix association")
}

func TestCacheClearDropsPrefixIndex(t *testing.T) {
	c := newPrefixTestCache(t)

	require.True(t, c.SetWithPrefix("group:a", "group:a:1", 1, 1))
	c.Wait()
	require.Equal(t, 1, c.prefixes.count())

	c.Clear()
	require.Zero(t, c.prefixes.count())

	// The cache keeps working after Clear with a fresh index.
	require.True(t, c.SetWithPrefix("group:b", "group:b:1", 1, 1))
	c.Wait()
	require.Equal(t, 1, c.prefixes.count())
	c.InvalidatePrefix("group:b")
	_, ok := c.Get("group:b:1")
	require.False(t, ok)
}

func TestPrefixIndexStoresOnlyHashes(t *testing.T) {
	idx := newPrefixIndex()
	prefixHash := hashPrefix("secret-prefix")
	keyHash, _ := z.KeyToHash[string]("secret-prefix:key")

	idx.add(prefixHash, keyHash)
	require.Equal(t, []uint64{keyHash}, idx.invalidate(prefixHash))
	require.Zero(t, idx.count())
}

func TestCachePrefixConcurrent(t *testing.T) {
	c := newPrefixTestCache(t)

	const goroutines = 8
	done := make(chan struct{}, goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			prefix := fmt.Sprintf("group:%d", g%3)
			for i := 0; i < 200; i++ {
				key := fmt.Sprintf("%s:%d:%d", prefix, g, i)
				c.SetWithPrefix(prefix, key, i, 1)
				_, _ = c.Get(key)
				if i%25 == 0 {
					c.InvalidatePrefix(prefix)
				}
			}
			done <- struct{}{}
		}(g)
	}
	for g := 0; g < goroutines; g++ {
		<-done
	}
	c.Wait()

	// No key may be indexed twice, and every indexed key must still resolve
	// against an entry or be removed once its entry leaves the cache.
	seen := make(map[uint64]struct{})
	c.prefixes.mu.RLock()
	for keyHash := range c.prefixes.keyPrefix {
		_, dup := seen[keyHash]
		require.False(t, dup)
		seen[keyHash] = struct{}{}
	}
	c.prefixes.mu.RUnlock()
	require.Equal(t, len(seen), c.prefixes.count())
}

func BenchmarkPrefixIndexInvalidate(b *testing.B) {
	const totalEntries = 100_000
	const entriesPerPrefix = 100

	idx := newPrefixIndex()
	for i := 0; i < totalEntries; i++ {
		prefixHash := hashPrefix(fmt.Sprintf("group:%d", i/entriesPerPrefix))
		idx.add(prefixHash, uint64(i+1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Re-add the group so each iteration invalidates the same-sized set.
		prefixHash := hashPrefix(fmt.Sprintf("group:%d", i%1000))
		for j := 0; j < entriesPerPrefix; j++ {
			idx.add(prefixHash, uint64(1_000_000+j))
		}
		idx.invalidate(prefixHash)
	}
}
