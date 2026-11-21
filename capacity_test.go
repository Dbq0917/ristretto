/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package ristretto

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// measureCacheGrowth fills a cache and returns the number of entries actually
// stored, the cost tracked by the eviction policy (the capacity estimate) and
// the real heap growth observed via runtime.MemStats.
func measureCacheGrowth(t *testing.T, n int, payload int64) (stored, estimatedCost, realHeap int64) {
	t.Helper()

	c, err := NewCache(&Config[uint64, []byte]{
		NumCounters: int64(10 * n),
		// Effectively unbounded so nothing is evicted during measurement.
		MaxCost:     1 << 45,
		BufferItems: 64,
		Metrics:     true,
	})
	require.NoError(t, err)
	defer c.Close()
	c.Wait()

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	values := make([][]byte, n)
	for i := range values {
		values[i] = make([]byte, payload)
	}

	for i := 0; i < n; i++ {
		c.Set(uint64(i+1), values[i], payload)
		// Periodically drain the set buffer so no write is dropped under
		// contention and the measured cost matches the stored entries.
		if i%512 == 0 {
			c.Wait()
		}
	}
	c.Wait()

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	stored = int64(c.Metrics.KeysAdded() - c.Metrics.KeysEvicted())
	require.Equal(t, int64(n), stored)
	require.Zero(t, c.Metrics.SetsDropped())
	estimatedCost = c.cachePolicy.evict.used
	realHeap = int64(after.HeapAlloc - before.HeapAlloc)
	return stored, estimatedCost, realHeap
}

// TestCapacityEstimateMatchesRealMemory verifies that the cost-based capacity
// estimate tracks the real heap footprint across a range of value sizes.
//
// Previously only the storeItem struct (~56 bytes) was charged as internal
// cost, while each entry also consumes policy bookkeeping, two Go maps worth
// of bucket/overflow space and allocator alignment (~180 bytes measured per
// entry beyond the payload). The estimate therefore ran well below the real
// footprint and a cache "80% full" could already exhaust the container.
//
// The estimate must never under-report the real heap growth, and it should not
// over-report by more than 10% so capacity is not wasted.
func TestCapacityEstimateMatchesRealMemory(t *testing.T) {
	for _, payload := range []int64{0, 32, 128, 512, 1024, 4096} {
		stored, estimated, real := measureCacheGrowth(t, 100000, payload)

		ratio := float64(estimated) / float64(real)
		t.Logf("payload=%-4d estimate/entry=%.1f real/entry=%.1f ratio=%.3f",
			payload, float64(estimated)/float64(stored),
			float64(real)/float64(stored), ratio)

		require.GreaterOrEqual(t, estimated, real,
			"capacity estimate must not under-report real heap usage (payload=%d)", payload)
		require.LessOrEqual(t, ratio, 1.10,
			"capacity estimate over-reports real heap usage by more than 10%% (payload=%d)", payload)
	}
}

// TestCapacityEstimateMatchesRealMemoryWithTTL verifies the estimate also
// covers expiration-map bookkeeping when entries carry a TTL. TTLs are spread
// across multiple cleanup buckets to avoid an unrealistically compact layout.
func TestCapacityEstimateMatchesRealMemoryWithTTL(t *testing.T) {
	const n = 100000
	const payload = int64(128)

	c, err := NewCache(&Config[uint64, []byte]{
		NumCounters: int64(10 * n),
		MaxCost:     1 << 45,
		BufferItems: 64,
		Metrics:     true,
	})
	require.NoError(t, err)
	defer c.Close()
	c.Wait()

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	values := make([][]byte, n)
	for i := range values {
		values[i] = make([]byte, payload)
	}
	for i := 0; i < n; i++ {
		ttl := time.Duration(1+(i%50)) * time.Second
		c.SetWithTTL(uint64(i+1), values[i], payload, ttl)
		if i%512 == 0 {
			c.Wait()
		}
	}
	c.Wait()

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	stored := int64(c.Metrics.KeysAdded() - c.Metrics.KeysEvicted())
	require.Equal(t, int64(n), stored)
	estimated := c.cachePolicy.evict.used
	real := int64(after.HeapAlloc - before.HeapAlloc)
	ratio := float64(estimated) / float64(real)
	t.Logf("TTL estimate/entry=%.1f real/entry=%.1f ratio=%.3f",
		float64(estimated)/float64(stored), float64(real)/float64(stored), ratio)

	require.GreaterOrEqual(t, estimated, real,
		"capacity estimate must cover expiration-map overhead under TTL")
	require.LessOrEqual(t, ratio, 1.10)
}

// TestCapacityAt80PercentNoEviction fills a cache to 80% of MaxCost using the
// existing capacity test pattern and confirms that eviction has not started
// yet while the tracked cost stays below the eviction watermark.
func TestCapacityAt80PercentNoEviction(t *testing.T) {
	const targetEntries = 1000
	const payload = int64(128)

	perEntry := payload + entryOverhead
	maxCost := targetEntries * perEntry

	c, err := NewCache(&Config[uint64, []byte]{
		NumCounters: 10 * targetEntries,
		MaxCost:     maxCost,
		BufferItems: 64,
		Metrics:     true,
	})
	require.NoError(t, err)
	defer c.Close()

	fill := int(maxCost * 8 / (10 * perEntry)) // 80% of MaxCost in entry units
	for i := 0; i < fill; i++ {
		c.Set(uint64(i+1), make([]byte, payload), payload)
		if i%256 == 0 {
			c.Wait()
		}
	}
	c.Wait()

	used := c.cachePolicy.evict.used
	t.Logf("filled %d entries to %.1f%% of MaxCost (%d/%d)",
		fill, 100*float64(used)/float64(maxCost), used, maxCost)

	require.Zero(t, c.Metrics.KeysEvicted())
	require.Zero(t, c.Metrics.SetsRejected())
	require.LessOrEqual(t, float64(used), 0.85*float64(maxCost))
	require.LessOrEqual(t, used, c.cachePolicy.evict.evictionLimit())
}

// TestEvictionWatermarkTriggersBeforeHardLimit pushes writes past the
// watermark and asserts eviction brings the tracked cost back to (or under)
// the watermark instead of letting it run all the way to MaxCost. This is the
// guard against lagging eviction under a hard container memory limit.
func TestEvictionWatermarkTriggersBeforeHardLimit(t *testing.T) {
	const targetEntries = 1000

	perEntry := int64(1 + entryOverhead)
	maxCost := targetEntries * perEntry

	c, err := NewCache(&Config[int, int]{
		NumCounters: 10 * targetEntries,
		MaxCost:     maxCost,
		BufferItems: 64,
		Metrics:     true,
	})
	require.NoError(t, err)
	defer c.Close()

	// Write 3x the configured number of entries.
	for i := 0; i < 3*targetEntries; i++ {
		c.Set(i, i, 1)
		if i%256 == 0 {
			c.Wait()
		}
	}
	c.Wait()

	eviction := c.cachePolicy.evict
	require.NotZero(t, c.Metrics.KeysEvicted())
	require.LessOrEqual(t, eviction.used, eviction.evictionLimit(),
		"tracked cost must be reclaimed down to the watermark, not left near MaxCost")
	require.Less(t, eviction.evictionLimit(), eviction.getMaxCost())
	require.Equal(t, maxCost*evictionWatermarkNumerator/evictionWatermarkDenominator,
		eviction.evictionLimit())
}

// TestEvictionLimitTinyCache makes sure the watermark cannot shrink the
// effective limit below a single item for very small caches.
func TestEvictionLimitTinyCache(t *testing.T) {
	e := newSampledLFU(1)
	require.Equal(t, int64(1), e.evictionLimit())

	e = newSampledLFU(5)
	require.Equal(t, int64(4), e.evictionLimit())
}
