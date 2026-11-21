/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package ristretto

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type evictionRecorder struct {
	mu   sync.Mutex
	keys map[int]int
}

func newEvictionRecorder() *evictionRecorder {
	return &evictionRecorder{keys: make(map[int]int)}
}

func (r *evictionRecorder) record(item *Item[int]) {
	r.mu.Lock()
	r.keys[item.Value]++
	r.mu.Unlock()
}

func (r *evictionRecorder) snapshot() map[int]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[int]int, len(r.keys))
	for key, count := range r.keys {
		out[key] = count
	}
	return out
}

// TestExpiredKeysEvictedBeforeLiveUnderPressure fills the cache with a mix of
// dead TTL entries and cold-but-live entries. Once the TTLs elapse, new writes
// must reclaim the dead entries before SampledLFU is allowed to evict a live
// key. The periodic bucket sweep is effectively disabled so the test exercises
// the write-pressure path.
func TestExpiredKeysEvictedBeforeLiveUnderPressure(t *testing.T) {
	const (
		capacity     = 10
		expiredKeys  = 5
		liveKeys     = 3
		liveTTLKeys  = 2
		replacement  = 5
		expiredStart = 0
		liveStart    = expiredKeys
		liveTTLStart = liveStart + liveKeys
		newStart     = liveTTLStart + liveTTLKeys
	)

	recorder := newEvictionRecorder()
	c, err := NewCache(&Config[int, int]{
		NumCounters:            100,
		MaxCost:                capacity,
		BufferItems:            64,
		IgnoreInternalCost:     true,
		Metrics:                true,
		TtlTickerDurationInSec: 3600,
		OnEvict: func(item *Item[int]) {
			recorder.record(item)
		},
	})
	require.NoError(t, err)
	t.Cleanup(c.Close)

	for i := expiredStart; i < expiredStart+expiredKeys; i++ {
		require.True(t, c.SetWithTTL(i, i, 1, 20*time.Millisecond))
	}
	for i := liveStart; i < liveStart+liveKeys; i++ {
		require.True(t, c.Set(i, i, 1))
	}
	for i := liveTTLStart; i < liveTTLStart+liveTTLKeys; i++ {
		require.True(t, c.SetWithTTL(i, i, 1, time.Hour))
	}
	c.Wait()

	time.Sleep(30 * time.Millisecond)

	for i := newStart; i < newStart+replacement; i++ {
		require.True(t, c.Set(i, i, 1))
	}
	c.Wait()

	for i := liveStart; i < newStart+replacement; i++ {
		_, ok := c.Get(i)
		require.True(t, ok, "live key %d should have survived dead-key cleanup", i)
	}
	require.InDelta(t, 1.0, c.Metrics.Ratio(), 0.0001)

	for i := expiredStart; i < expiredStart+expiredKeys; i++ {
		_, ok := c.Get(i)
		require.False(t, ok, "expired key %d should be gone", i)
	}

	evicted := recorder.snapshot()
	for i := expiredStart; i < expiredStart+expiredKeys; i++ {
		require.Equal(t, 1, evicted[i], "expired key %d should be reclaimed exactly once", i)
	}
	for i := newStart; i < newStart+replacement; i++ {
		require.NotContains(t, evicted, i, "new key %d should not be evicted", i)
	}

	require.Equal(t, uint64(expiredKeys), c.Metrics.KeysExpired())
	require.Equal(t, uint64(expiredKeys), c.Metrics.CostExpired())
	require.Zero(t, c.Metrics.KeysEvicted(),
		"live keys must not be capacity-evicted while expired keys are available")
}

// TestExpiredBacklogUnderSustainedWrites covers the high-write case where a
// large fraction of capacity is occupied by dead keys. A batch of new writes
// should free the entire backlog and leave all cold live entries addressable.
func TestExpiredBacklogUnderSustainedWrites(t *testing.T) {
	const (
		capacity    = 200
		expiredKeys = 100
		liveKeys    = 100
		replacement = 100
	)

	recorder := newEvictionRecorder()
	c, err := NewCache(&Config[int, int]{
		NumCounters:            1000,
		MaxCost:                capacity,
		BufferItems:            64,
		IgnoreInternalCost:     true,
		Metrics:                true,
		TtlTickerDurationInSec: 3600,
		OnEvict: func(item *Item[int]) {
			recorder.record(item)
		},
	})
	require.NoError(t, err)
	t.Cleanup(c.Close)

	for i := 0; i < expiredKeys; i++ {
		require.True(t, c.SetWithTTL(i, i, 1, 20*time.Millisecond))
	}
	for i := expiredKeys; i < expiredKeys+liveKeys; i++ {
		require.True(t, c.Set(i, i, 1))
	}
	c.Wait()
	time.Sleep(30 * time.Millisecond)

	for i := expiredKeys + liveKeys; i < expiredKeys+liveKeys+replacement; i++ {
		require.True(t, c.Set(i, i, 1), fmt.Sprintf("replacement key %d was dropped", i))
	}
	c.Wait()

	for i := expiredKeys; i < expiredKeys+liveKeys+replacement; i++ {
		_, ok := c.Get(i)
		require.True(t, ok, "live key %d should still be cached", i)
	}

	evicted := recorder.snapshot()
	require.Len(t, evicted, expiredKeys)
	for i := 0; i < expiredKeys; i++ {
		require.Equal(t, 1, evicted[i])
	}
	t.Logf("dead_evicted=%d live_evicted=%d new_evicted=%d live_hit_ratio=%.3f",
		countEvictions(evicted, 0, expiredKeys),
		countEvictions(evicted, expiredKeys, expiredKeys+liveKeys),
		countEvictions(evicted, expiredKeys+liveKeys, expiredKeys+liveKeys+replacement),
		c.Metrics.Ratio())
	require.Equal(t, uint64(expiredKeys), c.Metrics.KeysExpired())
	require.Zero(t, c.Metrics.KeysEvicted())
	require.InDelta(t, 1.0, c.Metrics.Ratio(), 0.0001)
}

func countEvictions(evicted map[int]int, start, end int) int {
	count := 0
	for key, times := range evicted {
		if key >= start && key < end {
			count += times
		}
	}
	return count
}

// TestExpiredCleanupKeepsRefreshedEntry verifies the version check used by the
// pressure path. Refreshing a key must move its deadline in the queue, and a
// stale cleanup observation must not delete the refreshed value.
func TestExpiredCleanupKeepsRefreshedEntry(t *testing.T) {
	s := newShardedMap[int]()
	p := newDefaultPolicy[int](100, 10)
	t.Cleanup(p.Close)

	key, conflict := uint64(1), uint64(1)
	oldExpiry := time.Now().Add(-time.Second)
	s.Set(&Item[int]{
		Key:        key,
		Conflict:   conflict,
		Value:      1,
		Expiration: oldExpiry,
	})
	_, added := p.Add(key, 1)
	require.True(t, added)

	newExpiry := time.Now().Add(time.Hour)
	_, updated := s.Update(&Item[int]{
		Key:        key,
		Conflict:   conflict,
		Value:      2,
		Expiration: newExpiry,
	})
	require.True(t, updated)

	cleaned := s.expiryMap.cleanupExpired(s, p, nil, time.Now(), p.Cap()+1)
	require.Zero(t, cleaned)
	value, ok := s.Get(key, conflict)
	require.True(t, ok)
	require.Equal(t, 2, value)
	require.Equal(t, int64(9), p.Cap())
}
