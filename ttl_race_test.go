/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package ristretto

import (
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/stretchr/testify/require"
)

// TestCleanupConcurrentUpdateAccounting deterministically reproduces the
// divergence between the store contents and the policy cost accounting when an
// expiring key is refreshed by a concurrent SetWithTTL.
//
// The cleanup goroutine used to remove the key from the policy first and only
// afterwards from the store. A SetWithTTL landing in that window re-inserted
// the key into the store while the policy had already dropped its cost, so the
// cache ended up holding items that were never accounted for and the MaxCost
// boundary could no longer be enforced.
func TestCleanupConcurrentUpdateAccounting(t *testing.T) {
	const maxCost = 8

	s := newShardedMap[int]()
	p := newDefaultPolicy[int](1000, maxCost)
	t.Cleanup(p.Close)

	now := time.Now()
	oldExpiry := now.Add(time.Second)
	key, conflict := z.KeyToHash(1)

	// Seed the store, the expiration map and the policy with one accounted key.
	s.Set(&Item[int]{Key: key, Conflict: conflict, Value: 1, Expiration: oldExpiry})
	s.expiryMap.add(key, conflict, oldExpiry)
	_, added := p.Add(key, 1)
	require.True(t, added)
	require.Equal(t, int64(maxCost-1), p.Cap())

	// Move the cleanup watermark past the key's expiration bucket while the
	// key is still present, then refresh the key with a long TTL. Cleanup
	// must observe the refreshed entry and leave both store and policy alone.
	s.expiryMap.Lock()
	s.expiryMap.lastCleanedBucketNum = storageBucket(oldExpiry) - 2
	s.expiryMap.Unlock()

	newExpiry := now.Add(time.Hour)
	_, refreshed := s.Update(&Item[int]{
		Key: key, Conflict: conflict, Value: 2, Expiration: newExpiry,
	})
	require.True(t, refreshed)

	cleaned := s.expiryMap.cleanup(s, p, nil)
	require.Zero(t, cleaned, "refreshed key must not be swept with its old bucket")

	// The refreshed key survives and the policy still accounts for its cost.
	_, ok := s.Get(key, conflict)
	require.True(t, ok, "refreshed key must remain visible in the store")
	require.Equal(t, int64(maxCost-1), p.Cap(),
		"policy accounting must match the items still held by the store")
	require.Zero(t, p.metrics.KeysExpired())
}

// TestCleanupConcurrentSetTTL keeps a cache under sustained concurrent
// TTL refresh/expiry pressure and verifies that the bookkeeping used for the
// MaxCost boundary never drifts away from the items actually stored.
func TestCleanupConcurrentSetTTL(t *testing.T) {
	const (
		numKeys = 64
		maxCost = 16
		rounds  = 200
		workers = 8
	)

	c, err := NewCache(&Config[int, int]{
		NumCounters:            numKeys * 10,
		MaxCost:                maxCost,
		BufferItems:            64,
		IgnoreInternalCost:     true,
		TtlTickerDurationInSec: 1,
		Metrics:                true,
	})
	require.NoError(t, err)
	t.Cleanup(c.Close)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				k := (id*rounds + r) % numKeys
				// Alternate very short and long TTLs so refreshes constantly
				// race against expiration cleanup.
				ttl := 20 * time.Millisecond
				if r%2 == 0 {
					ttl = time.Hour
				}
				c.SetWithTTL(k, r, 1, ttl)
				_, _ = c.Get(k)
				if r%7 == 0 {
					c.Del(k)
				}
			}
		}(w)
	}
	wg.Wait()
	c.Wait()

	// Give the TTL cleanup goroutine a chance to drain the last expired bucket.
	time.Sleep(120 * time.Millisecond)
	c.Wait()

	// The policy is the single authority for the MaxCost invariant: the cost
	// it considers used must never exceed the configured capacity.
	require.GreaterOrEqual(t, c.RemainingCost(), int64(0),
		"used cost must never exceed MaxCost; remaining=%s", c.RemainingCost())

	// Count both live entries and entries that are expired but not yet swept
	// (the TTL sweeper runs on a fixed tick). Both legitimately occupy ledger
	// cost until the next cleanup; the invariant is that the ledger matches
	// physical store entries one-for-one and never exceeds MaxCost.
	stored := 0
	pendingExpiry := 0
	now := time.Now()
	sm := c.storedItems.(*shardedMap[int])
	c.storedItems.IterValues(func(v int) bool {
		stored++
		return false
	})
	for _, shard := range sm.shards {
		shard.RLock()
		for _, si := range shard.data {
			if !si.expiration.IsZero() && now.After(si.expiration) {
				pendingExpiry++
			}
		}
		shard.RUnlock()
	}
	used := c.MaxCost() - c.RemainingCost()
	if int64(stored) != used {
		c.cachePolicy.Lock()
		ghosts := make([]uint64, 0)
		liveInStore := make(map[uint64]struct{}, stored)
		for _, shard := range sm.shards {
			shard.RLock()
			for h, si := range shard.data {
				if si.expiration.IsZero() || !now.After(si.expiration) {
					liveInStore[h] = struct{}{}
				}
			}
			shard.RUnlock()
		}
		for k := range c.cachePolicy.evict.keyCosts {
			if _, ok := liveInStore[k]; !ok {
				ghosts = append(ghosts, k)
			}
		}
		policyKeys := len(c.cachePolicy.evict.keyCosts)
		c.cachePolicy.Unlock()
		t.Logf("divergence: live=%d pendingExpiry=%d policyKeys=%d used=%d remaining=%d liveGhosts=%v",
			stored, pendingExpiry, policyKeys, used, c.RemainingCost(), ghosts)
		t.Logf("metrics: added=%d evicted=%d expired=%d updated=%d rejected=%d",
			c.Metrics.KeysAdded(), c.Metrics.KeysEvicted(), c.Metrics.KeysExpired(),
			c.Metrics.KeysUpdated(), c.Metrics.SetsRejected())
	}
	require.Equal(t, int64(stored+pendingExpiry), used,
		"ledger used cost (%d) must equal live (%d) plus pending-expiry (%d) entries",
		used, stored, pendingExpiry)
}

// TestExpirationAndEvictionMetricsAreSeparate verifies that TTL expirations
// and capacity evictions update distinct counters instead of overwriting a
// shared "removal" statistic.
func TestExpirationAndEvictionMetricsAreSeparate(t *testing.T) {
	// Drive the components directly so the TTL sweep is deterministic and does
	// not depend on the 5-second bucket granularity of the background ticker.
	s := newShardedMap[int]()
	p := newDefaultPolicy[int](1000, 4)
	t.Cleanup(p.Close)
	m := newMetrics()
	p.CollectMetrics(m)

	now := time.Now()
	// An already-elapsed expiration: its bucket is behind the current cleanup
	// watermark so the sweep deterministically reaches it.
	expiry := now.Add(-time.Second)
	expiryBucket := storageBucket(expiry)
	key1, conflict1 := z.KeyToHash(1)
	key2, conflict2 := z.KeyToHash(2)
	s.Set(&Item[int]{Key: key1, Conflict: conflict1, Value: 1, Expiration: expiry})
	s.Set(&Item[int]{Key: key2, Conflict: conflict2, Value: 2, Expiration: expiry})
	s.expiryMap.add(key1, conflict1, expiry)
	s.expiryMap.add(key2, conflict2, expiry)
	p.Add(key1, 1)
	p.Add(key2, 1)

	// Move the sweep watermark past the expiration bucket, then sweep.
	s.expiryMap.Lock()
	s.expiryMap.lastCleanedBucketNum = expiryBucket - 1
	s.expiryMap.Unlock()
	swept := s.expiryMap.cleanup(s, p, nil)
	require.Equal(t, 1, swept)

	require.Equal(t, uint64(2), m.KeysExpired(),
		"TTL cleanup must record expirations on their own counter")
	require.Equal(t, uint64(0), m.KeysEvicted())
	require.Equal(t, uint64(2), m.CostExpired())

	// Capacity pressure on the same policy uses the independent eviction path.
	var evicted uint64
	for k := uint64(3); k <= 8; k++ {
		victims, added := p.Add(k, 1)
		require.True(t, added)
		// Mirror the accepted key in the store, matching processItems.
		s.Set(&Item[int]{Key: k, Value: int(k), Cost: 1})
		for _, victim := range victims {
			if _, _, _, ok := s.Del(victim.Key, 0); ok {
				evicted++
				p.EvictAccounting(victim.Key)
			}
		}
	}
	require.GreaterOrEqual(t, evicted, uint64(1))
	require.GreaterOrEqual(t, m.KeysEvicted(), uint64(1),
		"capacity pressure must record evictions on their own counter")
	require.GreaterOrEqual(t, m.CostEvicted(), uint64(1))
	require.GreaterOrEqual(t, p.Cap(), int64(0))
}

// TestEvictionVictimsRefreshedKeepLedger verifies the deferred accounting of
// sampled-LFU victims: a victim whose store removal fails because it was
// refreshed concurrently keeps its ledger entry, and the policy never reports
// more used cost than the configured MaxCost.
func TestEvictionVictimsRefreshedKeepLedger(t *testing.T) {
	p := newDefaultPolicy[int](1000, 4)
	t.Cleanup(p.Close)

	for k := uint64(1); k <= 4; k++ {
		_, added := p.Add(k, 1)
		require.True(t, added)
	}
	require.Equal(t, int64(0), p.Cap())

	// A fifth key requires evicting one victim.
	victims, added := p.Add(5, 1)
	require.True(t, added)
	require.Len(t, victims, 1)

	// Simulate the store failing to remove the victim (concurrent refresh).
	// No RemoveAccounting/EvictAccounting call happens, so the ledger keeps
	// the victim's cost alongside the newly admitted key: nothing is silently
	// dropped from the ledger just because a victim got selected.
	require.Equal(t, int64(5), p.evict.used)
	require.Len(t, p.evict.keyCosts, 5)

	// Confirming the victim removal reclaims exactly its cost.
	cost := p.EvictAccounting(victims[0].Key)
	require.Equal(t, int64(1), cost)
	require.Equal(t, int64(4), p.evict.used)
	require.GreaterOrEqual(t, p.Cap(), int64(0))
}
