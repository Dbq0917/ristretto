/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package ristretto

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConcurrentNewWritesStayWithinReservationBudget(t *testing.T) {
	c, err := NewCache(&Config[int, int]{
		NumCounters:        1000,
		MaxCost:            1000,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	require.NoError(t, err)

	// Stop the consumer so every accepted write remains in flight.
	c.stop <- struct{}{}
	<-c.done

	const (
		writes    = 200
		itemCost  = int64(10)
		maxCost   = int64(1000)
		allowance = maxCost / maxCostOvershootDivisor
	)

	var accepted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < writes; i++ {
		wg.Add(1)
		go func(key int) {
			defer wg.Done()
			if c.Set(key, key, itemCost) {
				accepted.Add(1)
			}
		}(i)
	}
	wg.Wait()

	// One extra item may be reserved so an item that will trigger eviction can
	// reach the policy, but a batch cannot accumulate an unbounded backlog.
	require.LessOrEqual(t, c.cachePolicy.inFlight.Load(), maxCost+allowance+itemCost)
	require.Less(t, accepted.Load(), int64(writes))
	require.NotZero(t, accepted.Load())

	go c.processItems()
	c.Wait()
	require.Zero(t, c.cachePolicy.inFlight.Load())
	c.Close()
}

func TestConcurrentUpdatesStayWithinReservationBudget(t *testing.T) {
	c, err := NewCache(&Config[int, int]{
		NumCounters:        1000,
		MaxCost:            1000,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	require.NoError(t, err)

	const keys = 20
	for i := 0; i < keys; i++ {
		require.True(t, c.Set(i, 1, 1))
	}
	c.Wait()

	// Updates are written into the store before the policy processes them.
	// Stopping the consumer makes the in-flight overrun deterministic.
	c.stop <- struct{}{}
	<-c.done

	const newCost = int64(100)
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < keys; i++ {
		wg.Add(1)
		go func(key int) {
			defer wg.Done()
			if c.Set(key, 2, newCost) {
				accepted.Add(1)
			}
		}(i)
	}
	wg.Wait()

	maxAllowed := c.MaxCost() + c.MaxCost()/maxCostOvershootDivisor + newCost
	require.LessOrEqual(t, cacheStoreCost(c), maxAllowed)
	require.LessOrEqual(t, c.cachePolicy.inFlight.Load(), maxAllowed-int64(keys))
	require.Less(t, accepted.Load(), int64(keys))
	require.NotZero(t, accepted.Load())

	go c.processItems()
	c.Wait()
	require.Zero(t, c.cachePolicy.inFlight.Load())
	c.Close()
}

func cacheStoreCost(c *Cache[int, int]) int64 {
	store := c.storedItems.(*shardedMap[int])
	var total int64
	for _, shard := range store.shards {
		shard.RLock()
		for _, item := range shard.data {
			total += item.cost
		}
		shard.RUnlock()
	}
	return total
}
