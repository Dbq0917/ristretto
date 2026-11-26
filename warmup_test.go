/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package ristretto

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCacheWarmupLoadsBeforeReturn(t *testing.T) {
	c, err := NewCache(&Config[int, string]{
		NumCounters:        1000,
		MaxCost:            1000,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	require.NoError(t, err)
	defer c.Close()

	keys := make([]int, 100)
	for i := range keys {
		keys[i] = i
	}
	result := c.Warmup(keys, func(key int) (string, int64) {
		return fmt.Sprintf("value-%d", key), 1
	}, WarmupOptions{Concurrency: 8})

	require.Equal(t, WarmupResult{Loaded: len(keys)}, result)
	for _, key := range keys {
		value, ok := c.Get(key)
		require.True(t, ok, "warmed key %d should be immediately visible", key)
		require.Equal(t, fmt.Sprintf("value-%d", key), value)
	}
}

func TestCacheWarmupReportsAdmissionRejections(t *testing.T) {
	c, err := NewCache(&Config[int, int]{
		NumCounters:        100,
		MaxCost:            10,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	require.NoError(t, err)
	defer c.Close()

	result := c.Warmup([]int{1, 2, 3}, func(key int) (int, int64) {
		if key == 3 {
			return key, 100
		}
		return key, 1
	})

	require.Equal(t, WarmupResult{Loaded: 2, Rejected: 1}, result)
	for _, key := range []int{1, 2} {
		_, ok := c.Get(key)
		require.True(t, ok)
	}
	_, ok := c.Get(3)
	require.False(t, ok)
}

func TestCacheWarmupReportsDroppedSetBuffer(t *testing.T) {
	originalSetBufSize := setBufSize
	setBufSize = 1
	defer func() { setBufSize = originalSetBufSize }()

	var exited atomic.Int64
	c, err := NewCache(&Config[int, int]{
		NumCounters:        100,
		MaxCost:            100,
		BufferItems:        64,
		IgnoreInternalCost: true,
		OnExit: func(int) {
			exited.Add(1)
		},
	})
	require.NoError(t, err)

	// Stop the consumer and leave the single setBuf slot occupied so the
	// warmup write must take the same non-blocking drop path as a regular Set.
	c.stop <- struct{}{}
	<-c.done
	blocker := &Item[int]{}
	c.setBuf <- blocker
	defer func() {
		<-c.setBuf
		go c.processItems()
		c.Close()
	}()

	result := c.Warmup([]int{1}, func(key int) (int, int64) {
		return key, 1
	})

	require.Equal(t, WarmupResult{Dropped: 1}, result)
	require.Equal(t, int64(1), exited.Load(), "a dropped value must be released")
}

func TestCacheWarmupHonorsConcurrency(t *testing.T) {
	c, err := NewCache(&Config[int, int]{
		NumCounters:        100,
		MaxCost:            1000,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	require.NoError(t, err)
	defer c.Close()

	var inFlight atomic.Int64
	var maxInFlight atomic.Int64
	keys := make([]int, 30)
	for i := range keys {
		keys[i] = i
	}

	result := c.Warmup(keys, func(key int) (int, int64) {
		current := inFlight.Add(1)
		for {
			max := maxInFlight.Load()
			if current <= max || maxInFlight.CompareAndSwap(max, current) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		inFlight.Add(-1)
		return key, 1
	}, WarmupOptions{Concurrency: 3})

	require.Equal(t, WarmupResult{Loaded: len(keys)}, result)
	require.LessOrEqual(t, maxInFlight.Load(), int64(3))
}

func TestCacheWarmupEntriesParticipateInEviction(t *testing.T) {
	c, err := NewCache(&Config[int, int]{
		NumCounters:        100,
		MaxCost:            2,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	require.NoError(t, err)
	defer c.Close()

	result := c.Warmup([]int{1, 2}, func(key int) (int, int64) {
		return key, 1
	})
	require.Equal(t, WarmupResult{Loaded: 2}, result)

	require.True(t, c.Set(3, 3, 1))
	c.Wait()

	value, ok := c.Get(3)
	require.True(t, ok)
	require.Equal(t, 3, value)
}

func TestCacheWarmupE(t *testing.T) {
	c, err := NewCache(&Config[int, int]{
		NumCounters:        100,
		MaxCost:            100,
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	require.NoError(t, err)
	defer c.Close()

	result, err := c.WarmupE([]int{1, 2}, func(key int) (int, int64, error) {
		if key == 2 {
			return 0, 0, fmt.Errorf("load %d", key)
		}
		return key, 1, nil
	})

	require.Equal(t, WarmupResult{Loaded: 1, Failed: 1}, result)
	require.EqualError(t, err, "load 2")
}
