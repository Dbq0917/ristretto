/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package ristretto

import (
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/ristretto/v2/sim"
	"github.com/stretchr/testify/require"
)

// withSmallSetBuf shrinks the write buffer for the duration of a test so that
// saturation can be forced deterministically. Tests using it must not run in
// parallel because setBufSize is package-global.
func withSmallSetBuf(t *testing.T, size int) {
	t.Helper()
	original := setBufSize
	setBufSize = size
	t.Cleanup(func() { setBufSize = original })
}

// pauseProcessor stops the background processItems goroutine so the write
// buffer is guaranteed not to drain while the test fills it. The goroutine is
// restarted on cleanup (before the cache's own Close cleanup) so shutdown still
// works.
func pauseProcessor(t *testing.T, c *Cache[int, int]) {
	t.Helper()
	c.stop <- struct{}{}
	<-c.done
	t.Cleanup(func() { go c.processItems() })
}

func newDropTestCache(t *testing.T, metrics bool) *Cache[int, int] {
	t.Helper()
	c, err := NewCache(&Config[int, int]{
		NumCounters:        1000,
		MaxCost:            1000,
		BufferItems:        64,
		IgnoreInternalCost: true,
		Metrics:            metrics,
	})
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

func TestSetEInvalidArguments(t *testing.T) {
	c := newDropTestCache(t, false)

	// Negative TTL is an invalid request, not back-pressure.
	err := c.SetWithTTLE(1, 1, 1, -time.Second)
	require.ErrorIs(t, err, ErrInvalidTTL)

	// A closed/nil cache is also a hard error, never ErrSetBufferFull.
	c.Close()
	require.ErrorIs(t, c.SetE(1, 1, 1), ErrCacheClosed)
	var nilCache *Cache[int, int]
	require.ErrorIs(t, nilCache.SetE(1, 1, 1), ErrCacheClosed)
}

func TestSetEBufferFullIsDistinct(t *testing.T) {
	withSmallSetBuf(t, 4)
	c := newDropTestCache(t, true)
	pauseProcessor(t, c)

	var accepted, dropped int
	for i := 0; i < 40; i++ {
		err := c.SetE(i, i, 1)
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, ErrSetBufferFull):
			dropped++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}

	// Buffer capacity is the upper bound on accepted items while the processor
	// is paused; everything past it must be reported as a busy/full error.
	require.Equal(t, setBufSize, accepted)
	require.Equal(t, 40-setBufSize, dropped)
	require.Equal(t, uint64(dropped), c.DroppedSets())
	// The optional metric stays consistent with the always-on counter.
	require.Equal(t, c.DroppedSets(), c.Metrics.SetsDropped())

	// Legacy bool API keeps its original contract: busy == false.
	require.False(t, c.SetWithTTL(99, 99, 1, 0))
}

func TestDroppedSetsWithoutMetrics(t *testing.T) {
	withSmallSetBuf(t, 2)
	c := newDropTestCache(t, false)
	pauseProcessor(t, c)

	require.Nil(t, c.Metrics)
	for i := 0; i < 20; i++ {
		_ = c.SetE(i, i, 1)
	}
	// Saturation remains observable even with metrics collection disabled.
	require.Equal(t, uint64(20-setBufSize), c.DroppedSets())
}

func TestSetWaitTimesOut(t *testing.T) {
	withSmallSetBuf(t, 2)
	c := newDropTestCache(t, false)
	pauseProcessor(t, c)

	// Fill the buffer.
	require.NoError(t, c.SetE(1, 1, 1))
	require.NoError(t, c.SetE(2, 2, 1))

	// A bounded wait gives up with the same typed "busy" error instead of
	// hanging the caller indefinitely on the background goroutine.
	start := time.Now()
	err := c.SetWait(3, 3, 1, 20*time.Millisecond)
	elapsed := time.Since(start)
	require.ErrorIs(t, err, ErrSetBufferFull)
	require.GreaterOrEqual(t, elapsed, 15*time.Millisecond)
	require.Equal(t, uint64(1), c.DroppedSets())
}

func TestSetWaitSucceedsAfterDrain(t *testing.T) {
	withSmallSetBuf(t, 2)
	c := newDropTestCache(t, false)

	// With the processor running, a generous bounded wait must succeed even
	// under a burst that momentarily saturates the tiny buffer.
	for i := 0; i < 50; i++ {
		require.NoError(t, c.SetWait(i, i, 1, time.Second))
	}
	c.Wait()

	// All items were eventually processed; nothing stays dropped on a healthy
	// cache when callers are willing to wait.
	require.Equal(t, uint64(0), c.DroppedSets())
	for i := 0; i < 50; i++ {
		val, ok := c.Get(i)
		require.True(t, ok, "key %d missing", i)
		require.Equal(t, i, val)
	}
}

func TestClearResetsDroppedSets(t *testing.T) {
	c := newDropTestCache(t, false)
	c.droppedSets.Add(7)
	require.Equal(t, uint64(7), c.DroppedSets())

	// Clear zeroes the back-pressure counter.
	c.Clear()
	require.Equal(t, uint64(0), c.DroppedSets())
}

// TestSmallBufferBurstHitRatio mirrors the documented write use case: drive a
// Zipfian (skewed) read/write workload through a deliberately small write
// buffer using the non-blocking error API. It checks the success/failure
// accounting is exact, drops are observable, and the admitted hot keys still
// sustain a healthy hit ratio.
func TestSmallBufferBurstHitRatio(t *testing.T) {
	withSmallSetBuf(t, 16)
	c, err := NewCache(&Config[uint64, uint64]{
		NumCounters:        10000,
		MaxCost:            256,
		BufferItems:        64,
		IgnoreInternalCost: true,
		Metrics:            true,
	})
	require.NoError(t, err)
	defer c.Close()

	zipf := sim.NewZipfian(1.01, 1, 256)
	const total = 4000
	var accepted, dropped int
	for i := 0; i < total; i++ {
		k, zerr := zipf()
		require.NoError(t, zerr)
		if _, ok := c.Get(k); !ok {
			if err := c.SetE(k, k, 1); errors.Is(err, ErrSetBufferFull) {
				dropped++
			} else {
				require.NoError(t, err)
				accepted++
			}
		}
	}
	require.Greater(t, accepted, 0)
	require.Equal(t, uint64(dropped), c.DroppedSets())

	c.Wait()

	// Re-read with the same skew: the hot keys were written early, while the
	// buffer still had room, and stay cached; drops are now visible instead of
	// silently degrading the hit ratio.
	hits := 0
	const reads = total
	for i := 0; i < reads; i++ {
		k, zerr := zipf()
		require.NoError(t, zerr)
		if _, ok := c.Get(k); ok {
			hits++
		}
	}
	ratio := float64(hits) / float64(reads)
	t.Logf("accepted=%d dropped=%d droppedSets=%d hit-ratio=%.2f",
		accepted, dropped, c.DroppedSets(), ratio)
	require.Greater(t, ratio, 0.5)
}

// TestSmallBufferBurstNoDropsWhenWaiting shows the alternative strategy: with
// the same tiny buffer, bounded waits keep every write and reach full coverage
// instead of discarding during the burst.
func TestSmallBufferBurstNoDropsWhenWaiting(t *testing.T) {
	withSmallSetBuf(t, 8)
	c, err := NewCache(&Config[int, int]{
		NumCounters:        1000,
		MaxCost:            500,
		BufferItems:        64,
		IgnoreInternalCost: true,
		Metrics:            false,
	})
	require.NoError(t, err)
	defer c.Close()

	for i := 0; i < 500; i++ {
		require.NoError(t, c.SetWait(i, i, 1, time.Second))
	}
	c.Wait()

	require.Equal(t, uint64(0), c.DroppedSets())
	for i := 0; i < 500; i++ {
		val, ok := c.Get(i)
		require.True(t, ok, "key %d missing", i)
		require.Equal(t, i, val)
	}
}
