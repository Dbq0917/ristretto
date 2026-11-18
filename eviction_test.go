/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package ristretto

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEvictionQueueLosslessAndNonBlocking(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	var count atomic.Uint64

	q := newEvictionQueue(func(*Item[int]) {
		startedOnce.Do(func() { close(started) })
		<-release
		count.Add(1)
	})

	const numItems = 10000
	enqueued := make(chan struct{})
	go func() {
		for i := 0; i < numItems; i++ {
			q.enqueue(&Item[int]{Key: uint64(i)})
		}
		close(enqueued)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("eviction callback did not start")
	}
	select {
	case <-enqueued:
	case <-time.After(time.Second):
		t.Fatal("enqueue blocked behind the eviction callback")
	}

	close(release)
	q.flush()
	require.Equal(t, uint64(numItems), count.Load())
	q.close()
}

func TestOnEvictDoesNotBlockWrites(t *testing.T) {
	originalSetBufSize := setBufSize
	setBufSize = 8
	defer func() { setBufSize = originalSetBufSize }()

	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	c, err := NewCache(&Config[int, int]{
		NumCounters:        1000,
		MaxCost:            10,
		BufferItems:        64,
		IgnoreInternalCost: true,
		OnEvict: func(*Item[int]) {
			startedOnce.Do(func() { close(started) })
			<-release
		},
	})
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		require.True(t, c.Set(i, i, 1), "set %d was dropped while OnEvict was blocked", i)
		time.Sleep(200 * time.Microsecond)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("eviction callback did not start")
	}

	close(release)
	c.Close()
}

func TestWaitForEvictionsWaitsForPendingCallbacks(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	c, err := NewCache(&Config[int, int]{
		NumCounters:        100,
		MaxCost:            2,
		BufferItems:        64,
		IgnoreInternalCost: true,
		OnEvict: func(*Item[int]) {
			once.Do(func() { close(started) })
			<-release
		},
	})
	require.NoError(t, err)

	require.True(t, c.Set(1, 1, 1))
	require.True(t, c.Set(2, 2, 1))
	require.True(t, c.Set(3, 3, 1))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("eviction callback did not start")
	}

	waited := make(chan struct{})
	go func() {
		c.WaitForEvictions()
		close(waited)
	}()

	select {
	case <-waited:
		t.Fatal("WaitForEvictions returned before the callback completed")
	case <-time.After(20 * time.Millisecond):
	}

	close(release)
	select {
	case <-waited:
	case <-time.After(time.Second):
		t.Fatal("WaitForEvictions did not return after callbacks completed")
	}
	c.Close()
}

func TestClearDeliversEveryEvictedItem(t *testing.T) {
	const numItems = 500
	evicted := make(map[int]int, numItems)
	var mu sync.Mutex

	c, err := NewCache(&Config[int, int]{
		NumCounters:        1000,
		MaxCost:            1000,
		BufferItems:        64,
		IgnoreInternalCost: true,
		OnEvict: func(item *Item[int]) {
			mu.Lock()
			evicted[item.Value]++
			mu.Unlock()
		},
	})
	require.NoError(t, err)

	for i := 0; i < numItems; i++ {
		require.True(t, c.Set(i, i, 1))
		time.Sleep(50 * time.Microsecond)
	}
	c.Wait()
	c.Clear()

	mu.Lock()
	require.Len(t, evicted, numItems)
	for i := 0; i < numItems; i++ {
		require.Equal(t, 1, evicted[i], "item %d was not delivered exactly once", i)
	}
	mu.Unlock()
	c.Close()
}
