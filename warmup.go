/*
 * SPDX-FileCopyrightText: © 2017-2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package ristretto

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ErrNilWarmupLoader is returned when WarmupE is called without a loader.
var ErrNilWarmupLoader = errors.New("ristretto: warmup loader is nil")

// WarmupOptions controls how aggressively Warmup fetches values from the
// caller's backing store.
type WarmupOptions struct {
	// Concurrency is the maximum number of loader calls in flight at once.
	// Values less than one are treated as one.
	Concurrency int

	// RatePerSecond limits the aggregate number of loader calls started per
	// second. Values less than or equal to zero disable rate limiting.
	//
	// RatePerSecond is applied across all workers, not per worker.
	RatePerSecond int
}

// WarmupResult describes what happened to the keys passed to Warmup.
//
// Loaded counts entries accepted by the admission policy and applied to the
// cache. Rejected counts entries refused by the admission policy. Dropped
// counts entries that could not be queued because the non-blocking set buffer
// was full (or because the cache was closed). Failed counts keys for which the
// loader returned an error.
//
// Entries counted as Loaded use the same admission and SampledLFU eviction
// path as regular Set calls, so they can be evicted by later writes.
type WarmupResult struct {
	Loaded   int
	Rejected int
	Dropped  int
	Failed   int
}

// WarmupLoader loads a value and its cost from the caller's backing store.
type WarmupLoader[K Key, V any] func(key K) (value V, cost int64)

// WarmupLoaderE is the error-returning form of WarmupLoader.
type WarmupLoaderE[K Key, V any] func(key K) (value V, cost int64, err error)

// Warmup loads a caller-supplied set of keys into the cache and waits until the
// writes have gone through the normal admission and eviction path. Callers do
// not need to call Wait after it returns.
//
// Use WarmupOptions to bound backend concurrency and the aggregate load rate.
// If a write is dropped because the internal set buffer is full, Warmup does
// not bypass the non-blocking write path: the entry is reported in Dropped.
func (c *Cache[K, V]) Warmup(
	keys []K, load WarmupLoader[K, V], options ...WarmupOptions,
) WarmupResult {
	if load == nil {
		return WarmupResult{Failed: len(keys)}
	}
	result, _ := c.warmup(keys, func(key K) (V, int64, error) {
		value, cost := load(key)
		return value, cost, nil
	}, firstWarmupOptions(options))
	return result
}

// WarmupE is Warmup with loader errors reported to the caller. The returned
// result is complete even when some loader calls fail.
func (c *Cache[K, V]) WarmupE(
	keys []K, load WarmupLoaderE[K, V], options ...WarmupOptions,
) (WarmupResult, error) {
	if len(keys) == 0 {
		return WarmupResult{}, nil
	}
	if load == nil {
		return WarmupResult{Failed: len(keys)}, ErrNilWarmupLoader
	}
	return c.warmup(keys, load, firstWarmupOptions(options))
}

func firstWarmupOptions(options []WarmupOptions) WarmupOptions {
	if len(options) == 0 {
		return WarmupOptions{}
	}
	return options[0]
}

func (c *Cache[K, V]) warmup(
	keys []K, load WarmupLoaderE[K, V], options WarmupOptions,
) (WarmupResult, error) {
	if len(keys) == 0 {
		return WarmupResult{}, nil
	}
	if c == nil || c.isClosed.Load() {
		return WarmupResult{Dropped: len(keys)}, nil
	}
	if load == nil {
		return WarmupResult{Failed: len(keys)}, ErrNilWarmupLoader
	}

	concurrency := options.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	if concurrency > len(keys) {
		concurrency = len(keys)
	}

	var ticker *time.Ticker
	var rateC <-chan time.Time
	if options.RatePerSecond > 0 {
		interval := time.Second / time.Duration(options.RatePerSecond)
		if interval < time.Nanosecond {
			interval = time.Nanosecond
		}
		ticker = time.NewTicker(interval)
		rateC = ticker.C
		defer ticker.Stop()
	}

	var loaded atomic.Int64
	var rejected atomic.Int64
	var dropped atomic.Int64
	var failed atomic.Int64
	var queued atomic.Int64
	var started atomic.Int64

	var errMu sync.Mutex
	var errs []error

	jobs := make(chan K)
	var wg sync.WaitGroup
	wg.Add(concurrency)

	worker := func() {
		defer wg.Done()
		for key := range jobs {
			// Start the first load immediately, then pace the rest.
			if rateC != nil && started.Add(1) > 1 {
				<-rateC
			}
			if c.isClosed.Load() {
				dropped.Add(1)
				continue
			}

			value, cost, err := load(key)
			if err != nil {
				failed.Add(1)
				errMu.Lock()
				errs = append(errs, err)
				errMu.Unlock()
				continue
			}

			state := &warmupItemState{done: make(chan bool, 1)}
			switch c.setWithTTL(key, value, cost, 0, state) {
			case setQueued:
				queued.Add(1)
				if <-state.done {
					loaded.Add(1)
				} else {
					rejected.Add(1)
				}
			case setApplied:
				loaded.Add(1)
			case setDropped:
				dropped.Add(1)
				// The cache never took ownership of this value.
				c.onExit(value)
			}
		}
	}

	for i := 0; i < concurrency; i++ {
		go worker()
	}
	for _, key := range keys {
		jobs <- key
	}
	close(jobs)
	wg.Wait()

	// All tracked items have reported their admission result, but keep the
	// regular Wait barrier as the final visibility guarantee for queued writes.
	if queued.Load() > 0 {
		c.Wait()
	}

	result := WarmupResult{
		Loaded:   int(loaded.Load()),
		Rejected: int(rejected.Load()),
		Dropped:  int(dropped.Load()),
		Failed:   int(failed.Load()),
	}
	return result, errors.Join(errs...)
}

type warmupItemState struct {
	done chan bool
}

func (w *warmupItemState) complete(accepted bool) {
	if w == nil {
		return
	}
	select {
	case w.done <- accepted:
	default:
	}
}
