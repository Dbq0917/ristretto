/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package ristretto

import "sync"

// evictionQueue decouples eviction callbacks from the goroutine that processes
// cache writes. Enqueueing is lossless and never waits for a callback to
// finish. A single worker preserves the callback ordering of the old
// synchronous implementation.
type evictionQueue[V any] struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []*Item[V]
	head   int
	closed bool
	done   chan struct{}

	// enqueued and completed make flush a stable barrier. New items enqueued
	// concurrently with flush are handled by a later flush.
	enqueued  uint64
	completed uint64

	callback func(*Item[V])
}

func newEvictionQueue[V any](callback func(*Item[V])) *evictionQueue[V] {
	q := &evictionQueue[V]{
		done:     make(chan struct{}),
		callback: callback,
	}
	q.cond = sync.NewCond(&q.mu)
	go q.run()
	return q
}

func (q *evictionQueue[V]) enqueue(item *Item[V]) {
	if q == nil || item == nil {
		return
	}

	q.mu.Lock()
	if q.closed {
		// This should only be possible if a caller races with Close. Falling
		// back to a synchronous callback is preferable to silently losing an
		// eviction.
		q.mu.Unlock()
		q.callback(item)
		return
	}
	q.items = append(q.items, item)
	q.enqueued++
	q.cond.Signal()
	q.mu.Unlock()
}

// flush waits for all callbacks enqueued before the call to finish. Callbacks
// enqueued after flush starts are intentionally not part of this barrier.
func (q *evictionQueue[V]) flush() {
	if q == nil {
		return
	}

	q.mu.Lock()
	target := q.enqueued
	for q.completed < target {
		q.cond.Wait()
	}
	q.mu.Unlock()
}

func (q *evictionQueue[V]) close() {
	if q == nil {
		return
	}

	q.mu.Lock()
	q.closed = true
	q.cond.Broadcast()
	q.mu.Unlock()
	<-q.done
}

func (q *evictionQueue[V]) run() {
	defer close(q.done)

	for {
		q.mu.Lock()
		for q.head == len(q.items) && !q.closed {
			q.cond.Wait()
		}
		if q.head == len(q.items) {
			q.mu.Unlock()
			return
		}

		item := q.items[q.head]
		q.mu.Unlock()

		q.callback(item)

		q.mu.Lock()
		q.items[q.head] = nil
		q.head++
		q.completed++
		switch {
		case q.head == len(q.items):
			q.items = nil
			q.head = 0
		case q.head >= 1024 && q.head*2 >= len(q.items):
			n := copy(q.items, q.items[q.head:])
			clear(q.items[n:])
			q.items = q.items[:n]
			q.head = 0
		}
		q.cond.Broadcast()
		q.mu.Unlock()
	}
}
