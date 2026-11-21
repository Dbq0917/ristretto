/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package ristretto

import (
	"container/heap"
	"sync"
	"time"
)

var (
	// TODO: find the optimal value or make it configurable.
	bucketDurationSecs = int64(5)
)

func storageBucket(t time.Time) int64 {
	return (t.Unix() / bucketDurationSecs) + 1
}

func cleanupBucket(t time.Time) int64 {
	// The bucket to cleanup is always behind the storage bucket by one so that
	// no elements in that bucket (which might not have expired yet) are deleted.
	return storageBucket(t) - 1
}

// bucket type is a map of key to conflict.
type bucket map[uint64]uint64

// expirationEntry is an entry in the expiration queue. The queue is ordered by
// expiration time so capacity-driven cleanup can remove only keys that are
// already dead instead of sampling or scanning the whole cache.
type expirationEntry struct {
	key        uint64
	conflict   uint64
	expiration time.Time
	index      int
}

type expirationQueue []*expirationEntry

func (q expirationQueue) Len() int {
	return len(q)
}

func (q expirationQueue) Less(i, j int) bool {
	if q[i].expiration.Equal(q[j].expiration) {
		return q[i].key < q[j].key
	}
	return q[i].expiration.Before(q[j].expiration)
}

func (q expirationQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}

func (q *expirationQueue) Push(x any) {
	entry := x.(*expirationEntry)
	entry.index = len(*q)
	*q = append(*q, entry)
}

func (q *expirationQueue) Pop() any {
	old := *q
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil
	entry.index = -1
	*q = old[:n-1]
	return entry
}

// expirationMap is a map of bucket number to the corresponding bucket.
type expirationMap[V any] struct {
	sync.RWMutex
	buckets              map[int64]bucket
	queue                expirationQueue
	queueIndex           map[uint64]*expirationEntry
	lastCleanedBucketNum int64
}

func newExpirationMap[V any]() *expirationMap[V] {
	return &expirationMap[V]{
		buckets:              make(map[int64]bucket),
		queueIndex:           make(map[uint64]*expirationEntry),
		lastCleanedBucketNum: cleanupBucket(time.Now()),
	}
}

func (m *expirationMap[_]) add(key, conflict uint64, expiration time.Time) {
	if m == nil {
		return
	}

	// Items that don't expire don't need to be in the expiration map.
	if expiration.IsZero() {
		return
	}

	bucketNum := storageBucket(expiration)
	m.Lock()
	defer m.Unlock()

	b, ok := m.buckets[bucketNum]
	if !ok {
		b = make(bucket)
		m.buckets[bucketNum] = b
	}
	b[key] = conflict
	m.upsertQueueLocked(key, conflict, expiration)
}

func (m *expirationMap[_]) update(key, conflict uint64, oldExpTime, newExpTime time.Time) {
	if m == nil {
		return
	}

	m.Lock()
	defer m.Unlock()

	oldBucketNum := storageBucket(oldExpTime)
	if oldBucket, ok := m.buckets[oldBucketNum]; ok {
		delete(oldBucket, key)
	}

	// Items that don't expire don't need to be in the expiration map.
	if newExpTime.IsZero() {
		m.removeQueueLocked(key, conflict, oldExpTime)
		return
	}

	newBucketNum := storageBucket(newExpTime)
	newBucket, ok := m.buckets[newBucketNum]
	if !ok {
		newBucket = make(bucket)
		m.buckets[newBucketNum] = newBucket
	}
	newBucket[key] = conflict
	m.upsertQueueLocked(key, conflict, newExpTime)
}

func (m *expirationMap[_]) del(key, conflict uint64, expiration time.Time) {
	if m == nil || expiration.IsZero() {
		return
	}

	bucketNum := storageBucket(expiration)
	m.Lock()
	defer m.Unlock()
	if b, ok := m.buckets[bucketNum]; ok {
		if storedConflict, exists := b[key]; exists && storedConflict == conflict {
			delete(b, key)
		}
	}
	m.removeQueueLocked(key, conflict, expiration)
}

// upsertQueueLocked adds or refreshes the queue entry for a key. m must be
// locked for writing.
func (m *expirationMap[V]) upsertQueueLocked(key, conflict uint64, expiration time.Time) {
	if entry, ok := m.queueIndex[key]; ok {
		entry.conflict = conflict
		entry.expiration = expiration
		heap.Fix(&m.queue, entry.index)
		return
	}
	entry := &expirationEntry{
		key:        key,
		conflict:   conflict,
		expiration: expiration,
	}
	heap.Push(&m.queue, entry)
	m.queueIndex[key] = entry
}

// removeQueueLocked removes the queue entry only when it still describes the
// expiration being deleted. A SetWithTTL that refreshes a key updates the
// entry in place, so a stale cleanup cannot remove the refreshed deadline.
func (m *expirationMap[V]) removeQueueLocked(key, conflict uint64, expiration time.Time) {
	entry, ok := m.queueIndex[key]
	if !ok || entry.conflict != conflict || !entry.expiration.Equal(expiration) {
		return
	}
	heap.Remove(&m.queue, entry.index)
	delete(m.queueIndex, key)
}

// popExpiredLocked returns the earliest expired entry, if any. m must be
// locked for writing.
func (m *expirationMap[V]) popExpiredLocked(now time.Time) (*expirationEntry, bool) {
	if len(m.queue) == 0 || m.queue[0].expiration.After(now) {
		return nil, false
	}
	entry := heap.Pop(&m.queue).(*expirationEntry)
	delete(m.queueIndex, entry.key)
	return entry, true
}

// cleanupExpired removes only entries whose exact expiration has elapsed. It
// is called when a write needs capacity, which prevents expired entries from
// occupying room until the periodic bucket sweep runs. The queue orders keys
// by deadline and the loop stops once targetFree is available, so a single
// write never scans the live cache or drains more than the required backlog.
func (m *expirationMap[V]) cleanupExpired(
	store store[V],
	policy *defaultPolicy[V],
	onEvict func(item *Item[V]),
	now time.Time,
	targetFree int64,
) int {
	if m == nil {
		return 0
	}

	cleaned := 0
	for {
		if policy.Cap() >= targetFree {
			break
		}
		m.Lock()
		entry, ok := m.popExpiredLocked(now)
		m.Unlock()
		if !ok {
			break
		}

		item, removed := store.DelExpired(entry.key, entry.conflict, entry.expiration)
		// The store delete is conditional on the expiration version. If the
		// key was refreshed concurrently, del leaves the newer queue entry and
		// bucket intact.
		m.del(entry.key, entry.conflict, entry.expiration)
		if !removed {
			continue
		}

		item.Cost = policy.Expire(entry.key)
		cleaned++
		if onEvict != nil {
			onEvict(item)
		}
	}
	return cleaned
}

// cleanup removes all the items in the bucket that was just completed. It deletes
// those items from the store, and calls the onEvict function on those items.
// This function is meant to be called periodically.
func (m *expirationMap[V]) cleanup(store store[V], policy *defaultPolicy[V], onEvict func(item *Item[V])) int {
	if m == nil {
		return 0
	}

	m.Lock()
	now := time.Now()
	currentBucketNum := cleanupBucket(now)
	// Clean up all buckets up to and including currentBucketNum, starting from
	// (but not including) the last one that was cleaned up
	var buckets []bucket
	for bucketNum := m.lastCleanedBucketNum + 1; bucketNum <= currentBucketNum; bucketNum++ {
		// With an empty bucket, we don't need to add it to the Clean list
		if b := m.buckets[bucketNum]; b != nil {
			buckets = append(buckets, b)
		}
		delete(m.buckets, bucketNum)
	}
	m.lastCleanedBucketNum = currentBucketNum
	m.Unlock()

	for _, keys := range buckets {
		for key, conflict := range keys {
			expr := store.Expiration(key)
			// Sanity check. Verify that the store agrees that this key is expired.
			if expr.IsZero() || expr.After(now) {
				continue
			}

			item, removed := store.DelExpired(key, conflict, expr)
			m.del(key, conflict, expr)
			if !removed {
				continue
			}

			item.Cost = policy.Expire(key)
			if onEvict != nil {
				onEvict(item)
			}
		}
	}

	cleanedBucketsCount := len(buckets)

	return cleanedBucketsCount
}

// clear clears the expirationMap, the caller is responsible for properly
// evicting the referenced items
func (m *expirationMap[V]) clear() {
	if m == nil {
		return
	}

	m.Lock()
	m.buckets = make(map[int64]bucket)
	m.queue = nil
	m.queueIndex = make(map[uint64]*expirationEntry)
	m.lastCleanedBucketNum = cleanupBucket(time.Now())
	m.Unlock()
}
