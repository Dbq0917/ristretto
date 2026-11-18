/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package ristretto

import (
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

// expirationMap is a map of bucket number to the corresponding bucket.
type expirationMap[V any] struct {
	sync.RWMutex
	buckets              map[int64]bucket
	lastCleanedBucketNum int64
}

func newExpirationMap[V any]() *expirationMap[V] {
	return &expirationMap[V]{
		buckets:              make(map[int64]bucket),
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
}

func (m *expirationMap[_]) update(key, conflict uint64, oldExpTime, newExpTime time.Time) {
	if m == nil {
		return
	}

	m.Lock()
	defer m.Unlock()

	oldBucketNum := storageBucket(oldExpTime)
	oldBucket, ok := m.buckets[oldBucketNum]
	if ok {
		delete(oldBucket, key)
	}

	// Items that don't expire don't need to be in the expiration map.
	if newExpTime.IsZero() {
		return
	}

	newBucketNum := storageBucket(newExpTime)
	newBucket, ok := m.buckets[newBucketNum]
	if !ok {
		newBucket = make(bucket)
		m.buckets[newBucketNum] = newBucket
	}
	newBucket[key] = conflict
}

func (m *expirationMap[_]) del(key uint64, expiration time.Time) {
	if m == nil {
		return
	}

	bucketNum := storageBucket(expiration)
	m.Lock()
	defer m.Unlock()
	_, ok := m.buckets[bucketNum]
	if !ok {
		return
	}
	delete(m.buckets[bucketNum], key)
}

// cleanup removes all the items in the bucket that was just completed. It
// deletes those items from the store, and calls the onEvict function on those
// items. This function is meant to be called periodically.
//
// The store is the single source of truth for whether an entry is really gone:
// a key whose expiration was refreshed concurrently moves to a different bucket
// and its conditional delete fails, so cleanup neither removes it nor touches
// its policy accounting. Policy cost is reclaimed only after the store confirms
// the deletion, which keeps the two former ledgers permanently in sync without
// a global write lock.
func (m *expirationMap[V]) cleanup(store store[V], policy policyEvictor[V], onEvict func(item *Item[V])) int {
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
			// Look up the expiration the store currently holds for the key.
			// The conditional delete below re-checks it under the shard lock,
			// so a refresh landing here makes the delete a no-op.
			expiration := store.Expiration(key)
			if expiration.After(now) {
				continue
			}
			item, removed := store.DelExpired(key, conflict, expiration)
			if !removed {
				// The entry was refreshed or deleted concurrently; its
				// accounting is owned by that writer, leave it untouched.
				continue
			}

			if cost := policy.RemoveAccounting(key); cost >= 0 {
				item.Cost = cost
			}
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
	m.lastCleanedBucketNum = cleanupBucket(time.Now())
	m.Unlock()
}
