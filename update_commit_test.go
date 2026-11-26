package ristretto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUpdateCommitRollbackWhenSetBufFull(t *testing.T) {
	originalSetBufSize := setBufSize
	setBufSize = 1
	defer func() { setBufSize = originalSetBufSize }()

	c, err := NewCache(&Config[int, string]{
		NumCounters:        100,
		MaxCost:            100,
		BufferItems:        64,
		IgnoreInternalCost: true,
		Metrics:            true,
	})
	require.NoError(t, err)

	require.True(t, c.SetWithTTL(1, "old", 7, time.Hour))
	c.Wait()

	keyHash, _ := c.keyToHash(1)
	oldExpiration := c.storedItems.Expiration(keyHash)

	c.stop <- struct{}{}
	<-c.done
	c.setBuf <- &Item[string]{}

	completed := make(chan bool, 1)
	go func() {
		completed <- c.SetWithTTL(1, "new", 9, 2*time.Hour)
	}()

	select {
	case accepted := <-completed:
		require.False(t, accepted)
	case <-time.After(time.Second):
		t.Fatal("update blocked while setBuf was full")
	}

	value, ok := c.Get(1)
	require.True(t, ok)
	require.Equal(t, "old", value)
	require.Equal(t, int64(7), c.cachePolicy.Cost(keyHash))
	require.Equal(t, oldExpiration, c.storedItems.Expiration(keyHash))

	expiryMap := c.storedItems.(*shardedMap[string]).expiryMap
	expiryMap.RLock()
	_, oldBucketHasKey := expiryMap.buckets[storageBucket(oldExpiration)][keyHash]
	bucketsContainingKey := 0
	for _, bucket := range expiryMap.buckets {
		if _, ok := bucket[keyHash]; ok {
			bucketsContainingKey++
		}
	}
	expiryMap.RUnlock()
	require.True(t, oldBucketHasKey)
	require.Equal(t, 1, bucketsContainingKey)
	require.Equal(t, uint64(1), c.Metrics.SetsDropped())

	<-c.setBuf
	go c.processItems()
	c.Close()
}
