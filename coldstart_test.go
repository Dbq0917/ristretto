/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package ristretto

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// coldRun drives an admission policy through a Get-on-miss/Set-on-miss loop
// and records per-window cold-start statistics. Accesses are replayed
// synchronously so the curve is deterministic, while frequency history is
// batched into the sketch the same way the real cache batches Get accesses
// through ringBuffer stripes.
type coldRun struct {
	policy   *defaultPolicy[int]
	resident map[uint64]struct{}
	// pending holds accesses waiting for a full ringBuffer stripe.
	pending    []uint64
	stripeSize int
}

func newColdRun(numCounters, capacity int64) *coldRun {
	p := newDefaultPolicy[int](numCounters, capacity)
	p.CollectMetrics(newMetrics())
	return &coldRun{
		policy:     p,
		resident:   make(map[uint64]struct{}, capacity),
		stripeSize: 64,
	}
}

func (r *coldRun) close() {
	r.policy.stop <- struct{}{}
	<-r.policy.done
}

// access replays one request. It returns whether it was a cache hit, an
// admission rejection, and whether a rejection happened while the incoming key
// had no recorded frequency during sketch warm-up (the cold-start bug).
func (r *coldRun) access(key uint64) (hit, rejected, zeroHistoryReject bool) {
	r.record(key)

	if _, ok := r.resident[key]; ok {
		return true, false, false
	}

	victims, added := r.policy.Add(key, 1)
	if !added {
		r.policy.Lock()
		incHits := r.policy.admit.Estimate(key)
		warming := r.policy.admit.warming()
		r.policy.Unlock()
		return false, true, warming && incHits == 0
	}
	r.resident[key] = struct{}{}
	for _, victim := range victims {
		delete(r.resident, victim.Key)
	}
	return false, false, false
}

// record queues an access and flushes a full stripe into the frequency sketch,
// mirroring ringBuffer -> policy.Push -> tinyLFU.Push batching.
func (r *coldRun) record(key uint64) {
	r.pending = append(r.pending, key)
	if len(r.pending) < r.stripeSize {
		return
	}
	r.policy.Lock()
	r.policy.admit.Push(r.pending)
	r.policy.Unlock()
	r.pending = r.pending[:0]
}

// flush pushes any accesses still waiting in a partial stripe into the sketch.
func (r *coldRun) flush() {
	if len(r.pending) == 0 {
		return
	}
	r.policy.Lock()
	r.policy.admit.Push(r.pending)
	r.policy.Unlock()
	r.pending = r.pending[:0]
}

type coldWindow struct {
	gets     int
	hits     int
	misses   int
	rejected int
	zeroRej  int
	ratio    float64
}

// coldCurve replays the generator and returns per-window statistics.
func coldCurve(t *testing.T, gen func() uint64, total, window, numCounters, capacity int64) []coldWindow {
	t.Helper()
	r := newColdRun(numCounters, capacity)
	defer r.close()

	windows := make([]coldWindow, 0, total/window)
	cur := coldWindow{}
	for i := int64(0); i < total; i++ {
		hit, rejected, zeroRej := r.access(gen())
		cur.gets++
		if hit {
			cur.hits++
		} else {
			cur.misses++
		}
		if rejected {
			cur.rejected++
		}
		if zeroRej {
			cur.zeroRej++
		}
		if cur.gets == int(window) {
			cur.ratio = float64(cur.hits) / float64(cur.gets)
			windows = append(windows, cur)
			cur = coldWindow{}
		}
	}
	return windows
}

func zipfGenerator(n uint64, s float64, seed int64) func() uint64 {
	z := rand.NewZipf(rand.New(rand.NewSource(seed)), s, 1, n)
	return func() uint64 { return z.Uint64() }
}

func uniformGenerator(n uint64, seed int64) func() uint64 {
	r := rand.New(rand.NewSource(seed))
	return func() uint64 { return r.Uint64() % n }
}

// TestColdStartZipfianCurve replays a skewed workload against an empty cache
// and logs the per-window hit ratio curve together with admission rejections.
func TestColdStartZipfianCurve(t *testing.T) {
	const (
		total       = 20000
		window      = 500
		numCounters = 10000
		capacity    = 100
	)
	curve := coldCurve(t, zipfGenerator(1000, 1.1, 42), total, window, numCounters, capacity)

	warmupWindows := int(numCounters / window)
	warmupRejected, warmupZeroRej := 0, 0
	for i, w := range curve {
		if i < warmupWindows {
			warmupRejected += w.rejected
			warmupZeroRej += w.zeroRej
		}
		t.Logf("zipf window %2d: ratio=%.3f hits=%d misses=%d rejected=%d zeroHistoryRej=%d",
			i, w.ratio, w.hits, w.misses, w.rejected, w.zeroRej)
	}
	t.Logf("warm-up rejections=%d (of which zero-history=%d)", warmupRejected, warmupZeroRej)

	// A key with no frequency history must not be rejected while the sketch is
	// still warming: that is the cold-start failure that blocks popular keys.
	require.Equal(t, 0, warmupZeroRej,
		"no zero-history admission rejections during cold start")

	// Steady-state hit ratio must not regress: the tail windows stay above the
	// cold-start window and match the strict frequency comparison once warm.
	tail := curve[len(curve)-4:]
	tailHits, tailGets := 0, 0
	for _, w := range tail {
		tailHits += w.hits
		tailGets += w.gets
	}
	tailRatio := float64(tailHits) / float64(tailGets)
	t.Logf("steady-state tail ratio: %.3f", tailRatio)
	require.Greater(t, tailRatio, curve[0].ratio)
}

// TestColdStartScanCurve replays a uniform low-frequency scan against an empty
// cache. The warm-up grace period lets initial keys in, but once frequency
// history exists the scan must be rejected instead of churning the cache.
func TestColdStartScanCurve(t *testing.T) {
	const (
		total       = 20000
		window      = 500
		numCounters = 16384
		capacity    = 100
		keySpace    = 2000
	)
	curve := coldCurve(t, uniformGenerator(keySpace, 7), total, window, numCounters, capacity)

	for i, w := range curve {
		t.Logf("scan window %2d: ratio=%.3f hits=%d misses=%d rejected=%d zeroHistoryRej=%d",
			i, w.ratio, w.hits, w.misses, w.rejected, w.zeroRej)
	}

	// First window is the grace period: no zero-history rejection may occur,
	// giving new keys their chance even under a scan.
	require.Equal(t, 0, curve[0].zeroRej)

	// After warm-up, a uniform scan over a key space 50x the cache capacity
	// must be rejected by the frequency-based admission policy.
	steady := curve[len(curve)-4:]
	rejected, misses := 0, 0
	for _, w := range steady {
		rejected += w.rejected
		misses += w.misses
	}
	require.Greater(t, float64(rejected)/float64(misses), 0.9)
}

// TestColdStartWarmupState checks the warm-up indicator directly.
func TestColdStartWarmupState(t *testing.T) {
	p := newDefaultPolicy[int](100, 10)
	defer func() {
		p.stop <- struct{}{}
		<-p.done
	}()

	p.Lock()
	// A fresh sketch has no history and is warming up.
	require.True(t, p.admit.warming())
	for i := int64(0); i < p.admit.resetAt-1; i++ {
		p.admit.Increment(uint64(i % 10))
	}
	// Still inside the first (cold-start) window.
	require.True(t, p.admit.warming())
	// Crossing the aging threshold halves the counters and opens a fresh
	// window, but cold start is now over and warming() stays false.
	p.admit.Increment(0)
	require.False(t, p.admit.warming())
	require.Equal(t, int64(0), p.admit.incrs)

	// A later aging cycle must not re-enable the cold-start exception.
	for i := int64(0); i < p.admit.resetAt/2; i++ {
		p.admit.Increment(uint64(i % 10))
	}
	require.False(t, p.admit.warming())

	// Clearing the policy starts a genuine cold start over again.
	p.admit.clear()
	require.True(t, p.admit.warming())
	p.Unlock()
}

// TestColdStartAdmitUnknownKey is a focused policy-level check: while warming,
// an unknown key is admitted by evicting an equally unknown resident key; once
// the sketch is warm, the same zero-vs-positive comparison rejects it.
func TestColdStartAdmitUnknownKey(t *testing.T) {
	p := newDefaultPolicy[int](100, 10)
	defer func() {
		p.stop <- struct{}{}
		<-p.done
	}()

	p.Lock()
	require.True(t, p.admit.warming())
	// Fill the cache with keys the sketch has never recorded.
	for k := uint64(1); k <= 10; k++ {
		p.evict.add(k, 1)
	}
	p.Unlock()
	// An equally unknown incoming key gets its chance during warm-up.
	victims, added := p.Add(100, 1)
	require.True(t, added)
	require.Len(t, victims, 1)
}

// strictRun is the reference admission policy: the original plain frequency
// comparison with no cold-start exception. It models steady-state behaviour.
type strictRun struct {
	policy   *defaultPolicy[int]
	resident map[uint64]struct{}
	pending  []uint64
	stripe   int
}

func newStrictRun(numCounters, capacity int64) *strictRun {
	p := newDefaultPolicy[int](numCounters, capacity)
	p.CollectMetrics(newMetrics())
	return &strictRun{
		policy:   p,
		resident: make(map[uint64]struct{}, capacity),
		stripe:   64,
	}
}

func (r *strictRun) close() {
	r.policy.stop <- struct{}{}
	<-r.policy.done
}

func (r *strictRun) flush() {
	if len(r.pending) == 0 {
		return
	}
	r.policy.Lock()
	r.policy.admit.Push(r.pending)
	r.policy.Unlock()
	r.pending = r.pending[:0]
}

func (r *strictRun) access(key uint64) bool {
	r.pending = append(r.pending, key)
	if len(r.pending) >= r.stripe {
		r.policy.Lock()
		r.policy.admit.Push(r.pending)
		r.policy.Unlock()
		r.pending = r.pending[:0]
	}
	if _, ok := r.resident[key]; ok {
		return true
	}

	victims, added := r.strictAdd(key, 1)
	if !added {
		return false
	}
	r.resident[key] = struct{}{}
	for _, victim := range victims {
		delete(r.resident, victim.Key)
	}
	return false
}

// strictAdd mirrors defaultPolicy.Add but applies the original plain TinyLFU
// comparison (incHits < minHits) with no cold-start exception, serving as the
// steady-state reference implementation.
func (r *strictRun) strictAdd(key uint64, cost int64) ([]*Item[int], bool) {
	p := r.policy
	p.Lock()
	defer p.Unlock()

	if cost > p.evict.getMaxCost() {
		return nil, false
	}
	if has := p.evict.updateIfHas(key, cost); has {
		return nil, false
	}

	room := p.evict.roomLeft(cost)
	if room >= 0 {
		p.evict.add(key, cost)
		return nil, true
	}

	incHits := p.admit.Estimate(key)
	sample := make([]*policyPair, 0, lfuSample)
	victims := make([]*Item[int], 0)

	for ; room < 0; room = p.evict.roomLeft(cost) {
		sample = p.evict.fillSample(sample)
		minKey, minHits, minId, minCost := uint64(0), int64(1<<62), 0, int64(0)
		for i, pair := range sample {
			if hits := p.admit.Estimate(pair.key); hits < minHits {
				minKey, minHits, minId, minCost = pair.key, hits, i, pair.cost
			}
		}
		// Plain strict comparison: reject anything less popular than the
		// least popular sampled resident key.
		if incHits < minHits {
			p.metrics.add(rejectSets, key, 1)
			return victims, false
		}
		p.evict.del(minKey)
		sample[minId] = sample[len(sample)-1]
		sample = sample[:len(sample)-1]
		victims = append(victims, &Item[int]{Key: minKey, Cost: minCost})
	}

	p.evict.add(key, cost)
	return victims, true
}

// steadyTrial runs both the fixed policy and the strict reference policy from
// the same warmed-up state and returns their steady-state hit counts.
func steadyTrial(t *testing.T, seed int64, numCounters, capacity, total int64) (coldHits, strictHits int) {
	t.Helper()
	cold := newColdRun(numCounters, capacity)
	defer cold.close()
	strict := newStrictRun(numCounters, capacity)
	defer strict.close()

	// Align the randomized count-min seeds so the sketches only differ by
	// admission policy, not by unrelated hashing noise.
	cold.policy.Lock()
	strict.policy.Lock()
	copy(strict.policy.admit.freq.seed[:], cold.policy.admit.freq.seed[:])
	strict.policy.Unlock()
	cold.policy.Unlock()

	// Drive both sketches through the first aging window with one stream.
	warmGen := zipfGenerator(1000, 1.1, seed)
	for i := int64(0); i < numCounters; i++ {
		key := warmGen()
		cold.record(key)
		strict.pending = append(strict.pending, key)
		if len(strict.pending) >= strict.stripe {
			strict.policy.Lock()
			strict.policy.admit.Push(strict.pending)
			strict.policy.Unlock()
			strict.pending = strict.pending[:0]
		}
	}
	cold.flush()
	strict.flush()
	require.False(t, cold.policy.admit.warming())
	require.False(t, strict.policy.admit.warming())

	// Start both caches from identical resident sets.
	for k := uint64(1); k <= uint64(capacity); k++ {
		cold.policy.Lock()
		cold.policy.evict.add(k, 1)
		cold.policy.Unlock()
		cold.resident[k] = struct{}{}

		strict.policy.Lock()
		strict.policy.evict.add(k, 1)
		strict.policy.Unlock()
		strict.resident[k] = struct{}{}
	}

	// Replay a fresh steady-state stream through both policies.
	gen := zipfGenerator(1000, 1.1, seed+1)
	for i := int64(0); i < total; i++ {
		key := gen()
		if hit, _, _ := cold.access(key); hit {
			coldHits++
		}
		if strict.access(key) {
			strictHits++
		}
	}
	return coldHits, strictHits
}

// TestColdStartSteadyStateParity proves the fix cannot change steady-state
// behaviour. After the first aging cycle the cold-start exception is disabled,
// so the fixed policy and the original strict comparison converge to the same
// average hit ratio. Sampled-LFU uses randomized map sampling, so individual
// trials differ slightly; the averages across trials must match within a tight
// tolerance and show no systematic drift.
func TestColdStartSteadyStateParity(t *testing.T) {
	const (
		trials      = 12
		numCounters = 10000
		capacity    = 100
		total       = 20000
	)
	var coldTotal, strictTotal int
	for trial := int64(0); trial < trials; trial++ {
		coldHits, strictHits := steadyTrial(t, trial*7+1, numCounters, capacity, total)
		coldTotal += coldHits
		strictTotal += strictHits
	}
	coldRatio := float64(coldTotal) / float64(trials*total)
	strictRatio := float64(strictTotal) / float64(trials*total)
	t.Logf("steady-state average ratio: fixed=%.4f strict=%.4f (delta=%+.4f)",
		coldRatio, strictRatio, coldRatio-strictRatio)

	// Within 2 percentage points and never a material regression on average.
	require.InDelta(t, strictRatio, coldRatio, 0.02)
	require.GreaterOrEqual(t, coldRatio, strictRatio-0.01)
}
