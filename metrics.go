// Package opencensus normalizes the use of OpenCensus through local, "sharded"
// aggregation that is GENERIC over the labels key K.
//
// Instead of calling stats.Record per event (each call builds a tag.Map and sends
// a recordReq to the global worker), we accumulate per key K across N shards and
// emit in bursts every `interval`. The key K is any comparable struct you define;
// a Schema[K] (Strategy pattern) projects it onto OpenCensus.
//
// Three variants behind the SAME Aggregator[K, N] interface (SumCount and
// Distribution here; LastValue in lastvalue.go). The hot path (Add) does not
// allocate on the heap after a key is seen for the first time; the flush swaps the
// map to avoid blocking writers and reuses the per-key context via ctxCache.
package opencensus

import (
	"math/rand/v2"
	"sync"
	"time"
)

// Aggregator is the common interface, generic over the labels key K and the
// measure value type N.
type Aggregator[K comparable, N Number] interface {
	Add(k K, value N)
	Stop()
}

type storeShard[K comparable, A any] struct {
	mu sync.Mutex
	m  map[K]*A
}

type shardedStore[K comparable, A any] struct {
	shards []*storeShard[K, A]
	mask   uint64
	schema Schema[K]
}

func newStore[K comparable, A any](shards int, schema Schema[K]) *shardedStore[K, A] {
	n := nextPow2(shards)
	var mask uint64
	if n > 0 {
		mask = uint64(n - 1)
	}
	s := &shardedStore[K, A]{
		shards: make([]*storeShard[K, A], n),
		mask:   mask,
		schema: schema,
	}
	for i := range s.shards {
		s.shards[i] = &storeShard[K, A]{m: make(map[K]*A)}
	}
	return s
}

func (s *shardedStore[K, A]) shardFor(k K) *storeShard[K, A] {
	return s.shards[s.schema.Hash(k)&s.mask]
}

func (s *shardedStore[K, A]) drainEach(fn func(K, *A)) {
	for _, sh := range s.shards {
		sh.mu.Lock()
		old := sh.m
		if len(old) == 0 {
			sh.mu.Unlock()
			continue
		}
		sh.m = make(map[K]*A, len(old))
		sh.mu.Unlock()

		for k, acc := range old {
			fn(k, acc)
		}
	}
}

// flusher is independent of K: it just fires a func.
type flusher struct {
	done chan struct{}
	wg   sync.WaitGroup
}

// startFlusher runs flush every interval on a background goroutine, plus a final
// flush on stop. It applies a random startup delay in [0, interval) so aggregators
// created together don't all flush on the same tick and burst the global
// OpenCensus worker.
func startFlusher(interval time.Duration, flush func()) *flusher {
	return startFlusherJittered(interval, rand.N(interval), flush)
}

// startFlusherJittered is the seam used by tests: it takes the startup jitter
// explicitly instead of drawing it randomly.
func startFlusherJittered(interval, jitter time.Duration, flush func()) *flusher {
	f := &flusher{done: make(chan struct{})}
	f.wg.Go(func() {
		if !f.waitJitter(jitter) {
			flush()
			return
		}
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				flush()
			case <-f.done:
				flush()
				return
			}
		}
	})
	return f
}

// waitJitter blocks for the startup delay, returning false if stop() was called
// during the wait so the caller can run the final flush and exit.
func (f *flusher) waitJitter(jitter time.Duration) bool {
	if jitter <= 0 {
		return true
	}
	timer := time.NewTimer(jitter)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-f.done:
		return false
	}
}

func (f *flusher) stop() {
	close(f.done)
	f.wg.Wait()
}

func nextPow2(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}
