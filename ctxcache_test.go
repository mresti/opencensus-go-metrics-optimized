package opencensus

// Tests for the bounded-LRU ctxCache: the default capacity applied by Config, the
// capacity bound, LRU eviction order, and that an evicted key rebuilds its context on
// the next lookup. All direct, deterministic calls — no flusher, no sleeps.

import (
	"context"
	"strconv"
	"testing"
)

func ctxKey(i int) HTTPLabels {
	return HTTPLabels{User: "u" + strconv.Itoa(i), Route: "/r", Status: "200"}
}

func mustCtx(t *testing.T, c *ctxCache[HTTPLabels], k HTTPLabels) context.Context {
	t.Helper()
	ctx, err := c.contextFor(k)
	if err != nil {
		t.Fatalf("contextFor(%v): %v", k, err)
	}
	if ctx == nil {
		t.Fatalf("contextFor(%v) returned a nil context", k)
	}
	return ctx
}

func inCache[K comparable](c *ctxCache[K], k K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.items[k]
	return ok
}

func TestConfigAppliesDefaultCtxCacheSize(t *testing.T) {
	for _, in := range []int{0, -1, -1000} {
		cfg := Config[HTTPLabels]{CtxCacheSize: in}
		cfg.applyDefaults()
		if cfg.CtxCacheSize != defaultCtxCacheSize {
			t.Errorf(
				"CtxCacheSize=%d after applyDefaults(input=%d); want %d",
				cfg.CtxCacheSize,
				in,
				defaultCtxCacheSize,
			)
		}
	}

	cfg := Config[HTTPLabels]{CtxCacheSize: 5}
	cfg.applyDefaults()
	if cfg.CtxCacheSize != 5 {
		t.Errorf("positive CtxCacheSize overwritten: got %d; want 5", cfg.CtxCacheSize)
	}
}

func TestCtxCacheNeverExceedsCapacity(t *testing.T) {
	schema := newSharedSchema(t)
	const capacity = 4
	c := newCtxCache[HTTPLabels](capacity, schema)

	for i := range 50 {
		mustCtx(t, c, ctxKey(i))
		if got := c.ll.Len(); got > capacity {
			t.Fatalf("after %d inserts list len = %d; want <= %d", i+1, got, capacity)
		}
		if got := len(c.items); got > capacity {
			t.Fatalf("after %d inserts map len = %d; want <= %d", i+1, got, capacity)
		}
	}
	if c.ll.Len() != len(c.items) {
		t.Errorf("list/map out of sync: list=%d map=%d", c.ll.Len(), len(c.items))
	}
}

func TestCtxCacheEvictsLeastRecentlyUsed(t *testing.T) {
	schema := newSharedSchema(t)
	c := newCtxCache[HTTPLabels](2, schema)
	k1, k2, k3 := ctxKey(1), ctxKey(2), ctxKey(3)

	mustCtx(t, c, k1)
	mustCtx(t, c, k2)
	mustCtx(t, c, k1) // touch k1 so k2 becomes the coldest entry
	mustCtx(t, c, k3) // over capacity: must evict k2, not k1

	if !inCache(c, k1) {
		t.Error("recently-used k1 evicted; want retained")
	}
	if inCache(c, k2) {
		t.Error("least-recently-used k2 retained; want evicted")
	}
	if !inCache(c, k3) {
		t.Error("newest k3 missing; want retained")
	}
}

func TestCtxCacheHitReturnsCachedInstance(t *testing.T) {
	schema := newSharedSchema(t)
	c := newCtxCache[HTTPLabels](8, schema)
	k := ctxKey(1)

	first := mustCtx(t, c, k)
	if again := mustCtx(t, c, k); again != first {
		t.Error("second lookup rebuilt the context; want the cached instance")
	}
}

func TestCtxCacheEvictedKeyRebuilds(t *testing.T) {
	schema := newSharedSchema(t)
	c := newCtxCache[HTTPLabels](1, schema)
	k1, k2 := ctxKey(1), ctxKey(2)

	first := mustCtx(t, c, k1)
	mustCtx(t, c, k2) // capacity 1: evicts k1
	if inCache(c, k1) {
		t.Fatal("k1 not evicted at capacity 1")
	}

	rebuilt := mustCtx(t, c, k1) // rebuilds k1 (and evicts k2)
	if rebuilt == first {
		t.Error("evicted key returned its original instance; want a freshly built context")
	}
	if !inCache(c, k1) {
		t.Error("k1 missing after rebuild")
	}
}
