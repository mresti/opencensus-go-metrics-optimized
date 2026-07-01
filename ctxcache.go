package opencensus

// ctxCache memoizes the context.Context with tags already applied per key K, to
// avoid rebuilding it (tag.New) on every flush. The context derived from Background
// with a tag.Map is immutable and has no deadline/cancellation: safe to retain and
// for the OpenCensus worker to read in parallel.
//
// TRADE-OFF: trades CPU/allocations for RAM. It grows with the number of DISTINCT
// keys seen. With bounded cardinality it is stable; with unbounded labels it is
// advisable to cap it (LRU) or clear it periodically.

import (
	"context"
	"sync"

	"go.opencensus.io/tag"
)

type ctxCache[K comparable] struct {
	mu     sync.Mutex
	m      map[K]context.Context
	schema Schema[K]
}

func newCtxCache[K comparable](schema Schema[K]) *ctxCache[K] {
	return &ctxCache[K]{
		m:      make(map[K]context.Context),
		schema: schema,
	}
}

func (c *ctxCache[K]) contextFor(k K) (context.Context, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ctx, ok := c.m[k]; ok {
		return ctx, nil
	}
	ctx, err := tag.New(context.Background(), c.schema.Mutators(k)...)
	if err != nil {
		return nil, err
	}
	c.m[k] = ctx
	return ctx, nil
}
