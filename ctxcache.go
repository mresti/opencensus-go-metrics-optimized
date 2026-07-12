package opencensus

// ctxCache memoizes the context.Context with tags already applied per key K, to
// avoid rebuilding it (tag.New) on every flush. The context derived from Background
// with a tag.Map is immutable and has no deadline/cancellation: safe to retain and
// for the OpenCensus worker to read in parallel.
//
// TRADE-OFF: trades CPU/allocations for RAM. It is a bounded LRU of at most capacity
// entries: on a miss the new context is inserted at the front and the least-recently
// -used entries are evicted from the back until the cache is back within capacity.
// Memory is therefore capped regardless of label cardinality; an evicted key just
// pays one tag.New again the next flush that touches it. capacity is expected to be
// positive (Config.applyDefaults enforces a default); a non-positive capacity would
// evict every entry immediately, degrading to no caching rather than misbehaving.

import (
	"container/list"
	"context"
	"sync"

	"go.opencensus.io/tag"
)

// ctxEntry is the value stored in each list element: the key is kept alongside the
// context so eviction from the back of the list can delete the corresponding map
// entry without a reverse lookup.
type ctxEntry[K comparable] struct {
	key K
	ctx context.Context
}

type ctxCache[K comparable] struct {
	mu       sync.Mutex
	ll       *list.List // front = most-recently-used, back = least-recently-used
	items    map[K]*list.Element
	capacity int
	schema   Schema[K]
}

func newCtxCache[K comparable](capacity int, schema Schema[K]) *ctxCache[K] {
	return &ctxCache[K]{
		ll:       list.New(),
		items:    make(map[K]*list.Element),
		capacity: capacity,
		schema:   schema,
	}
}

func (c *ctxCache[K]) contextFor(k K) (context.Context, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.items[k]; ok {
		c.ll.MoveToFront(el)
		return el.Value.(*ctxEntry[K]).ctx, nil
	}

	ctx, err := tag.New(context.Background(), c.schema.Mutators(k)...)
	if err != nil {
		return nil, err
	}
	c.insertFront(k, ctx)
	c.evictBeyondCapacity()
	return ctx, nil
}

func (c *ctxCache[K]) insertFront(k K, ctx context.Context) {
	c.items[k] = c.ll.PushFront(&ctxEntry[K]{key: k, ctx: ctx})
}

func (c *ctxCache[K]) evictBeyondCapacity() {
	for c.ll.Len() > c.capacity {
		c.removeOldest()
	}
}

func (c *ctxCache[K]) removeOldest() {
	el := c.ll.Back()
	if el == nil {
		return
	}
	c.ll.Remove(el)
	delete(c.items, el.Value.(*ctxEntry[K]).key)
}
