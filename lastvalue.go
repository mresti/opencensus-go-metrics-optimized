package opencensus

// Generic LastValue variant: for gauges (instantaneous value) whose view in
// OpenCensus uses view.LastValue().
//
// Pre-aggregating to last-value is SEMANTICALLY LOSSLESS: a LastValue() view only
// keeps the last value per key anyway.

import (
	"go.opencensus.io/stats"
)

// LastValueConfig configures a LastValueAggregator: it embeds the shared Config
// and the measure whose view must be of type view.LastValue().
type LastValueConfig[K comparable] struct {
	Config[K]
	// Measure must be backed by a view of type view.LastValue().
	Measure *stats.Float64Measure
}

type lastValueAcc struct {
	value float64
}

// LastValueAggregator keeps the last value recorded per key K across sharded
// stores and flushes it to OpenCensus on the configured interval.
type LastValueAggregator[K comparable] struct {
	store   *shardedStore[K, lastValueAcc]
	flusher *flusher
	ctx     *ctxCache[K]

	measure *stats.Float64Measure
}

// NewLastValueAggregator builds a LastValueAggregator from cfg, applying defaults
// and starting the background flusher.
func NewLastValueAggregator[K comparable](cfg LastValueConfig[K]) *LastValueAggregator[K] {
	cfg.applyDefaults()
	a := &LastValueAggregator[K]{
		store:   newStore[K, lastValueAcc](cfg.Shards, cfg.Schema),
		ctx:     newCtxCache[K](cfg.Schema),
		measure: cfg.Measure,
	}
	a.flusher = startFlusher(cfg.Interval, a.flush)
	return a
}

// Add overwrites the value of the key. The shard lock serializes the writes:
// "last-write-wins" is well defined by the acquisition order.
func (a *LastValueAggregator[K]) Add(k K, value float64) {
	sh := a.store.shardFor(k)
	sh.mu.Lock()
	acc := sh.m[k]
	if acc == nil {
		acc = &lastValueAcc{}
		sh.m[k] = acc
	}
	acc.value = value
	sh.mu.Unlock()
}

func (a *LastValueAggregator[K]) flush() {
	a.store.drainEach(func(k K, acc *lastValueAcc) {
		ctx, err := a.ctx.contextFor(k)
		if err != nil {
			return
		}
		stats.Record(ctx, a.measure.M(acc.value))
	})
}

// Stop halts the background flusher.
func (a *LastValueAggregator[K]) Stop() { a.flusher.stop() }
