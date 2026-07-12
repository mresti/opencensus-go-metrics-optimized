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
type LastValueConfig[K comparable, N Number] struct {
	Config[K]
	// Measure must be backed by a view of type view.LastValue().
	Measure Measure[N]
}

type lastValueAcc[N Number] struct {
	value N
}

// LastValueAggregator keeps the last value recorded per key K across sharded
// stores and flushes it to OpenCensus on the configured interval.
type LastValueAggregator[K comparable, N Number] struct {
	store   *shardedStore[K, lastValueAcc[N]]
	flusher *flusher
	ctx     *ctxCache[K]

	measure Measure[N]
}

// NewLastValueAggregator builds a LastValueAggregator from cfg, applying defaults
// and starting the background flusher.
func NewLastValueAggregator[K comparable, N Number](cfg LastValueConfig[K, N]) *LastValueAggregator[K, N] {
	cfg.applyDefaults()
	a := &LastValueAggregator[K, N]{
		store:   newStore[K, lastValueAcc[N]](cfg.Shards, cfg.Schema),
		ctx:     newCtxCache[K](cfg.CtxCacheSize, cfg.Schema),
		measure: cfg.Measure,
	}
	a.flusher = startFlusher(cfg.Interval, a.flush)
	return a
}

// Add overwrites the value of the key. The shard lock serializes the writes:
// "last-write-wins" is well defined by the acquisition order.
func (a *LastValueAggregator[K, N]) Add(k K, value N) {
	sh := a.store.shardFor(k)
	sh.mu.Lock()
	acc := sh.m[k]
	if acc == nil {
		acc = &lastValueAcc[N]{}
		sh.m[k] = acc
	}
	acc.value = value
	sh.mu.Unlock()
}

func (a *LastValueAggregator[K, N]) flush() {
	a.store.drainEach(func(k K, acc *lastValueAcc[N]) {
		ctx, err := a.ctx.contextFor(k)
		if err != nil {
			return
		}
		stats.Record(ctx, a.measure.M(acc.value))
	})
}

// Stop halts the background flusher.
func (a *LastValueAggregator[K, N]) Stop() { a.flusher.stop() }
