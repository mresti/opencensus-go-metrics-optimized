package opencensus

import (
	"go.opencensus.io/stats"
)

// CountConfig configures a SumCountAggregator, pairing the shared Config with
// the measures used to record the accumulated count.
type CountConfig[K comparable, N Number] struct {
	Config[K]
	CountMeasure Measure[N]
}

type countAcc struct {
	count int64
}

// CountAggregator accumulates the running count per key K across
// sharded stores and flushes both to OpenCensus on the configured interval.
type CountAggregator[K comparable, N Number] struct {
	store   *shardedStore[K, countAcc]
	flusher *flusher
	ctx     *ctxCache[K]

	countMeasure Measure[N]
}

// NewCountAggregator builds a CountAggregator from cfg, applying defaults
// and starting the background flusher.
func NewCountAggregator[K comparable, N Number](cfg CountConfig[K, N]) *CountAggregator[K, N] {
	cfg.applyDefaults()
	a := &CountAggregator[K, N]{
		store:        newStore[K, countAcc](cfg.Shards, cfg.Schema),
		ctx:          newCtxCache[K](cfg.CtxCacheSize, cfg.Schema),
		countMeasure: cfg.CountMeasure,
	}
	a.flusher = startFlusher(cfg.Interval, a.flush)
	return a
}

// Add adds value to the running increments its count.
func (a *CountAggregator[K, N]) Add(k K, _ N) {
	sh := a.store.shardFor(k)
	sh.mu.Lock()
	acc := sh.m[k]
	if acc == nil {
		acc = &countAcc{}
		sh.m[k] = acc
	}
	acc.count++
	sh.mu.Unlock()
}

func (a *CountAggregator[K, N]) flush() {
	a.store.drainEach(func(k K, acc *countAcc) {
		ctx, err := a.ctx.contextFor(k)
		if err != nil {
			return
		}
		stats.Record(ctx,
			a.countMeasure.M(N(acc.count)),
		)
	})
}

// Stop halts the background flusher.
func (a *CountAggregator[K, N]) Stop() { a.flusher.stop() }
