package opencensus

import (
	"go.opencensus.io/stats"
)

// SumConfig configures a SumAggregator, pairing the shared Config with
// the measures used to record the accumulated sum and count.
type SumConfig[K comparable] struct {
	Config[K]
	SumMeasure *stats.Float64Measure
}

type sumCountAcc struct {
	sum float64
}

// SumAggregator accumulates the running sum per key K across
// sharded stores and flushes both to OpenCensus on the configured interval.
type SumAggregator[K comparable] struct {
	store   *shardedStore[K, sumCountAcc]
	flusher *flusher
	ctx     *ctxCache[K]

	sumMeasure *stats.Float64Measure
}

// NewSumAggregator builds a SumCountAggregator from cfg, applying defaults
// and starting the background flusher.
func NewSumAggregator[K comparable](cfg SumConfig[K]) *SumAggregator[K] {
	cfg.applyDefaults()
	a := &SumAggregator[K]{
		store:      newStore[K, sumCountAcc](cfg.Shards, cfg.Schema),
		ctx:        newCtxCache[K](cfg.Schema),
		sumMeasure: cfg.SumMeasure,
	}
	a.flusher = startFlusher(cfg.Interval, a.flush)
	return a
}

// Add adds value to the running sum for k.
func (a *SumAggregator[K]) Add(k K, value float64) {
	sh := a.store.shardFor(k)
	sh.mu.Lock()
	acc := sh.m[k]
	if acc == nil {
		acc = &sumCountAcc{}
		sh.m[k] = acc
	}
	acc.sum += value
	sh.mu.Unlock()
}

func (a *SumAggregator[K]) flush() {
	a.store.drainEach(func(k K, acc *sumCountAcc) {
		ctx, err := a.ctx.contextFor(k)
		if err != nil {
			return
		}
		stats.Record(ctx,
			a.sumMeasure.M(acc.sum),
		)
	})
}

// Stop halts the background flusher.
func (a *SumAggregator[K]) Stop() { a.flusher.stop() }
