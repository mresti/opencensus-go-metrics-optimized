package opencensus

import (
	"go.opencensus.io/stats"
)

// SumConfig configures a SumAggregator, pairing the shared Config with
// the measures used to record the accumulated sum and count.
type SumConfig[K comparable, N Number] struct {
	Config[K]
	SumMeasure Measure[N]
}

type sumCountAcc[N Number] struct {
	sum N
}

// SumAggregator accumulates the running sum per key K across
// sharded stores and flushes both to OpenCensus on the configured interval.
type SumAggregator[K comparable, N Number] struct {
	store   *shardedStore[K, sumCountAcc[N]]
	flusher *flusher
	ctx     *ctxCache[K]

	sumMeasure Measure[N]
}

// NewSumAggregator builds a SumCountAggregator from cfg, applying defaults
// and starting the background flusher.
func NewSumAggregator[K comparable, N Number](cfg SumConfig[K, N]) *SumAggregator[K, N] {
	cfg.applyDefaults()
	a := &SumAggregator[K, N]{
		store:      newStore[K, sumCountAcc[N]](cfg.Shards, cfg.Schema),
		ctx:        newCtxCache[K](cfg.Schema),
		sumMeasure: cfg.SumMeasure,
	}
	a.flusher = startFlusher(cfg.Interval, a.flush)
	return a
}

// Add adds value to the running sum for k.
func (a *SumAggregator[K, N]) Add(k K, value N) {
	sh := a.store.shardFor(k)
	sh.mu.Lock()
	acc := sh.m[k]
	if acc == nil {
		acc = &sumCountAcc[N]{}
		sh.m[k] = acc
	}
	acc.sum += value
	sh.mu.Unlock()
}

func (a *SumAggregator[K, N]) flush() {
	a.store.drainEach(func(k K, acc *sumCountAcc[N]) {
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
func (a *SumAggregator[K, N]) Stop() { a.flusher.stop() }
