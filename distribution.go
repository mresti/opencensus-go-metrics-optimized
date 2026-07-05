package opencensus

import (
	"math/rand/v2"

	"go.opencensus.io/stats"
)

// DistributionConfig configures a DistributionAggregator with the shared Config,
// the measure to record samples against and the optional per-key sample cap.
type DistributionConfig[K comparable, N Number] struct {
	Config[K]
	Measure          Measure[N]
	MaxSamplesPerKey int // 0 = exact; >0 = reservoir sampling (bounded memory)
}

const recordChunk = 128

type distAcc[N Number] struct {
	samples []N
	seen    int64
}

// DistributionAggregator collects per-key samples across sharded stores and
// flushes them to OpenCensus on the configured interval. When MaxSamplesPerKey is
// set it keeps a bounded reservoir sample per key.
type DistributionAggregator[K comparable, N Number] struct {
	store   *shardedStore[K, distAcc[N]]
	flusher *flusher
	ctx     *ctxCache[K]

	measure    Measure[N]
	maxSamples int
}

// NewDistributionAggregator builds a DistributionAggregator from cfg, applying
// defaults and starting the background flusher.
func NewDistributionAggregator[K comparable, N Number](cfg DistributionConfig[K, N]) *DistributionAggregator[K, N] {
	cfg.applyDefaults()
	a := &DistributionAggregator[K, N]{
		store:      newStore[K, distAcc[N]](cfg.Shards, cfg.Schema),
		ctx:        newCtxCache[K](cfg.Schema),
		measure:    cfg.Measure,
		maxSamples: cfg.MaxSamplesPerKey,
	}
	a.flusher = startFlusher(cfg.Interval, a.flush)
	return a
}

// Add records value as a sample for k, using reservoir sampling once the per-key
// sample cap is reached.
func (a *DistributionAggregator[K, N]) Add(k K, value N) {
	sh := a.store.shardFor(k)
	sh.mu.Lock()
	acc := sh.m[k]
	if acc == nil {
		acc = &distAcc[N]{}
		sh.m[k] = acc
	}
	acc.seen++

	if a.maxSamples <= 0 || len(acc.samples) < a.maxSamples {
		acc.samples = append(acc.samples, value)
	} else {
		j := rand.Int64N(acc.seen)
		if j < int64(a.maxSamples) {
			acc.samples[j] = value
		}
	}
	sh.mu.Unlock()
}

func (a *DistributionAggregator[K, N]) flush() {
	a.store.drainEach(func(k K, acc *distAcc[N]) {
		if len(acc.samples) == 0 {
			return
		}
		ctx, err := a.ctx.contextFor(k)
		if err != nil {
			return
		}

		// Each batch is a NEW slice; a buffer cannot be reused:
		// stats.Record enqueues the measurements in the asynchronous OpenCensus
		// worker, which reads them later (rewriting them would be a data race).
		samples := acc.samples
		for start := 0; start < len(samples); start += recordChunk {
			end := min(start+recordChunk, len(samples))
			batch := make([]stats.Measurement, 0, end-start)
			for _, v := range samples[start:end] {
				batch = append(batch, a.measure.M(v))
			}
			stats.Record(ctx, batch...)
		}
	})
}

// Stop halts the background flusher.
func (a *DistributionAggregator[K, N]) Stop() { a.flusher.stop() }
