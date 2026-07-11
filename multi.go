package opencensus

// Multi-metric variant: a SINGLE shardedStore + a SINGLE flusher + a SINGLE
// ctxCache shared by up to 64 user-defined metrics of type Count, Sum or
// LastValue over the SAME labels key K.
//
// Motivation: running N independent aggregators for the same key K means N shard
// lookups, N locks and N ctxCache entries per event and per flush. Folding them
// into one accumulator collapses the hot path to one shardFor + one lock + one
// slice write, and the flush to one ctxCache lookup and one stats.Record per key
// carrying every metric at once.
//
// Cardinality: the single shared ctxCache holds one context per distinct key
// regardless of how many metrics are registered, so memory scales with keys, not
// keys×metrics — roughly 1/N the ctxCache footprint of N separate aggregators. Each
// flush emits ONE stats.Record per key (carrying every metric) instead of N, which
// generates N× fewer records for the same key set. This eases the "do not drop
// Interval below ~5s at high cardinality" guidance in Config's doc comment.
//
// Metrics are declared with a builder that hands back lightweight handles:
//
//	b := NewMultiBuilder[HTTPLabels, float64](cfg)
//	reqs  := b.Count(reqMeasure)     // +1 per Add, ignores the value
//	bytes := b.Sum(byteMeasure)      // accumulates the value
//	depth := b.LastValue(gaugeMeasure) // last-write-wins under the shard lock
//	agg := b.Build()                 // applies defaults, starts the flusher
//	reqs.Add(k, 1)
//	bytes.Add(k, 2048)
//	agg.Stop()
//
// Only MultiAggregator owns Stop(); the handles are pure write endpoints and do
// NOT satisfy Aggregator[K, N] (they expose Add but not Stop) by design.

import (
	"go.opencensus.io/stats"
)

// maxMultiMetrics is the metric ceiling per builder. The per-key "touched" bitmask
// is a uint64, so it addresses exactly 64 slots.
const maxMultiMetrics = 64

// metricKind selects how a slot folds successive Add values.
type metricKind uint8

const (
	kindCount     metricKind = iota // ignores the value, increments by one
	kindSum                         // accumulates the value
	kindLastValue                   // overwrites with the value (last-write-wins)
)

// MultiConfig configures a MultiAggregator with the shared Config plus the
// zero-slot policy applied to Count/Sum on flush.
type MultiConfig[K comparable, N Number] struct {
	Config[K]

	// SkipZeros, when true, omits Count/Sum slots whose value is 0 from the flush.
	// It never affects LastValue: a gauge of 0 is a legitimate reading and is
	// emitted whenever its slot was written this window (see multiAcc.touched).
	SkipZeros bool
}

// multiMetric is the immutable descriptor of a registered metric: its folding kind
// and the OpenCensus measure its slot is recorded against.
type multiMetric[N Number] struct {
	kind    metricKind
	measure Measure[N]
}

// multiAcc is the per-key accumulator shared by every metric. vals is indexed by
// metric id and is allocated once, the first time the key is seen. touched marks
// which slots were written this window so the flush can tell a real 0 (LastValue
// gauge set to 0) from an untouched slot.
type multiAcc[N Number] struct {
	vals    []N
	touched uint64
}

// MultiHandle is the write endpoint for one registered metric. It is a lightweight
// value (a shared aggregator pointer plus the slot coordinates) meant to be copied
// freely; every copy writes the same underlying slot. It intentionally lacks Stop,
// so it does not satisfy Aggregator[K, N].
type MultiHandle[K comparable, N Number] struct {
	agg  *MultiAggregator[K, N]
	id   int
	kind metricKind
}

// Add folds value into this metric's slot for key k under the shard lock: Count
// increments by one (value ignored), Sum accumulates, LastValue overwrites.
func (h MultiHandle[K, N]) Add(k K, value N) {
	h.agg.add(h.id, h.kind, k, value)
}

// MultiBuilder registers the metrics of a MultiAggregator and freezes them on
// Build. It is single-use: registering after Build or calling Build twice panics.
type MultiBuilder[K comparable, N Number] struct {
	cfg MultiConfig[K, N]
	agg *MultiAggregator[K, N]
}

// NewMultiBuilder starts a builder for a MultiAggregator over key K and value type
// N. Register metrics with Count/Sum/LastValue, then call Build.
func NewMultiBuilder[K comparable, N Number](cfg MultiConfig[K, N]) *MultiBuilder[K, N] {
	return &MultiBuilder[K, N]{
		cfg: cfg,
		agg: &MultiAggregator[K, N]{skipZeros: cfg.SkipZeros},
	}
}

// Count registers a count metric and returns its handle. Each Add increments the
// slot by one regardless of the value passed.
func (b *MultiBuilder[K, N]) Count(m Measure[N]) MultiHandle[K, N] {
	return b.register(kindCount, m)
}

// Sum registers a sum metric and returns its handle. Each Add accumulates the value
// into the slot.
func (b *MultiBuilder[K, N]) Sum(m Measure[N]) MultiHandle[K, N] {
	return b.register(kindSum, m)
}

// LastValue registers a gauge metric and returns its handle. Each Add overwrites the
// slot; the measure must be backed by a view of type view.LastValue().
func (b *MultiBuilder[K, N]) LastValue(m Measure[N]) MultiHandle[K, N] {
	return b.register(kindLastValue, m)
}

func (b *MultiBuilder[K, N]) register(kind metricKind, m Measure[N]) MultiHandle[K, N] {
	if b.agg.built {
		panic("opencensus: MultiBuilder cannot register a metric after Build")
	}
	if m == nil {
		panic("opencensus: MultiBuilder metric Measure must not be nil")
	}
	if len(b.agg.metrics) >= maxMultiMetrics {
		panic("opencensus: MultiBuilder supports at most 64 metrics")
	}
	id := len(b.agg.metrics)
	b.agg.metrics = append(b.agg.metrics, multiMetric[N]{kind: kind, measure: m})
	return MultiHandle[K, N]{agg: b.agg, id: id, kind: kind}
}

// Build freezes the registered metrics, applies Config defaults, starts the
// background flusher and returns the aggregator. It panics if called twice.
func (b *MultiBuilder[K, N]) Build() *MultiAggregator[K, N] {
	if b.agg.built {
		panic("opencensus: MultiBuilder.Build called more than once")
	}
	b.cfg.applyDefaults()

	a := b.agg
	a.built = true
	a.nMetrics = len(a.metrics)
	a.store = newStore[K, multiAcc[N]](b.cfg.Shards, b.cfg.Schema)
	a.ctx = newCtxCache[K](b.cfg.Schema)
	a.flusher = startFlusher(b.cfg.Interval, a.flush)
	return a
}

// MultiAggregator accumulates several Count/Sum/LastValue metrics per key K in one
// sharded store and flushes them together to OpenCensus on the configured interval.
type MultiAggregator[K comparable, N Number] struct {
	store   *shardedStore[K, multiAcc[N]]
	flusher *flusher
	ctx     *ctxCache[K]

	metrics   []multiMetric[N]
	nMetrics  int
	skipZeros bool
	built     bool
}

func (a *MultiAggregator[K, N]) add(id int, kind metricKind, k K, value N) {
	sh := a.store.shardFor(k)
	sh.mu.Lock()
	acc := sh.m[k]
	if acc == nil {
		acc = &multiAcc[N]{vals: make([]N, a.nMetrics)}
		sh.m[k] = acc
	}
	switch kind {
	case kindCount:
		acc.vals[id]++
	case kindSum:
		acc.vals[id] += value
	case kindLastValue:
		acc.vals[id] = value
	}
	acc.touched |= uint64(1) << id
	sh.mu.Unlock()
}

func (a *MultiAggregator[K, N]) flush() {
	a.store.drainEach(func(k K, acc *multiAcc[N]) {
		ms := a.measurementsFor(acc)
		if len(ms) == 0 {
			return
		}
		ctx, err := a.ctx.contextFor(k)
		if err != nil {
			return
		}
		stats.Record(ctx, ms...)
	})
}

// measurementsFor builds a FRESH slice of measurements for one key. A per-key buffer
// cannot be reused: stats.Record enqueues the slice for the asynchronous OpenCensus
// worker, which reads it later (rewriting it would be a data race), same reason as
// the batching in distribution.go.
func (a *MultiAggregator[K, N]) measurementsFor(acc *multiAcc[N]) []stats.Measurement {
	ms := make([]stats.Measurement, 0, a.nMetrics)
	for id, met := range a.metrics {
		v := acc.vals[id]
		if met.kind == kindLastValue {
			if acc.touched&(uint64(1)<<id) != 0 {
				ms = append(ms, met.measure.M(v))
			}
			continue
		}
		if a.skipZeros && v == 0 {
			continue
		}
		ms = append(ms, met.measure.M(v))
	}
	return ms
}

// Stop halts the background flusher after a final flush.
func (a *MultiAggregator[K, N]) Stop() { a.flusher.stop() }
