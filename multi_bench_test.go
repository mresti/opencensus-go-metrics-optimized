package opencensus

// Benchmarks for the multi-metric aggregator against the equivalent set of separate
// single-metric aggregators (4 Count + 9 Sum), the same layout as the target case.
// Both sides share benchSchema, benchCombos and the measures/views registered here,
// so the only difference measured is the aggregation topology (one shared store +
// one flusher vs thirteen independent ones).

import (
	"strconv"
	"testing"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

const (
	benchMultiNumCounts  = 4
	benchMultiNumSums    = 9
	benchMultiNumMetrics = benchMultiNumCounts + benchMultiNumSums
	benchFlushKeys       = 1000
)

// benchAgg unifies *CountAggregator and *SumAggregator so the separated side can be
// driven through a single slice; flush() is unexported, hence the local interface.
type benchAgg interface {
	Add(k HTTPLabels, v float64)
	flush()
	Stop()
}

var (
	_ benchAgg = (*CountAggregator[HTTPLabels, float64])(nil)
	_ benchAgg = (*SumAggregator[HTTPLabels, float64])(nil)

	benchMultiCountMeasures [benchMultiNumCounts]*stats.Float64Measure
	benchMultiSumMeasures   [benchMultiNumSums]*stats.Float64Measure

	benchMultiViewsOnce onceFlag
)

// onceFlag is a tiny idempotency guard; sync.Once would work too but a bool keeps the
// registration site inline and readable in this test-only helper.
type onceFlag struct{ done bool }

func setupMultiBenchViews() {
	if benchMultiViewsOnce.done {
		return
	}
	benchMultiViewsOnce.done = true

	keys := []tag.Key{benchKeyUser, benchKeyRoute, benchKeyStatus}
	var views []*view.View
	for i := range benchMultiCountMeasures {
		name := "bench/multi_count_" + strconv.Itoa(i)
		m := stats.Float64(name, "1", stats.UnitDimensionless)
		benchMultiCountMeasures[i] = m
		views = append(views, &view.View{
			Name:        name + "_v",
			Measure:     m,
			TagKeys:     keys,
			Aggregation: view.Sum(),
		})
	}
	for i := range benchMultiSumMeasures {
		name := "bench/multi_sum_" + strconv.Itoa(i)
		m := stats.Float64(name, "ms", stats.UnitMilliseconds)
		benchMultiSumMeasures[i] = m
		views = append(views, &view.View{
			Name:        name + "_v",
			Measure:     m,
			TagKeys:     keys,
			Aggregation: view.Sum(),
		})
	}
	_ = view.Register(views...)
}

func newBenchMultiAggregator() (*MultiAggregator[HTTPLabels, float64], []MultiHandle[HTTPLabels, float64]) {
	b := NewMultiBuilder[HTTPLabels, float64](MultiConfig[HTTPLabels, float64]{
		Config: Config[HTTPLabels]{Shards: 32, Interval: time.Hour, Schema: benchSchema},
	})
	handles := make([]MultiHandle[HTTPLabels, float64], 0, benchMultiNumMetrics)
	for _, m := range benchMultiCountMeasures {
		handles = append(handles, b.Count(m))
	}
	for _, m := range benchMultiSumMeasures {
		handles = append(handles, b.Sum(m))
	}
	return b.Build(), handles
}

func newBenchSeparatedAggregators() []benchAgg {
	aggs := make([]benchAgg, 0, benchMultiNumMetrics)
	for _, m := range benchMultiCountMeasures {
		aggs = append(aggs, NewCountAggregator(CountConfig[HTTPLabels, float64]{
			Config:       Config[HTTPLabels]{Shards: 32, Interval: time.Hour, Schema: benchSchema},
			CountMeasure: m,
		}))
	}
	for _, m := range benchMultiSumMeasures {
		aggs = append(aggs, NewSumAggregator(SumConfig[HTTPLabels, float64]{
			Config:     Config[HTTPLabels]{Shards: 32, Interval: time.Hour, Schema: benchSchema},
			SumMeasure: m,
		}))
	}
	return aggs
}

func stopAll(aggs []benchAgg) {
	for _, a := range aggs {
		a.Stop()
	}
}

func benchFlushKeySet(n int) []HTTPLabels {
	keys := make([]HTTPLabels, n)
	for i := range keys {
		keys[i] = HTTPLabels{User: "user-" + strconv.Itoa(i), Route: "/api/x", Status: "200"}
	}
	return keys
}

// (a) Per-Add hot path: one event writes exactly one metric. The multi handle must
// not add measurable overhead over an independent aggregator; both should be 0
// allocs/op in steady state.
func BenchmarkMultiAdd(b *testing.B) {
	setupMultiBenchViews()
	combos := benchCombos()
	agg, handles := newBenchMultiAggregator()
	defer agg.Stop()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handles[i%len(handles)].Add(combos[i%len(combos)], 42.5)
	}
}

func BenchmarkSeparatedAdd(b *testing.B) {
	setupMultiBenchViews()
	combos := benchCombos()
	aggs := newBenchSeparatedAggregators()
	defer stopAll(aggs)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aggs[i%len(aggs)].Add(combos[i%len(combos)], 42.5)
	}
}

// (b) One event fanning out to all 13 metrics of a single key. Separated pays 13
// shard lookups + 13 locks; multi pays 13 locks on the same shard entry but resolves
// one accumulator slice (better locality). Reported as-is.
func BenchmarkMultiEvent13Metrics(b *testing.B) {
	setupMultiBenchViews()
	k := benchCombos()[0]
	agg, handles := newBenchMultiAggregator()
	defer agg.Stop()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, h := range handles {
			h.Add(k, 42.5)
		}
	}
	b.ReportMetric(float64(len(handles)), "metrics/event")
}

func BenchmarkSeparatedEvent13Metrics(b *testing.B) {
	setupMultiBenchViews()
	k := benchCombos()[0]
	aggs := newBenchSeparatedAggregators()
	defer stopAll(aggs)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, a := range aggs {
			a.Add(k, 42.5)
		}
	}
	b.ReportMetric(float64(len(aggs)), "metrics/event")
}

// (c) Flush of benchFlushKeys keys with all 13 metrics touched. Multi emits one
// stats.Record (13 measurements) per key; separated emits 13 records per key across
// 13 flushes, plus 13x the ctxCache lookups. Populate happens with the timer
// stopped, matching the existing *Flush benchmarks (which also time Record).
func BenchmarkMultiFlush(b *testing.B) {
	setupMultiBenchViews()
	keys := benchFlushKeySet(benchFlushKeys)
	agg, handles := newBenchMultiAggregator()
	defer agg.Stop()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for _, k := range keys {
			for _, h := range handles {
				h.Add(k, 42.5)
			}
		}
		b.StartTimer()
		agg.flush()
	}
	b.ReportMetric(float64(len(keys)), "keys/flush")
}

func BenchmarkSeparatedFlush(b *testing.B) {
	setupMultiBenchViews()
	keys := benchFlushKeySet(benchFlushKeys)
	aggs := newBenchSeparatedAggregators()
	defer stopAll(aggs)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for _, a := range aggs {
			for _, k := range keys {
				a.Add(k, 42.5)
			}
		}
		b.StartTimer()
		for _, a := range aggs {
			a.flush()
		}
	}
	b.ReportMetric(float64(len(keys)), "keys/flush")
}

// (d) Write contention: concurrent single-metric Adds through the shared store vs
// independent stores.
func BenchmarkMultiAddParallel(b *testing.B) {
	setupMultiBenchViews()
	combos := benchCombos()
	agg, handles := newBenchMultiAggregator()
	defer agg.Stop()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			handles[i%len(handles)].Add(combos[i%len(combos)], 42.5)
			i++
		}
	})
}

func BenchmarkSeparatedAddParallel(b *testing.B) {
	setupMultiBenchViews()
	combos := benchCombos()
	aggs := newBenchSeparatedAggregators()
	defer stopAll(aggs)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			aggs[i%len(aggs)].Add(combos[i%len(combos)], 42.5)
			i++
		}
	})
}
