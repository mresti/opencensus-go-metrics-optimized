package opencensus

// Benchmarks for the LastValue variant.

import (
	"sync"
	"testing"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var (
	benchGauge  = stats.Float64("bench/gauge", "1", stats.UnitDimensionless)
	benchLVOnce sync.Once
)

func setupLastValueView() {
	benchLVOnce.Do(func() {
		_ = view.Register(&view.View{
			Name:        "bench/gauge_lastvalue",
			Measure:     benchGauge,
			TagKeys:     []tag.Key{benchKeyUser, benchKeyRoute, benchKeyStatus},
			Aggregation: view.LastValue(),
		})
	})
}

func newBenchLastValueAggregator() *LastValueAggregator[HTTPLabels, float64] {
	return NewLastValueAggregator(LastValueConfig[HTTPLabels, float64]{
		Config:  Config[HTTPLabels]{Shards: 32, Interval: time.Hour, Schema: benchSchema},
		Measure: benchGauge,
	})
}

func BenchmarkLastValueAdd(b *testing.B) {
	setupLastValueView()
	combos := benchCombos()
	agg := newBenchLastValueAggregator()
	defer agg.Stop()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			agg.Add(combos[i%len(combos)], 42.5)
			i++
		}
	})
}

func BenchmarkLastValueFlush(b *testing.B) {
	setupLastValueView()
	combos := benchCombos()
	agg := newBenchLastValueAggregator()
	defer agg.Stop()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for _, c := range combos {
			agg.Add(c, 42.5)
		}
		b.StartTimer()
		agg.flush()
	}
	b.ReportMetric(float64(len(combos)), "combos/flush")
}
