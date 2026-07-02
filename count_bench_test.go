package opencensus

import (
	"testing"
	"time"

	"go.opencensus.io/stats"
)

var benchLatencyCount = stats.Float64("bench/latency_count", "1", stats.UnitDimensionless)

func BenchmarkCountAdd(b *testing.B) {
	setupBenchViews()
	combos := benchCombos()
	agg := NewCountAggregator(CountConfig[HTTPLabels]{
		Config:       Config[HTTPLabels]{Shards: 32, Interval: time.Hour, Schema: benchSchema},
		CountMeasure: benchLatencyCount,
	})
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

func BenchmarkCountFlush(b *testing.B) {
	setupBenchViews()
	combos := benchCombos()
	agg := NewCountAggregator(CountConfig[HTTPLabels]{
		Config:       Config[HTTPLabels]{Shards: 32, Interval: time.Hour, Schema: benchSchema},
		CountMeasure: benchLatencyCount,
	})
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
