package opencensus

import (
	"testing"
	"time"

	"go.opencensus.io/stats"
)

var benchLatencySum = stats.Float64("bench/latency_sum", "ms", stats.UnitMilliseconds)

func BenchmarkSumAdd(b *testing.B) {
	setupBenchViews()
	combos := benchCombos()
	agg := NewSumAggregator(SumConfig[HTTPLabels, float64]{
		Config:     Config[HTTPLabels]{Shards: 32, Interval: time.Hour, Schema: benchSchema},
		SumMeasure: benchLatencySum,
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

func BenchmarkSumFlush(b *testing.B) {
	setupBenchViews()
	combos := benchCombos()
	agg := NewSumAggregator(SumConfig[HTTPLabels, float64]{
		Config:     Config[HTTPLabels]{Shards: 32, Interval: time.Hour, Schema: benchSchema},
		SumMeasure: benchLatencySum,
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
