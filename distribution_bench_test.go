package opencensus

// Benchmark of the flush cost in Distribution.

import (
	"testing"
	"time"

	"go.opencensus.io/stats"
)

var benchLatency = stats.Float64("bench/latency", "ms", stats.UnitMilliseconds)

func BenchmarkDistributionAdd(b *testing.B) {
	setupBenchViews()
	combos := benchCombos()
	agg := NewDistributionAggregator(DistributionConfig[HTTPLabels, float64]{
		Config:           Config[HTTPLabels]{Shards: 32, Interval: time.Hour, Schema: benchSchema},
		Measure:          benchLatency,
		MaxSamplesPerKey: 4096,
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

func BenchmarkDistributionFlush(b *testing.B) {
	setupBenchViews()
	combos := benchCombos()
	const samplesPerCombo = 200

	agg := NewDistributionAggregator(DistributionConfig[HTTPLabels, float64]{
		Config:           Config[HTTPLabels]{Shards: 32, Interval: time.Hour, Schema: benchSchema},
		Measure:          benchLatency,
		MaxSamplesPerKey: 4096,
	})
	defer agg.Stop()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for _, c := range combos {
			for j := range samplesPerCombo {
				agg.Add(c, float64(10+(j%500)))
			}
		}
		b.StartTimer()
		agg.flush()
	}
	b.ReportMetric(float64(len(combos)*samplesPerCombo), "samples/flush")
}
