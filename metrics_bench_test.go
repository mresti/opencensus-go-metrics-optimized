package opencensus

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

func mustKey(name string) tag.Key {
	k, err := tag.NewKey(name)
	if err != nil {
		panic(err)
	}
	return k
}

var (
	benchKeyUser   = mustKey("bench_user")
	benchKeyRoute  = mustKey("bench_route")
	benchKeyStatus = mustKey("bench_status")
	benchSchema    = HTTPSchema{KeyUser: benchKeyUser, KeyRoute: benchKeyRoute, KeyStatus: benchKeyStatus}

	benchSetupOnce sync.Once
)

func setupBenchViews() {
	benchSetupOnce.Do(func() {
		keys := []tag.Key{benchKeyUser, benchKeyRoute, benchKeyStatus}
		_ = view.Register(
			&view.View{
				Name:        "bench/latency_distribution",
				Measure:     benchLatency,
				TagKeys:     keys,
				Aggregation: view.Distribution(10, 25, 50, 100, 250, 500, 1000),
			},
			&view.View{Name: "bench/latency_sum_v", Measure: benchLatencySum, TagKeys: keys, Aggregation: view.Sum()},
			&view.View{
				Name:        "bench/latency_count_v",
				Measure:     benchLatencyCount,
				TagKeys:     keys,
				Aggregation: view.Sum(),
			},
		)
	})
}

func benchCombos() []HTTPLabels {
	routes := []string{"/api/a", "/api/b", "/api/c", "/api/d", "/api/e"}
	statuses := []string{"200", "400", "404", "500"}
	var combos []HTTPLabels
	for u := range 10 {
		user := "user-" + strconv.Itoa(u)
		for _, r := range routes {
			for _, s := range statuses {
				combos = append(combos, HTTPLabels{User: user, Route: r, Status: s})
			}
		}
	}
	return combos
}

func emitOriginal(k HTTPLabels, value float64) {
	ctx, _ := tag.New(context.Background(),
		tag.Upsert(benchKeyUser, k.User),
		tag.Upsert(benchKeyRoute, k.Route),
		tag.Upsert(benchKeyStatus, k.Status),
	)
	stats.Record(ctx, benchLatency.M(value))
}

func BenchmarkEmitOriginal(b *testing.B) {
	setupBenchViews()
	combos := benchCombos()
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			emitOriginal(combos[i%len(combos)], 42.5)
			i++
		}
	})
}
