package opencensus

// Contract test for the Aggregator[K, N] interface. Walks through the FOUR variants
// and verifies what must behave the SAME in all of them: Add records exactly the
// observed keys, Stop does a final flush, flush() drains the store, and Add is
// safe under concurrency (-race). It does not verify value semantics (those differ).
// The whole suite runs for both value types (float64 and int64) via measureMaker.

import (
	"sync"
	"testing"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
)

type variantEnv[N Number] struct {
	make           func(interval time.Duration) Aggregator[HTTPLabels, N]
	flush          func(Aggregator[HTTPLabels, N])
	storeLen       func(Aggregator[HTTPLabels, N]) int
	recordedCombos func(t *testing.T) map[HTTPLabels]bool
}

// measureMaker builds a measure of value type N and returns it both as the typed
// Measure[N] the aggregator needs and as the stats.Measure the view needs.
type measureMaker[N Number] func(name string) (Measure[N], stats.Measure)

func float64Measure(name string) (Measure[float64], stats.Measure) {
	m := stats.Float64(name, "v", stats.UnitDimensionless)
	return m, m
}

func int64Measure(name string) (Measure[int64], stats.Measure) {
	m := stats.Int64(name, "v", stats.UnitDimensionless)
	return m, m
}

func setupCountVariant[N Number](t *testing.T, mk measureMaker[N]) variantEnv[N] {
	p := uniqPrefix()
	schema := HTTPSchema{
		KeyUser:   newTagKey(t, p+"_u"),
		KeyRoute:  newTagKey(t, p+"_r"),
		KeyStatus: newTagKey(t, p+"_s"),
	}
	cntM, statsM := mk(p + "/cnt")
	cntV := &view.View{Name: p + "/cnt_v", Measure: statsM, TagKeys: tagKeys(schema), Aggregation: view.Sum()}
	mustRegister(t, cntV)

	return variantEnv[N]{
		make: func(iv time.Duration) Aggregator[HTTPLabels, N] {
			return NewCountAggregator(CountConfig[HTTPLabels, N]{
				Config:       Config[HTTPLabels]{Shards: 8, Interval: iv, Schema: schema},
				CountMeasure: cntM,
			})
		},
		flush:          func(a Aggregator[HTTPLabels, N]) { a.(*CountAggregator[HTTPLabels, N]).flush() },
		storeLen:       func(a Aggregator[HTTPLabels, N]) int { return countStore(a.(*CountAggregator[HTTPLabels, N]).store) },
		recordedCombos: func(t *testing.T) map[HTTPLabels]bool { return combosInView(t, cntV.Name, schema) },
	}
}

func setupSumVariant[N Number](t *testing.T, mk measureMaker[N]) variantEnv[N] {
	p := uniqPrefix()
	schema := HTTPSchema{
		KeyUser:   newTagKey(t, p+"_u"),
		KeyRoute:  newTagKey(t, p+"_r"),
		KeyStatus: newTagKey(t, p+"_s"),
	}
	sumM, statsM := mk(p + "/sum")
	sumV := &view.View{Name: p + "/sum_v", Measure: statsM, TagKeys: tagKeys(schema), Aggregation: view.Sum()}
	mustRegister(t, sumV)

	return variantEnv[N]{
		make: func(iv time.Duration) Aggregator[HTTPLabels, N] {
			return NewSumAggregator(SumConfig[HTTPLabels, N]{
				Config:     Config[HTTPLabels]{Shards: 8, Interval: iv, Schema: schema},
				SumMeasure: sumM,
			})
		},
		flush:          func(a Aggregator[HTTPLabels, N]) { a.(*SumAggregator[HTTPLabels, N]).flush() },
		storeLen:       func(a Aggregator[HTTPLabels, N]) int { return countStore(a.(*SumAggregator[HTTPLabels, N]).store) },
		recordedCombos: func(t *testing.T) map[HTTPLabels]bool { return combosInView(t, sumV.Name, schema) },
	}
}

func setupDistributionVariant[N Number](t *testing.T, mk measureMaker[N]) variantEnv[N] {
	p := uniqPrefix()
	schema := HTTPSchema{
		KeyUser:   newTagKey(t, p+"_u"),
		KeyRoute:  newTagKey(t, p+"_r"),
		KeyStatus: newTagKey(t, p+"_s"),
	}
	m, statsM := mk(p + "/lat")
	v := &view.View{
		Name:        p + "/dist_v",
		Measure:     statsM,
		TagKeys:     tagKeys(schema),
		Aggregation: view.Distribution(10, 50, 100),
	}
	mustRegister(t, v)

	return variantEnv[N]{
		make: func(iv time.Duration) Aggregator[HTTPLabels, N] {
			return NewDistributionAggregator(DistributionConfig[HTTPLabels, N]{
				Config:           Config[HTTPLabels]{Shards: 8, Interval: iv, Schema: schema},
				Measure:          m,
				MaxSamplesPerKey: 0,
			})
		},
		flush: func(a Aggregator[HTTPLabels, N]) { a.(*DistributionAggregator[HTTPLabels, N]).flush() },
		storeLen: func(a Aggregator[HTTPLabels, N]) int {
			return countStore(a.(*DistributionAggregator[HTTPLabels, N]).store)
		},
		recordedCombos: func(t *testing.T) map[HTTPLabels]bool { return combosInView(t, v.Name, schema) },
	}
}

func setupLastValueVariant[N Number](t *testing.T, mk measureMaker[N]) variantEnv[N] {
	p := uniqPrefix()
	schema := HTTPSchema{
		KeyUser:   newTagKey(t, p+"_u"),
		KeyRoute:  newTagKey(t, p+"_r"),
		KeyStatus: newTagKey(t, p+"_s"),
	}
	m, statsM := mk(p + "/gauge")
	v := &view.View{Name: p + "/lv_v", Measure: statsM, TagKeys: tagKeys(schema), Aggregation: view.LastValue()}
	mustRegister(t, v)

	return variantEnv[N]{
		make: func(iv time.Duration) Aggregator[HTTPLabels, N] {
			return NewLastValueAggregator(LastValueConfig[HTTPLabels, N]{
				Config:  Config[HTTPLabels]{Shards: 8, Interval: iv, Schema: schema},
				Measure: m,
			})
		},
		flush: func(a Aggregator[HTTPLabels, N]) { a.(*LastValueAggregator[HTTPLabels, N]).flush() },
		storeLen: func(a Aggregator[HTTPLabels, N]) int {
			return countStore(a.(*LastValueAggregator[HTTPLabels, N]).store)
		},
		recordedCombos: func(t *testing.T) map[HTTPLabels]bool { return combosInView(t, v.Name, schema) },
	}
}

func TestAggregatorContract(t *testing.T) {
	t.Run("float64", func(t *testing.T) { runAggregatorContract(t, float64Measure) })
	t.Run("int64", func(t *testing.T) { runAggregatorContract(t, int64Measure) })
}

func runAggregatorContract[N Number](t *testing.T, mk measureMaker[N]) {
	variants := []struct {
		name  string
		setup func(t *testing.T) variantEnv[N]
	}{
		{"Count", func(t *testing.T) variantEnv[N] { return setupCountVariant(t, mk) }},
		{"Sum", func(t *testing.T) variantEnv[N] { return setupSumVariant(t, mk) }},
		{"Distribution", func(t *testing.T) variantEnv[N] { return setupDistributionVariant(t, mk) }},
		{"LastValue", func(t *testing.T) variantEnv[N] { return setupLastValueVariant(t, mk) }},
	}

	combos := []HTTPLabels{
		{User: "u1", Route: "/a", Status: "200"},
		{User: "u2", Route: "/b", Status: "404"},
		{User: "u3", Route: "/c", Status: "500"},
	}

	for _, vc := range variants {
		t.Run(vc.name, func(t *testing.T) {
			t.Run("AddThenStopFlushes", func(t *testing.T) {
				env := vc.setup(t)
				agg := env.make(time.Hour)
				for _, c := range combos {
					agg.Add(c, 5)
				}
				agg.Stop()

				got := env.recordedCombos(t)
				for _, c := range combos {
					if !got[c] {
						t.Errorf("combo %v not recorded after Stop", c)
					}
				}
				if len(got) != len(combos) {
					t.Errorf("recorded %d combos; want %d", len(got), len(combos))
				}
			})

			t.Run("FlushDrainsStore", func(t *testing.T) {
				env := vc.setup(t)
				agg := env.make(time.Hour)
				defer agg.Stop()

				for _, c := range combos {
					agg.Add(c, 5)
				}
				if n := env.storeLen(agg); n != len(combos) {
					t.Fatalf("before flush: %d; want %d", n, len(combos))
				}
				env.flush(agg)
				if n := env.storeLen(agg); n != 0 {
					t.Errorf("after flush: %d; want 0", n)
				}
			})

			t.Run("ConcurrentAddsRaceFree", func(t *testing.T) {
				env := vc.setup(t)
				agg := env.make(time.Hour)

				const goroutines, iterations = 8, 1000
				var wg sync.WaitGroup
				for range goroutines {
					wg.Go(func() {
						for i := range iterations {
							agg.Add(combos[i%len(combos)], 1)
						}
					})
				}
				wg.Wait()
				agg.Stop()

				got := env.recordedCombos(t)
				for _, c := range combos {
					if !got[c] {
						t.Errorf("combo %v missing after concurrent Adds", c)
					}
				}
			})
		})
	}
}
