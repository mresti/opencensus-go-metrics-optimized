package opencensus

// Contract test for the Aggregator[K] interface. Walks through the THREE variants
// and verifies what must behave the SAME in all of them: Add records exactly the
// observed keys, Stop does a final flush, flush() drains the store, and Add is
// safe under concurrency (-race). It does not verify value semantics (those differ).

import (
	"sync"
	"testing"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
)

type variantEnv struct {
	make           func(interval time.Duration) Aggregator[HTTPLabels]
	flush          func(Aggregator[HTTPLabels])
	storeLen       func(Aggregator[HTTPLabels]) int
	recordedCombos func(t *testing.T) map[HTTPLabels]bool
}

func setupCountVariant(t *testing.T) variantEnv {
	p := uniqPrefix()
	schema := HTTPSchema{
		KeyUser:   newTagKey(t, p+"_u"),
		KeyRoute:  newTagKey(t, p+"_r"),
		KeyStatus: newTagKey(t, p+"_s"),
	}
	keys := tagKeys(schema)
	cntM := stats.Float64(p+"/cnt", "v", stats.UnitDimensionless)
	cntV := &view.View{Name: p + "/cnt_v", Measure: cntM, TagKeys: keys, Aggregation: view.Sum()}
	mustRegister(t, cntV)

	return variantEnv{
		make: func(iv time.Duration) Aggregator[HTTPLabels] {
			return NewCountAggregator(CountConfig[HTTPLabels]{
				Config:       Config[HTTPLabels]{Shards: 8, Interval: iv, Schema: schema},
				CountMeasure: cntM,
			})
		},
		flush:          func(a Aggregator[HTTPLabels]) { a.(*CountAggregator[HTTPLabels]).flush() },
		storeLen:       func(a Aggregator[HTTPLabels]) int { return countStore(a.(*CountAggregator[HTTPLabels]).store) },
		recordedCombos: func(t *testing.T) map[HTTPLabels]bool { return combosInView(t, cntV.Name, schema) },
	}
}

func setupSumVariant(t *testing.T) variantEnv {
	p := uniqPrefix()
	schema := HTTPSchema{
		KeyUser:   newTagKey(t, p+"_u"),
		KeyRoute:  newTagKey(t, p+"_r"),
		KeyStatus: newTagKey(t, p+"_s"),
	}
	keys := tagKeys(schema)
	sumM := stats.Float64(p+"/sum", "v", stats.UnitDimensionless)
	sumV := &view.View{Name: p + "/sum_v", Measure: sumM, TagKeys: keys, Aggregation: view.Sum()}
	mustRegister(t, sumV)

	return variantEnv{
		make: func(iv time.Duration) Aggregator[HTTPLabels] {
			return NewSumAggregator(SumConfig[HTTPLabels]{
				Config:     Config[HTTPLabels]{Shards: 8, Interval: iv, Schema: schema},
				SumMeasure: sumM,
			})
		},
		flush:          func(a Aggregator[HTTPLabels]) { a.(*SumAggregator[HTTPLabels]).flush() },
		storeLen:       func(a Aggregator[HTTPLabels]) int { return countStore(a.(*SumAggregator[HTTPLabels]).store) },
		recordedCombos: func(t *testing.T) map[HTTPLabels]bool { return combosInView(t, sumV.Name, schema) },
	}
}

func setupDistributionVariant(t *testing.T) variantEnv {
	p := uniqPrefix()
	schema := HTTPSchema{
		KeyUser:   newTagKey(t, p+"_u"),
		KeyRoute:  newTagKey(t, p+"_r"),
		KeyStatus: newTagKey(t, p+"_s"),
	}
	m := stats.Float64(p+"/lat", "ms", stats.UnitMilliseconds)
	v := &view.View{
		Name:        p + "/dist_v",
		Measure:     m,
		TagKeys:     tagKeys(schema),
		Aggregation: view.Distribution(10, 50, 100),
	}
	mustRegister(t, v)

	return variantEnv{
		make: func(iv time.Duration) Aggregator[HTTPLabels] {
			return NewDistributionAggregator(DistributionConfig[HTTPLabels]{
				Config:           Config[HTTPLabels]{Shards: 8, Interval: iv, Schema: schema},
				Measure:          m,
				MaxSamplesPerKey: 0,
			})
		},
		flush:          func(a Aggregator[HTTPLabels]) { a.(*DistributionAggregator[HTTPLabels]).flush() },
		storeLen:       func(a Aggregator[HTTPLabels]) int { return countStore(a.(*DistributionAggregator[HTTPLabels]).store) },
		recordedCombos: func(t *testing.T) map[HTTPLabels]bool { return combosInView(t, v.Name, schema) },
	}
}

func setupLastValueVariant(t *testing.T) variantEnv {
	p := uniqPrefix()
	schema := HTTPSchema{
		KeyUser:   newTagKey(t, p+"_u"),
		KeyRoute:  newTagKey(t, p+"_r"),
		KeyStatus: newTagKey(t, p+"_s"),
	}
	m := stats.Float64(p+"/gauge", "v", stats.UnitDimensionless)
	v := &view.View{Name: p + "/lv_v", Measure: m, TagKeys: tagKeys(schema), Aggregation: view.LastValue()}
	mustRegister(t, v)

	return variantEnv{
		make: func(iv time.Duration) Aggregator[HTTPLabels] {
			return NewLastValueAggregator(LastValueConfig[HTTPLabels]{
				Config:  Config[HTTPLabels]{Shards: 8, Interval: iv, Schema: schema},
				Measure: m,
			})
		},
		flush:          func(a Aggregator[HTTPLabels]) { a.(*LastValueAggregator[HTTPLabels]).flush() },
		storeLen:       func(a Aggregator[HTTPLabels]) int { return countStore(a.(*LastValueAggregator[HTTPLabels]).store) },
		recordedCombos: func(t *testing.T) map[HTTPLabels]bool { return combosInView(t, v.Name, schema) },
	}
}

func TestAggregatorContract(t *testing.T) {
	variants := []struct {
		name  string
		setup func(t *testing.T) variantEnv
	}{
		{"Count", setupCountVariant},
		{"Sum", setupSumVariant},
		{"Distribution", setupDistributionVariant},
		{"LastValue", setupLastValueVariant},
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
						t.Errorf("combo %v no registrada tras Stop", c)
					}
				}
				if len(got) != len(combos) {
					t.Errorf("registradas %d combos; quiero %d", len(got), len(combos))
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
					t.Fatalf("antes del flush: %d; quiero %d", n, len(combos))
				}
				env.flush(agg)
				if n := env.storeLen(agg); n != 0 {
					t.Errorf("tras el flush: %d; quiero 0", n)
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
						t.Errorf("combo %v ausente tras Adds concurrentes", c)
					}
				}
			})
		})
	}
}
