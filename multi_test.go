package opencensus

// Tests for the multi-metric aggregator: the folding semantics of each metric kind,
// the touched/SkipZeros flush rules, window draining, and the builder panics. Flush
// is driven directly (white-box) with Interval=time.Hour so the background flusher
// never races the assertions, mirroring the other variant tests.

import (
	"sync"
	"testing"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
)

func newSharedSchema(t *testing.T) HTTPSchema {
	t.Helper()
	p := uniqPrefix()
	return HTTPSchema{
		KeyUser:   newTagKey(t, p+"_u"),
		KeyRoute:  newTagKey(t, p+"_r"),
		KeyStatus: newTagKey(t, p+"_s"),
	}
}

// sumView registers a fresh float64 measure with a Sum view over schema and returns
// both. Count and Sum metrics record against a Sum view.
func sumView(t *testing.T, schema HTTPSchema) (Measure[float64], string) {
	return measureWithView(t, schema, view.Sum())
}

// lastValueView registers a fresh float64 measure with a LastValue view over schema.
func lastValueView(t *testing.T, schema HTTPSchema) (Measure[float64], string) {
	return measureWithView(t, schema, view.LastValue())
}

func measureWithView(t *testing.T, schema HTTPSchema, agg *view.Aggregation) (Measure[float64], string) {
	t.Helper()
	p := uniqPrefix()
	m := stats.Float64(p+"/m", "v", stats.UnitDimensionless)
	name := p + "/v"
	mustRegister(t, &view.View{Name: name, Measure: m, TagKeys: tagKeys(schema), Aggregation: agg})
	return m, name
}

func sumValueFor(t *testing.T, viewName string, s HTTPSchema, k HTTPLabels) *float64 {
	t.Helper()
	row := rowFor(t, viewName, s, k)
	if row == nil {
		return nil
	}
	d, ok := row.Data.(*view.SumData)
	if !ok {
		t.Fatalf("tipo de dato inesperado: %T", row.Data)
	}
	v := d.Value
	return &v
}

func TestMultiAggregator_FourCountsNineSums(t *testing.T) {
	schema := newSharedSchema(t)
	b := NewMultiBuilder[HTTPLabels, float64](Config1h(schema))

	const numCounts, numSums = 4, 9
	countHandles := make([]MultiHandle[HTTPLabels, float64], numCounts)
	countViews := make([]string, numCounts)
	for i := range countHandles {
		m, name := sumView(t, schema)
		countHandles[i] = b.Count(m)
		countViews[i] = name
	}

	sumHandles := make([]MultiHandle[HTTPLabels, float64], numSums)
	sumViews := make([]string, numSums)
	sumStep := make([]float64, numSums)
	for i := range sumHandles {
		m, name := sumView(t, schema)
		sumHandles[i] = b.Sum(m)
		sumViews[i] = name
		sumStep[i] = float64(i + 1)
	}

	agg := b.Build()
	defer agg.Stop()

	keys := []HTTPLabels{
		{User: "u1", Route: "/a", Status: "200"},
		{User: "u2", Route: "/b", Status: "404"},
		{User: "u3", Route: "/c", Status: "500"},
	}

	const goroutines, iterations = 8, 300
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for i := range iterations {
				k := keys[i%len(keys)]
				for _, h := range countHandles {
					h.Add(k, 1)
				}
				for j, h := range sumHandles {
					h.Add(k, sumStep[j])
				}
			}
		})
	}
	wg.Wait()
	agg.flush()

	addsPerKey := float64(goroutines * (iterations / len(keys)))
	for _, k := range keys {
		for _, vn := range countViews {
			got := sumValueFor(t, vn, schema, k)
			if got == nil || *got != addsPerKey {
				t.Errorf("count %s key %v = %v; quiero %v", vn, k, got, addsPerKey)
			}
		}
		for j, vn := range sumViews {
			want := addsPerKey * sumStep[j]
			got := sumValueFor(t, vn, schema, k)
			if got == nil || *got != want {
				t.Errorf("sum %s key %v = %v; quiero %v", vn, k, got, want)
			}
		}
	}
}

func TestMultiAggregator_LastValueUntouchedNotEmitted(t *testing.T) {
	schema := newSharedSchema(t)
	b := NewMultiBuilder[HTTPLabels, float64](Config1h(schema))
	countM, countView := sumView(t, schema)
	gaugeM, gv := lastValueView(t, schema)

	reqs := b.Count(countM)
	b.LastValue(gaugeM) // registered but never written this window
	agg := b.Build()
	defer agg.Stop()

	k := HTTPLabels{User: "u1", Route: "/a", Status: "200"}
	reqs.Add(k, 1) // key exists, but the gauge slot is never written
	agg.flush()

	if v := sumValueFor(t, countView, schema, k); v == nil || *v != 1 {
		t.Errorf("count = %v; quiero 1", v)
	}
	if v := lastValueFor(t, gv, schema, k); v != nil {
		t.Errorf("gauge no tocado emitió %v; quiero nada", *v)
	}
}

func TestMultiAggregator_LastValueExplicitZeroEmitted(t *testing.T) {
	schema := newSharedSchema(t)
	b := NewMultiBuilder[HTTPLabels, float64](Config1h(schema))
	gaugeM, gv := lastValueView(t, schema)
	depth := b.LastValue(gaugeM)
	agg := b.Build()
	defer agg.Stop()

	k := HTTPLabels{User: "u1", Route: "/a", Status: "200"}
	depth.Add(k, 0) // 0 is a legitimate gauge reading
	agg.flush()

	v := lastValueFor(t, gv, schema, k)
	if v == nil {
		t.Fatal("gauge=0 explícito no se emitió")
	}
	if *v != 0 {
		t.Errorf("gauge = %v; quiero 0", *v)
	}
}

func TestMultiAggregator_LastValueLastWriteWins(t *testing.T) {
	schema := newSharedSchema(t)
	b := NewMultiBuilder[HTTPLabels, float64](Config1h(schema))
	gaugeM, gv := lastValueView(t, schema)
	depth := b.LastValue(gaugeM)
	agg := b.Build()
	defer agg.Stop()

	k := HTTPLabels{User: "u1", Route: "/a", Status: "200"}
	depth.Add(k, 1)
	depth.Add(k, 2)
	depth.Add(k, 42)
	agg.flush()

	if v := lastValueFor(t, gv, schema, k); v == nil || *v != 42 {
		t.Errorf("gauge = %v; quiero 42", v)
	}
}

func TestMultiAggregator_SkipZeros(t *testing.T) {
	run := func(t *testing.T, skipZeros bool) (present bool) {
		schema := newSharedSchema(t)
		cfg := Config1h(schema)
		cfg.SkipZeros = skipZeros
		b := NewMultiBuilder[HTTPLabels, float64](cfg)

		countM, countView := sumView(t, schema)
		sumM, _ := sumView(t, schema)
		b.Count(countM) // count slot stays 0 (never added)
		bytes := b.Sum(sumM)
		agg := b.Build()
		defer agg.Stop()

		k := HTTPLabels{User: "u1", Route: "/a", Status: "200"}
		bytes.Add(k, 5) // touches the key so it exists, but the count slot is 0
		agg.flush()

		return sumValueFor(t, countView, schema, k) != nil
	}

	t.Run("true omits zero slots", func(t *testing.T) {
		if run(t, true) {
			t.Error("con SkipZeros=true el slot 0 se emitió; quiero omitido")
		}
	})
	t.Run("false emits zero slots", func(t *testing.T) {
		if !run(t, false) {
			t.Error("con SkipZeros=false el slot 0 no se emitió; quiero emitido")
		}
	})
}

func TestMultiAggregator_WindowDrainsAndReemitsOnlyNew(t *testing.T) {
	schema := newSharedSchema(t)
	b := NewMultiBuilder[HTTPLabels, float64](Config1h(schema))
	sumM, sv := sumView(t, schema)
	bytes := b.Sum(sumM)
	agg := b.Build()
	defer agg.Stop()

	k := HTTPLabels{User: "u1", Route: "/a", Status: "200"}

	bytes.Add(k, 10)
	agg.flush()
	if n := countStore(agg.store); n != 0 {
		t.Fatalf("store tras flush = %d; quiero 0", n)
	}
	if v := sumValueFor(t, sv, schema, k); v == nil || *v != 10 {
		t.Fatalf("ventana 1: sum = %v; quiero 10", v)
	}

	bytes.Add(k, 5)
	agg.flush()
	// The view aggregates with Sum(): 15 proves the second flush emitted only the
	// new 5 (a non-drained store would re-emit 10 and yield 25).
	if v := sumValueFor(t, sv, schema, k); v == nil || *v != 15 {
		t.Errorf("ventana 2: sum = %v; quiero 15", v)
	}
}

func TestMultiAggregator_SkipZerosDrainsKeyWithNoMeasurements(t *testing.T) {
	schema := newSharedSchema(t)
	cfg := Config1h(schema)
	cfg.SkipZeros = true
	b := NewMultiBuilder[HTTPLabels, float64](cfg)
	sumM, sv := sumView(t, schema)
	bytes := b.Sum(sumM)
	agg := b.Build()
	defer agg.Stop()

	k := HTTPLabels{User: "u1", Route: "/a", Status: "200"}
	bytes.Add(k, 0) // touches the key, but the only slot is a zero to be skipped
	agg.flush()

	if n := countStore(agg.store); n != 0 {
		t.Errorf("store tras flush = %d; quiero 0", n)
	}
	if v := sumValueFor(t, sv, schema, k); v != nil {
		t.Errorf("sum 0 con SkipZeros emitió %v; quiero nada", *v)
	}
}

func TestMultiBuilder_Panics(t *testing.T) {
	schema := newSharedSchema(t)

	t.Run("register after Build", func(t *testing.T) {
		b := NewMultiBuilder[HTTPLabels, float64](Config1h(schema))
		b.Build()
		mustPanic(t, "register tras Build", func() { b.Count(newFloatMeasure()) })
	})

	t.Run("double Build", func(t *testing.T) {
		b := NewMultiBuilder[HTTPLabels, float64](Config1h(schema))
		b.Build()
		mustPanic(t, "doble Build", func() { b.Build() })
	})

	t.Run("more than 64 metrics", func(t *testing.T) {
		b := NewMultiBuilder[HTTPLabels, float64](Config1h(schema))
		for range maxMultiMetrics {
			b.Count(newFloatMeasure())
		}
		mustPanic(t, "métrica 65", func() { b.Count(newFloatMeasure()) })
	})

	t.Run("nil Measure", func(t *testing.T) {
		b := NewMultiBuilder[HTTPLabels, float64](Config1h(schema))
		mustPanic(t, "Measure nil", func() { b.Sum(nil) })
	})
}

func Config1h(schema HTTPSchema) MultiConfig[HTTPLabels, float64] {
	return MultiConfig[HTTPLabels, float64]{
		Config: Config[HTTPLabels]{Shards: 8, Interval: time.Hour, Schema: schema},
	}
}

func newFloatMeasure() Measure[float64] {
	return stats.Float64(uniqPrefix()+"/m", "v", stats.UnitDimensionless)
}

func mustPanic(t *testing.T, what string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("se esperaba panic: %s", what)
		}
	}()
	fn()
}
