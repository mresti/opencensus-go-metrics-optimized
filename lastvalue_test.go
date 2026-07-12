package opencensus

// Unit tests for the LastValue variant (now generic, using HTTPLabels).

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"go.opencensus.io/stats/view"
)

func TestLastValueAggregator_LastWriteWins(t *testing.T) {
	schema, m, viewName := newHTTPFixture(t, view.LastValue())
	agg := NewLastValueAggregator(LastValueConfig[HTTPLabels, float64]{
		Config:  Config[HTTPLabels]{Shards: 8, Interval: time.Hour, Schema: schema},
		Measure: m,
	})
	defer agg.Stop()

	k := HTTPLabels{User: "u1", Route: "/api", Status: "200"}
	agg.Add(k, 1)
	agg.Add(k, 2)
	agg.Add(k, 42)
	agg.flush()

	got := lastValueFor(t, viewName, schema, k)
	if got == nil {
		t.Fatal("combination not recorded")
	}
	if *got != 42 {
		t.Errorf("last value = %v; want 42", *got)
	}
}

func TestLastValueAggregator_MultipleCombinations(t *testing.T) {
	schema, m, viewName := newHTTPFixture(t, view.LastValue())
	agg := NewLastValueAggregator(LastValueConfig[HTTPLabels, float64]{
		Config:  Config[HTTPLabels]{Shards: 8, Interval: time.Hour, Schema: schema},
		Measure: m,
	})
	defer agg.Stop()

	k1 := HTTPLabels{User: "u1", Route: "/a", Status: "200"}
	k2 := HTTPLabels{User: "u2", Route: "/b", Status: "500"}
	agg.Add(k1, 10)
	agg.Add(k2, 20)
	agg.flush()

	if v := lastValueFor(t, viewName, schema, k1); v == nil || *v != 10 {
		t.Errorf("combination 1 = %v; want 10", v)
	}
	if v := lastValueFor(t, viewName, schema, k2); v == nil || *v != 20 {
		t.Errorf("combination 2 = %v; want 20", v)
	}
}

func TestLastValueAggregator_DrainsAfterFlush(t *testing.T) {
	schema, m, _ := newHTTPFixture(t, view.LastValue())
	agg := NewLastValueAggregator(LastValueConfig[HTTPLabels, float64]{
		Config:  Config[HTTPLabels]{Shards: 8, Interval: time.Hour, Schema: schema},
		Measure: m,
	})
	defer agg.Stop()

	agg.Add(HTTPLabels{User: "u1", Route: "/a", Status: "200"}, 1)
	agg.Add(HTTPLabels{User: "u2", Route: "/b", Status: "200"}, 2)
	if n := countStore(agg.store); n != 2 {
		t.Fatalf("before flush: %d; want 2", n)
	}
	agg.flush()
	if n := countStore(agg.store); n != 0 {
		t.Errorf("after flush: %d; want 0", n)
	}
}

func TestLastValueAggregator_StopFlushesRemaining(t *testing.T) {
	schema, m, viewName := newHTTPFixture(t, view.LastValue())
	agg := NewLastValueAggregator(LastValueConfig[HTTPLabels, float64]{
		Config:  Config[HTTPLabels]{Shards: 8, Interval: time.Hour, Schema: schema},
		Measure: m,
	})

	k := HTTPLabels{User: "u1", Route: "/a", Status: "200"}
	agg.Add(k, 7)
	agg.Stop() // flush final

	if v := lastValueFor(t, viewName, schema, k); v == nil || *v != 7 {
		t.Errorf("after Stop = %v; want 7", v)
	}
	if n := countStore(agg.store); n != 0 {
		t.Errorf("after Stop the store has %d; want 0", n)
	}
}

func TestLastValueAggregator_ConcurrentAdds(t *testing.T) {
	schema, m, viewName := newHTTPFixture(t, view.LastValue())
	agg := NewLastValueAggregator(LastValueConfig[HTTPLabels, float64]{
		Config:  Config[HTTPLabels]{Shards: 8, Interval: time.Hour, Schema: schema},
		Measure: m,
	})
	defer agg.Stop()

	const goroutines, iterations, numKeys = 8, 2000, 50
	const theValue = 99.0

	keys := make([]HTTPLabels, numKeys)
	for i := range keys {
		keys[i] = HTTPLabels{User: "u", Route: "/r", Status: strconv.Itoa(i)}
	}

	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for i := range iterations {
				agg.Add(keys[i%numKeys], theValue)
			}
		})
	}
	wg.Wait()
	agg.flush()

	for _, k := range keys {
		if v := lastValueFor(t, viewName, schema, k); v == nil || *v != theValue {
			t.Errorf("key %v = %v; want %v", k, v, theValue)
		}
	}
}

// lastValueFor returns the LastValue of a key, or nil if it is not present.
func lastValueFor(t *testing.T, viewName string, s HTTPSchema, k HTTPLabels) *float64 {
	t.Helper()
	row := rowFor(t, viewName, s, k)
	if row == nil {
		return nil
	}
	d, ok := row.Data.(*view.LastValueData)
	if !ok {
		t.Fatalf("unexpected data type: %T", row.Data)
	}
	v := d.Value
	return &v
}
