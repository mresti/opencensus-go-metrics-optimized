package opencensus

// Tests proving each aggregator variant works with a *stats.Int64Measure and an
// int64-backed view, exercising the generic Number type parameter end to end.

import (
	"testing"
	"time"

	"go.opencensus.io/stats/view"
)

func TestSumAggregator_Int64Measure(t *testing.T) {
	schema, m, viewName := newHTTPFixtureInt64(t, view.Sum())
	agg := NewSumAggregator(SumConfig[HTTPLabels, int64]{
		Config:     Config[HTTPLabels]{Shards: 8, Interval: time.Hour, Schema: schema},
		SumMeasure: m,
	})
	defer agg.Stop()

	k := HTTPLabels{User: "u1", Route: "/api", Status: "200"}
	agg.Add(k, 3)
	agg.Add(k, 4)
	agg.flush()

	row := rowFor(t, viewName, schema, k)
	if row == nil {
		t.Fatal("combination not recorded")
	}
	d, ok := row.Data.(*view.SumData)
	if !ok {
		t.Fatalf("unexpected data type: %T", row.Data)
	}
	if d.Value != 7 {
		t.Errorf("sum = %v; want 7", d.Value)
	}
}

func TestCountAggregator_Int64Measure(t *testing.T) {
	schema, m, viewName := newHTTPFixtureInt64(t, view.Sum())
	agg := NewCountAggregator(CountConfig[HTTPLabels, int64]{
		Config:       Config[HTTPLabels]{Shards: 8, Interval: time.Hour, Schema: schema},
		CountMeasure: m,
	})
	defer agg.Stop()

	k := HTTPLabels{User: "u1", Route: "/api", Status: "200"}
	agg.Add(k, 0)
	agg.Add(k, 0)
	agg.Add(k, 0)
	agg.flush()

	row := rowFor(t, viewName, schema, k)
	if row == nil {
		t.Fatal("combination not recorded")
	}
	d, ok := row.Data.(*view.SumData)
	if !ok {
		t.Fatalf("unexpected data type: %T", row.Data)
	}
	if d.Value != 3 {
		t.Errorf("count = %v; want 3", d.Value)
	}
}

func TestLastValueAggregator_Int64Measure(t *testing.T) {
	schema, m, viewName := newHTTPFixtureInt64(t, view.LastValue())
	agg := NewLastValueAggregator(LastValueConfig[HTTPLabels, int64]{
		Config:  Config[HTTPLabels]{Shards: 8, Interval: time.Hour, Schema: schema},
		Measure: m,
	})
	defer agg.Stop()

	k := HTTPLabels{User: "u1", Route: "/api", Status: "200"}
	agg.Add(k, 1)
	agg.Add(k, 99)
	agg.flush()

	got := lastValueFor(t, viewName, schema, k)
	if got == nil {
		t.Fatal("combination not recorded")
	}
	if *got != 99 {
		t.Errorf("last value = %v; want 99", *got)
	}
}

func TestDistributionAggregator_Int64Measure(t *testing.T) {
	schema, m, viewName := newHTTPFixtureInt64(t, view.Distribution(10, 50, 100))
	agg := NewDistributionAggregator(DistributionConfig[HTTPLabels, int64]{
		Config:           Config[HTTPLabels]{Shards: 8, Interval: time.Hour, Schema: schema},
		Measure:          m,
		MaxSamplesPerKey: 0,
	})
	defer agg.Stop()

	k := HTTPLabels{User: "u1", Route: "/api", Status: "200"}
	agg.Add(k, 5)
	agg.Add(k, 60)
	agg.Add(k, 70)
	agg.flush()

	row := rowFor(t, viewName, schema, k)
	if row == nil {
		t.Fatal("combination not recorded")
	}
	d, ok := row.Data.(*view.DistributionData)
	if !ok {
		t.Fatalf("unexpected data type: %T", row.Data)
	}
	if d.Count != 3 {
		t.Errorf("count = %d; want 3", d.Count)
	}
	if d.Sum() != 135 {
		t.Errorf("sum = %v; want 135", d.Sum())
	}
}
