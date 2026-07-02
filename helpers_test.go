package opencensus

// Helpers shared by the tests (extracted here to avoid coupling test files to one
// another).

import (
	"strconv"
	"sync/atomic"
	"testing"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

func newTagKey(t *testing.T, name string) tag.Key {
	t.Helper()
	k, err := tag.NewKey(name)
	if err != nil {
		t.Fatalf("tag.NewKey(%q): %v", name, err)
	}
	return k
}

var viewSeq atomic.Int64

// uniqPrefix gives a unique prefix for measure/view names per test.
func uniqPrefix() string {
	return "t_" + strconv.FormatInt(viewSeq.Add(1), 10)
}

func mustRegister(t *testing.T, vs ...*view.View) {
	t.Helper()
	if err := view.Register(vs...); err != nil {
		t.Fatalf("view.Register: %v", err)
	}
	t.Cleanup(func() { view.Unregister(vs...) })
}

func tagsToMap(tags []tag.Tag) map[string]string {
	m := make(map[string]string, len(tags))
	for _, tg := range tags {
		m[tg.Key.Name()] = tg.Value
	}
	return m
}

// countStore counts the live entries in the store (white-box), generic.
func countStore[K comparable, A any](s *shardedStore[K, A]) int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.Lock()
		n += len(sh.m)
		sh.mu.Unlock()
	}
	return n
}

// newHTTPFixture creates keys + schema + an isolated view of the given type.
func newHTTPFixture(t *testing.T, agg *view.Aggregation) (HTTPSchema, *stats.Float64Measure, string) {
	t.Helper()
	p := uniqPrefix()
	schema := HTTPSchema{
		KeyUser:   newTagKey(t, p+"_u"),
		KeyRoute:  newTagKey(t, p+"_r"),
		KeyStatus: newTagKey(t, p+"_s"),
	}
	m := stats.Float64(p+"/m", "v", stats.UnitDimensionless)
	viewName := p + "/v"
	v := &view.View{
		Name:        viewName,
		Measure:     m,
		TagKeys:     []tag.Key{schema.KeyUser, schema.KeyRoute, schema.KeyStatus},
		Aggregation: agg,
	}
	mustRegister(t, v)
	return schema, m, viewName
}

// combosInView extracts the set of HTTPLabels present in a view.
func combosInView(t *testing.T, viewName string, s HTTPSchema) map[HTTPLabels]bool {
	t.Helper()
	rows, err := view.RetrieveData(viewName)
	if err != nil {
		t.Fatalf("view.RetrieveData(%q): %v", viewName, err)
	}
	out := make(map[HTTPLabels]bool, len(rows))
	for _, row := range rows {
		mm := tagsToMap(row.Tags)
		out[HTTPLabels{
			User:   mm[s.KeyUser.Name()],
			Route:  mm[s.KeyRoute.Name()],
			Status: mm[s.KeyStatus.Name()],
		}] = true
	}
	return out
}

// tagKeys returns the tag.Key values of an HTTPSchema in order.
func tagKeys(s HTTPSchema) []tag.Key {
	return []tag.Key{s.KeyUser, s.KeyRoute, s.KeyStatus}
}
