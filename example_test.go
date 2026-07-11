package opencensus

// Example of a concrete Schema for the original case (user/route/status). It serves
// as a reference: for a different set of properties, define your own key K and your
// Schema[K] the same way (with the fields you need).

import (
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

// HTTPLabels is an example labels key. Any comparable struct works.
type HTTPLabels struct {
	User   string
	Route  string
	Status string
}

// HTTPSchema implements Schema[HTTPLabels].
type HTTPSchema struct {
	KeyUser   tag.Key
	KeyRoute  tag.Key
	KeyStatus tag.Key
}

// Hash computes the shard hash for k from its user, route and status fields.
func (HTTPSchema) Hash(k HTTPLabels) uint64 {
	return hashStrings(k.User, k.Route, k.Status)
}

// Mutators returns the tag.Mutator values that upsert the user, route and status
// labels into an OpenCensus context.
func (s HTTPSchema) Mutators(k HTTPLabels) []tag.Mutator {
	return []tag.Mutator{
		tag.Upsert(s.KeyUser, k.User),
		tag.Upsert(s.KeyRoute, k.Route),
		tag.Upsert(s.KeyStatus, k.Status),
	}
}

// Compile-time conformance of the three variants with the interface.
var (
	_ Schema[HTTPLabels] = HTTPSchema{}

	_ Aggregator[HTTPLabels, float64] = (*CountAggregator[HTTPLabels, float64])(nil)
	_ Aggregator[HTTPLabels, float64] = (*SumAggregator[HTTPLabels, float64])(nil)
	_ Aggregator[HTTPLabels, float64] = (*DistributionAggregator[HTTPLabels, float64])(nil)
	_ Aggregator[HTTPLabels, float64] = (*LastValueAggregator[HTTPLabels, float64])(nil)

	_ Aggregator[HTTPLabels, int64] = (*CountAggregator[HTTPLabels, int64])(nil)
	_ Aggregator[HTTPLabels, int64] = (*SumAggregator[HTTPLabels, int64])(nil)
	_ Aggregator[HTTPLabels, int64] = (*DistributionAggregator[HTTPLabels, int64])(nil)
	_ Aggregator[HTTPLabels, int64] = (*LastValueAggregator[HTTPLabels, int64])(nil)
)

// ExampleNewMultiBuilder folds several metrics over the same HTTPLabels key into one
// aggregator: a single sharded store, flusher and ctxCache, and one stats.Record per
// key on flush. Each Count/Sum/LastValue registration returns a lightweight handle.
func ExampleNewMultiBuilder() {
	schema := HTTPSchema{
		KeyUser:   tag.MustNewKey("user"),
		KeyRoute:  tag.MustNewKey("route"),
		KeyStatus: tag.MustNewKey("status"),
	}

	requestMeasure := stats.Float64("myapp/requests", "HTTP requests", stats.UnitDimensionless)
	bytesMeasure := stats.Float64("myapp/bytes_out", "response bytes", stats.UnitBytes)
	inflightMeasure := stats.Float64("myapp/inflight", "in-flight requests", stats.UnitDimensionless)

	b := NewMultiBuilder[HTTPLabels, float64](MultiConfig[HTTPLabels, float64]{
		Config: Config[HTTPLabels]{
			Shards:   16,
			Interval: 10 * time.Second,
			Schema:   schema,
		},
		// SkipZeros: true, // omit Count/Sum slots still 0 at flush time
	})

	// Register every metric before Build; each call returns a handle to record against.
	requests := b.Count(requestMeasure)      // Count ignores the value: each Add is +1
	bytesOut := b.Sum(bytesMeasure)          // Sum accumulates the value
	inflight := b.LastValue(inflightMeasure) // LastValue keeps the last write

	agg := b.Build() // applies defaults, starts the background flusher
	defer agg.Stop() // final flush before returning

	k := HTTPLabels{User: "alice", Route: "/orders", Status: "200"}
	requests.Add(k, 1)
	bytesOut.Add(k, 2048)
	inflight.Add(k, 3)
}

// hashStrings computes FNV-1a inline over the string fields, without allocating on
// the heap (the variadic does not escape, the backing array goes on the stack). The
// separator avoids collisions of the form ("ab","c") vs ("a","bc").
func hashStrings(parts ...string) uint64 {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)
	h := offset
	for _, s := range parts {
		for i := 0; i < len(s); i++ {
			h ^= uint64(s[i])
			h *= prime
		}
		h ^= '|'
		h *= prime
	}
	return h
}
