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

// DatabaseLabels is an example labels key. Any comparable struct works.
type DatabaseLabels struct {
	User     string
	Database string
	Status   string
}

// DatabaseSchema implements Schema[DatabaseLabels].
type DatabaseSchema struct {
	KeyUser     tag.Key
	KeyDatabase tag.Key
	KeyStatus   tag.Key
}

// Hash computes the shard hash for k from its user, database and status fields.
func (DatabaseSchema) Hash(k DatabaseLabels) uint64 {
	return hashStrings(k.User, k.Database, k.Status)
}

// Mutators returns the tag.Mutator values that upsert the user, database and status
// labels into an OpenCensus context.
func (s DatabaseSchema) Mutators(k DatabaseLabels) []tag.Mutator {
	return []tag.Mutator{
		tag.Upsert(s.KeyUser, k.User),
		tag.Upsert(s.KeyDatabase, k.Database),
		tag.Upsert(s.KeyStatus, k.Status),
	}
}

// Compile-time conformance of the three variants with the interface.
var (
	_ Schema[HTTPLabels]     = HTTPSchema{}
	_ Schema[DatabaseLabels] = DatabaseSchema{}

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

// ExampleNewMultiBuilder_twoSchemas runs two MultiAggregators side by side, one per
// label schema (HTTP and Database). The rule is to group metrics by the key they
// share: use ONE MultiAggregator per schema, never a single aggregator spanning
// schemas. stats.Record carries a single context, so metrics with different tag sets
// can never share a Record; keeping stores separate also spreads write contention
// and lets each flusher keep its own anti-burst startup jitter.
func ExampleNewMultiBuilder_twoSchemas() {
	httpSchema := HTTPSchema{
		KeyUser:   tag.MustNewKey("user"),
		KeyRoute:  tag.MustNewKey("route"),
		KeyStatus: tag.MustNewKey("status"),
	}
	dbSchema := DatabaseSchema{
		KeyUser:     tag.MustNewKey("db_user"),
		KeyDatabase: tag.MustNewKey("database"),
		KeyStatus:   tag.MustNewKey("db_status"),
	}

	// HTTP domain: metrics keyed by HTTPLabels.
	httpRequests := stats.Float64("myapp/http_requests", "HTTP requests", stats.UnitDimensionless)
	httpB := NewMultiBuilder[HTTPLabels, float64](MultiConfig[HTTPLabels, float64]{
		Config: Config[HTTPLabels]{Interval: 10 * time.Second, Schema: httpSchema},
	})
	requests := httpB.Count(httpRequests)
	httpAgg := httpB.Build()
	defer httpAgg.Stop()

	// Database domain: metrics keyed by DatabaseLabels.
	dbQueries := stats.Float64("myapp/db_queries", "database queries", stats.UnitDimensionless)
	dbErrors := stats.Float64("myapp/db_query_errors", "failed queries", stats.UnitDimensionless)
	dbRows := stats.Float64("myapp/db_rows_read", "rows read", stats.UnitDimensionless)
	dbConns := stats.Float64("myapp/db_open_conns", "open connections", stats.UnitDimensionless)
	dbB := NewMultiBuilder[DatabaseLabels, float64](MultiConfig[DatabaseLabels, float64]{
		Config: Config[DatabaseLabels]{Interval: 10 * time.Second, Schema: dbSchema},
	})
	queries := dbB.Count(dbQueries)
	queryErrors := dbB.Count(dbErrors)
	rowsRead := dbB.Sum(dbRows)
	openConns := dbB.LastValue(dbConns)
	dbAgg := dbB.Build()
	defer dbAgg.Stop()

	requests.Add(HTTPLabels{User: "alice", Route: "/orders", Status: "200"}, 1)

	ok := DatabaseLabels{User: "alice", Database: "orders", Status: "ok"}
	queries.Add(ok, 1)   // one query
	rowsRead.Add(ok, 42) // that read 42 rows
	openConns.Add(ok, 7) // 7 connections open right now

	// Count ignores the value; call it once per failed query.
	queryErrors.Add(DatabaseLabels{User: "alice", Database: "orders", Status: "error"}, 1)
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
