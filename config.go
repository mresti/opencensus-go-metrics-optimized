package opencensus

import (
	"time"
)

// Config holds the settings shared by every aggregator variant: the shard count,
// the flush interval and the key projection Schema.
//
// Cardinality is the dimension that drives cost. Flush is O(distinct keys): each
// flush does one ctxCache lookup plus one stats.Record per key, and every record
// is funneled to the single global OpenCensus worker goroutine. The ctxCache
// (see ctxcache.go) grows with the number of distinct keys ever seen and is never
// evicted, so with unbounded label cardinality you must cap or project keys via
// Schema. Rough guidance:
//   - ≲1k keys: Interval 5–10s is fine.
//   - 10k–100k+ keys: prefer Interval 10–15s and set MaxSamplesPerKey on
//     distributions to bound memory.
//   - Do not go below ~5s at high cardinality: flush bursts can saturate the
//     OpenCensus worker channel and block the flusher goroutine.
//
// Shards affect writer contention (concurrent Add goroutines), not key count.
type Config[K comparable] struct {
	Shards int // rounded up to a power of 2. Default 16.

	// Interval is the flush cadence. Default 10s.
	//
	// Tuning rule: set Interval to an exact divisor of the exporter reporting
	// period (view.SetReportingPeriod) and at most half of it. For the common
	// 30s period use 10s or 15s; 10s also divides the common 60s period, hence
	// the default. Interval equal to the reporting period causes phase races
	// (export windows with no fresh flush); non-divisor intervals cause sawtooth
	// in delta/rate charts.
	//
	// For LastValue gauges, Interval is the maximum staleness at export time.
	// For distributions with MaxSamplesPerKey, the reservoir resets each flush,
	// so shorter intervals improve percentile fidelity.
	Interval time.Duration

	Schema Schema[K] // key projection strategy
}

func (c *Config[K]) applyDefaults() {
	if c.Shards <= 0 {
		c.Shards = 16
	}
	if c.Interval <= 0 {
		c.Interval = 10 * time.Second
	}
}
