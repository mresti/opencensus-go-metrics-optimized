# opencensus-go-metrics-optimized

[![CI](https://github.com/mresti/opencensus-go-metrics-optimized/actions/workflows/ci.yml/badge.svg)](https://github.com/mresti/opencensus-go-metrics-optimized/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/mresti/opencensus-go-metrics-optimized.svg)](https://pkg.go.dev/github.com/mresti/opencensus-go-metrics-optimized)
[![Go Report Card](https://goreportcard.com/badge/github.com/mresti/opencensus-go-metrics-optimized)](https://goreportcard.com/report/github.com/mresti/opencensus-go-metrics-optimized)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)

Generic, sharded pre-aggregation for [OpenCensus](https://opencensus.io) Go metrics.

Calling `stats.Record` on every event builds a `tag.Map` and enqueues a `recordReq`
to OpenCensus's single global worker goroutine — under high throughput that worker
becomes a bottleneck and a point of lock contention. This library accumulates
values per label key across N in-memory shards and flushes them to OpenCensus in
bursts on a configurable interval, cutting the number of `stats.Record` calls (and
their allocations) by orders of magnitude without changing the semantics of the
underlying views.

## Features

- **Generic over your label key.** Define any comparable struct `K` as your label
  set and a `Schema[K]` that knows how to project it onto OpenCensus — no
  `map[string]interface{}` or reflection involved.
- **Generic over the measure value type `N`.** Each config takes a second type
  parameter `N` (`~int64 | ~float64`), so a variant can be backed by either a
  `*stats.Float64Measure` or a `*stats.Int64Measure`.
- **Four aggregator variants behind one interface** (`Aggregator[K, N]`): `Count`,
  `Sum`, `Distribution` (with optional bounded reservoir sampling), and
  `LastValue` (for gauges).
- **Multi-metric aggregation.** Fold many `Count`/`Sum`/`LastValue` metrics over the
  same key into one shared store and a single `stats.Record` per key — see
  [Multi-metric aggregation](#multi-metric-aggregation).
- **Sharded, lock-striped storage.** Writes only lock the shard for their key, not
  a single global mutex.
- **Allocation-free hot path.** After a key has been seen once, `Add` does not
  allocate; the per-key `context.Context` (with tags already applied) is memoized
  and reused across flushes.
- **Non-blocking flush.** Each shard's map is swapped out under its own lock so
  writers are never blocked by a flush in progress.

## Installation

```sh
go get github.com/mresti/opencensus-go-metrics-optimized
```

Requires Go 1.26+ (uses generics and `sync.WaitGroup.Go`).

## Quick start

1. Define your label key and a `Schema[K]` for it:

```go
package main

import "go.opencensus.io/tag"

type HTTPLabels struct {
	User   string
	Route  string
	Status string
}

type HTTPSchema struct {
	KeyUser   tag.Key
	KeyRoute  tag.Key
	KeyStatus tag.Key
}

// Hash is called on every Add: keep it cheap and non-allocating.
func (HTTPSchema) Hash(k HTTPLabels) uint64 {
	return hashStrings(k.User, k.Route, k.Status)
}

// Mutators is called only on flush, once per distinct combination.
func (s HTTPSchema) Mutators(k HTTPLabels) []tag.Mutator {
	return []tag.Mutator{
		tag.Upsert(s.KeyUser, k.User),
		tag.Upsert(s.KeyRoute, k.Route),
		tag.Upsert(s.KeyStatus, k.Status),
	}
}
```

2. Create an aggregator and record events against it instead of calling
   `stats.Record` directly:

```go
package main

import (
	"time"

	oc "github.com/mresti/opencensus-go-metrics-optimized"
	"go.opencensus.io/stats"
)

var requestCount = stats.Float64("myapp/requests", "HTTP requests", stats.UnitDimensionless)

func main() {
	schema := HTTPSchema{ /* register tag.Key's via tag.NewKey */ }

	agg := oc.NewCountAggregator(oc.CountConfig[HTTPLabels, float64]{
		Config: oc.Config[HTTPLabels]{
			Shards:   16,               // default 16, rounded up to a power of 2
			Interval: 10 * time.Second, // default 10s
			Schema:   schema,
		},
		CountMeasure: requestCount,
	})
	defer agg.Stop() // flushes any remaining data before returning

	agg.Add(HTTPLabels{User: "alice", Route: "/orders", Status: "200"}, 1)
}
```

`Stop` performs a final flush before returning, so no data is lost on shutdown.

## Aggregator variants

All variants implement the same interface:

```go
type Aggregator[K comparable, N Number] interface {
	Add(k K, value N)
	Stop()
}
```

| Constructor                        | Config                     | Use case                                                        |
|------------------------------------|----------------------------|-------------------------------------------------------------------|
| `NewCountAggregator[K, N]`         | `CountConfig[K, N]`        | Counting occurrences per key (equivalent to `view.Count()`).       |
| `NewSumAggregator[K, N]`           | `SumConfig[K, N]`          | Summing a numeric value per key (equivalent to `view.Sum()`).      |
| `NewDistributionAggregator[K, N]`  | `DistributionConfig[K, N]` | Latency/size histograms; supports `MaxSamplesPerKey` reservoir sampling to bound memory under high/unbounded cardinality per key. |
| `NewLastValueAggregator[K, N]`     | `LastValueConfig[K, N]`    | Gauges; the measure's view **must** use `view.LastValue()`.        |

The value type `N` is inferred from the config literal, e.g.
`SumConfig[HTTPLabels, float64]` (Float64 measure) or
`SumConfig[HTTPLabels, int64]` (Int64 measure).

## Multi-metric aggregation

When several metrics share the **same** label key `K`, running one aggregator each
means N shard lookups, N locks and N `ctxCache` entries per event, plus N
`stats.Record` calls per key on every flush. `MultiAggregator` folds up to **64**
`Count`/`Sum`/`LastValue` metrics over the same `K` into a **single** sharded store,
flusher and `ctxCache`, emitting **one `stats.Record` per key** carrying every
metric at once.

Declare metrics with a builder; each registration returns a lightweight handle you
record against:

```go
b := oc.NewMultiBuilder[HTTPLabels, float64](oc.MultiConfig[HTTPLabels, float64]{
	Config:    oc.Config[HTTPLabels]{Shards: 16, Interval: 10 * time.Second, Schema: schema},
	SkipZeros: false, // if true, omit Count/Sum slots still 0 at flush time
})

requests := b.Count(requestMeasure)     // Count ignores the value: each Add is +1
bytesOut := b.Sum(bytesMeasure)         // Sum accumulates the value
inflight := b.LastValue(gaugeMeasure)   // LastValue keeps the last write

agg := b.Build() // applies defaults, starts the flusher
defer agg.Stop() // final flush on shutdown

k := HTTPLabels{User: "alice", Route: "/orders", Status: "200"}
requests.Add(k, 1)
bytesOut.Add(k, 2048)
inflight.Add(k, 3)
```

### Semantics

- **Count** ignores the value passed to `Add` and increments by one.
- **Sum** accumulates the value.
- **LastValue** overwrites (last-write-wins under the shard lock) and is emitted on
  flush **only if its slot was written that window** — an untouched gauge is never
  fabricated as 0, but an explicit `Add(k, 0)` *is* emitted (0 is a legitimate
  gauge reading).
- **`SkipZeros`** applies only to `Count`/`Sum`: when true, slots still at 0 at
  flush time are omitted. It never affects `LastValue`.
- Handles are values — copy them freely; every copy writes the same slot. Only
  `MultiAggregator` has `Stop()`; handles are pure write endpoints and do **not**
  implement `Aggregator[K, N]`.
- Registering a metric after `Build`, calling `Build` twice, exceeding 64 metrics,
  or passing a nil measure all **panic** (programming errors, surfaced early).

### One multi per schema

Group into a single `MultiAggregator` the metrics that share the same key `K` /
`Schema`. For metrics whose labels differ — e.g. HTTP request metrics vs. database
query metrics — use **one `MultiAggregator` per domain**; do not merge schemas.
`stats.Record` carries a single `context.Context`, so metrics with different tag sets
can never share a `Record`, and a unified store would only move tag projection onto
the hot path and risk emitting empty/zero tags into the wrong views. The extra cost
of a second aggregator is just one goroutine/ticker per schema, while separate stores
*improve* parallel-write throughput (see [Performance](#performance)) and let each
flusher keep its own anti-burst startup jitter. See the `ExampleNewMultiBuilder`
two-schemas example in the [Go reference](https://pkg.go.dev/github.com/mresti/opencensus-go-metrics-optimized).

### Cardinality

The shared `ctxCache` holds one context per distinct key regardless of metric count,
so memory scales with keys, not keys×metrics — roughly **1/N** the ctxCache
footprint of N separate aggregators, and **N× fewer** `stats.Record` calls per
flush. This directly eases the high-cardinality guidance in
[Configuration](#configuration) about not dropping `Interval` too low.

### Performance

Benchmarks on the target 4-`Count` + 9-`Sum` layout (`-benchmem`), multi vs. the
equivalent 13 separate aggregators:

| Scenario | Multi | Separate | Notes |
|---|---|---|---|
| Single-metric `Add` (one metric per event) | ~47 ns/op, 0 allocs | ~51 ns/op, 0 allocs | Handle adds no measurable overhead. |
| One event → all 13 metrics | ~446 ns/op, 0 allocs | ~589 ns/op, 0 allocs | Multi ~24% faster (one accumulator slice, better locality). |
| Flush 1 000 keys × 13 metrics | ~2.7 ms/op | ~6.0 ms/op | Multi ~2.2× faster, ~half the allocs (one record + ctx lookup per key). |
| Parallel `Add`, 8 cores | ~40 ns/op | ~23 ns/op | **Separate wins**: 13 independent stores spread lock contention across 13×`Shards` mutexes. |

The parallel-write result is the cost of a single shared store. If write contention
dominates your workload, raise `Shards` on the `MultiConfig` to widen the one store
(e.g. 64–128) — the flush and locality benefits are unaffected.

## Configuration

`Config[K]` is embedded in every variant's config struct:

```go
type Config[K comparable] struct {
	Shards   int           // rounded up to a power of 2. Default 16.
	Interval time.Duration // flush cadence. Default 10s.
	Schema   Schema[K]     // key -> OpenCensus projection strategy
}
```

- **Shards** trades memory/lock granularity for contention: more shards means
  less contention under concurrent `Add` calls from many goroutines.
- **Interval** trades staleness for the volume of `stats.Record` traffic sent to
  the OpenCensus worker (default 10s). Set it to an exact divisor of the exporter
  reporting period (`view.SetReportingPeriod`) and at most half of it — e.g. 10s
  or 15s for a 30s period; 10s also divides the common 60s period. A non-divisor
  interval causes sawtooth in delta/rate charts. Aggregators apply a random
  startup delay in `[0, Interval)` so instances created together don't all flush
  on the same tick and burst the OpenCensus worker.

## Design notes

- The per-key `context.Context` cache (`ctxCache`) grows with the number of
  **distinct** keys observed. It is stable under bounded label cardinality; for
  unbounded/high-cardinality label sets, keep `K`'s cardinality bounded upstream
  (e.g. bucket free-form values) or clear the aggregator periodically.
- `Distribution`'s `MaxSamplesPerKey` (when > 0) uses reservoir sampling so memory
  per key is bounded regardless of how many samples arrive between flushes,
  trading exactness for a bounded, uniformly-sampled subset.
- Concurrency-sensitive internals (sharded store, contract behavior across the
  three variants) are covered by a shared contract test suite and fuzz tests; see
  [Testing](#testing).

## Testing

The project uses `make` for all common workflows — run `make help` to list
targets. The most relevant ones:

```sh
make test           # go test -race -cover ./...
make test-bench     # benchmarks across the aggregator variants
make test-fuzz       # replay the committed Fuzz* seed corpus (fast, CI-safe)
make test-fuzz-one FUZZ=FuzzCountAggregator_Add FUZZTIME=1m  # fuzz one target
make lint            # golangci-lint (run `make tools` once to install it)
make ci              # vet + lint + test — what CI runs
```

## Contributing

Issues and pull requests are welcome. Please run `make ci` locally before
submitting a PR — the same target gates CI on GitHub Actions.

## License

Licensed under either of Apache License, Version 2.0 ([LICENSE](LICENSE) or http://www.apache.org/licenses/LICENSE-2.0)
