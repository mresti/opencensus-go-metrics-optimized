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
			Interval: 20 * time.Second, // default 20s
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

## Configuration

`Config[K]` is embedded in every variant's config struct:

```go
type Config[K comparable] struct {
	Shards   int           // rounded up to a power of 2. Default 16.
	Interval time.Duration // flush cadence. Default 20s.
	Schema   Schema[K]     // key -> OpenCensus projection strategy
}
```

- **Shards** trades memory/lock granularity for contention: more shards means
  less contention under concurrent `Add` calls from many goroutines.
- **Interval** trades staleness for the volume of `stats.Record` traffic sent to
  the OpenCensus worker.

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
