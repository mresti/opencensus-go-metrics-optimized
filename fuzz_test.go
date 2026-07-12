package opencensus

// Fuzz tests for the four Aggregator variants. Each one asserts, from raw fuzzed
// bytes, that Add()/flush() never panics (regardless of key content or float64
// edge cases like NaN/Inf) and that the white-box accumulator state matches the
// semantics of the variant. Assertions read the internal store directly instead of
// going through an OpenCensus view, since OpenCensus tag values are restricted to
// ASCII and fuzzed strings routinely are not: the "invalid tag -> ctxCache errors ->
// flush skips silently" path is exactly what we want exercised, not worked around.

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

var (
	fuzzSchema = HTTPSchema{
		KeyUser:   tag.MustNewKey("fuzz_user"),
		KeyRoute:  tag.MustNewKey("fuzz_route"),
		KeyStatus: tag.MustNewKey("fuzz_status"),
	}
	fuzzCountMeasure = stats.Float64("fuzz/count", "v", stats.UnitDimensionless)
	fuzzSumMeasure   = stats.Float64("fuzz/sum", "v", stats.UnitDimensionless)
	fuzzLastMeasure  = stats.Float64("fuzz/last", "v", stats.UnitDimensionless)
	fuzzDistMeasure  = stats.Float64("fuzz/dist", "v", stats.UnitDimensionless)
)

// maxFuzzValues bounds how many float64 values a single fuzz input can decode into,
// keeping each Add sequence (and reservoir sampling inside it) fast.
const maxFuzzValues = 256

// decodeFloats reads up to maxFuzzValues float64 values (8 bytes each, little-endian
// bit pattern) out of raw, so a single []byte fuzz input can drive a whole Add sequence.
func decodeFloats(raw []byte) []float64 {
	n := min(len(raw)/8, maxFuzzValues)
	out := make([]float64, n)
	for i := range n {
		bits := binary.LittleEndian.Uint64(raw[i*8 : i*8+8])
		out[i] = math.Float64frombits(bits)
	}
	return out
}

func floatsEqual(a, b float64) bool {
	return a == b || (math.IsNaN(a) && math.IsNaN(b))
}

// multisetCount buckets vals by exact value for containment checks; NaNs (which are
// never == to themselves) are counted separately.
func multisetCount(vals []float64) (counts map[float64]int, nanCount int) {
	counts = make(map[float64]int, len(vals))
	for _, v := range vals {
		if math.IsNaN(v) {
			nanCount++
			continue
		}
		counts[v]++
	}
	return counts, nanCount
}

func peekCountAcc(s *shardedStore[HTTPLabels, countAcc], k HTTPLabels) (countAcc, bool) {
	sh := s.shardFor(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	acc, ok := sh.m[k]
	if !ok {
		return countAcc{}, false
	}
	return *acc, true
}

func peekSumAcc(s *shardedStore[HTTPLabels, sumCountAcc[float64]], k HTTPLabels) (sumCountAcc[float64], bool) {
	sh := s.shardFor(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	acc, ok := sh.m[k]
	if !ok {
		return sumCountAcc[float64]{}, false
	}
	return *acc, true
}

func peekLastValueAcc(s *shardedStore[HTTPLabels, lastValueAcc[float64]], k HTTPLabels) (lastValueAcc[float64], bool) {
	sh := s.shardFor(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	acc, ok := sh.m[k]
	if !ok {
		return lastValueAcc[float64]{}, false
	}
	return *acc, true
}

func peekDistAcc(s *shardedStore[HTTPLabels, distAcc[float64]], k HTTPLabels) (distAcc[float64], bool) {
	sh := s.shardFor(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	acc, ok := sh.m[k]
	if !ok {
		return distAcc[float64]{}, false
	}
	return *acc, true
}

func FuzzCountAggregator_Add(f *testing.F) {
	f.Add("u1", "/a", "200", []byte{0, 0, 0, 0, 0, 0, 0x24, 0x40})
	f.Add("", "", "", []byte{})
	f.Add("ключ", "/ünïcödé", strings.Repeat("z", 300), make([]byte, 64))

	f.Fuzz(func(t *testing.T, user, route, status string, raw []byte) {
		agg := NewCountAggregator(CountConfig[HTTPLabels, float64]{
			Config:       Config[HTTPLabels]{Shards: 4, Interval: time.Hour, Schema: fuzzSchema},
			CountMeasure: fuzzCountMeasure,
		})
		defer agg.Stop()

		k := HTTPLabels{User: user, Route: route, Status: status}
		vals := decodeFloats(raw)
		for _, v := range vals {
			agg.Add(k, v)
		}

		acc, ok := peekCountAcc(agg.store, k)
		switch {
		case len(vals) == 0 && ok:
			t.Fatalf("unexpected entry in the store with no Add calls")
		case len(vals) > 0 && !ok:
			t.Fatalf("missing store entry after %d Add", len(vals))
		case ok && acc.count != int64(len(vals)):
			t.Fatalf("count = %d; want %d", acc.count, len(vals))
		}

		agg.flush() // must not panic no matter what happens with the tags
		if n := countStore(agg.store); n != 0 {
			t.Fatalf("store not empty after flush: %d entries", n)
		}
	})
}

func FuzzSumAggregator_Add(f *testing.F) {
	f.Add("u1", "/a", "200", []byte{0, 0, 0, 0, 0, 0, 0x24, 0x40})
	f.Add("", "", "", []byte{})
	f.Add("u2", "/b", "500", []byte{0, 0, 0, 0, 0, 0, 0xf0, 0x7f, 0, 0, 0, 0, 0, 0, 0xf0, 0xff}) // +Inf, -Inf

	f.Fuzz(func(t *testing.T, user, route, status string, raw []byte) {
		agg := NewSumAggregator(SumConfig[HTTPLabels, float64]{
			Config:     Config[HTTPLabels]{Shards: 4, Interval: time.Hour, Schema: fuzzSchema},
			SumMeasure: fuzzSumMeasure,
		})
		defer agg.Stop()

		k := HTTPLabels{User: user, Route: route, Status: status}
		vals := decodeFloats(raw)

		var want float64
		for _, v := range vals {
			agg.Add(k, v)
			want += v
		}

		acc, ok := peekSumAcc(agg.store, k)
		switch {
		case len(vals) == 0 && ok:
			t.Fatalf("unexpected entry in the store with no Add calls")
		case len(vals) > 0 && !ok:
			t.Fatalf("missing store entry after %d Add", len(vals))
		case ok && !floatsEqual(acc.sum, want):
			t.Fatalf("sum = %v; want %v", acc.sum, want)
		}

		agg.flush()
		if n := countStore(agg.store); n != 0 {
			t.Fatalf("store not empty after flush: %d entries", n)
		}
	})
}

func FuzzLastValueAggregator_Add(f *testing.F) {
	f.Add("u1", "/a", "200", []byte{0, 0, 0, 0, 0, 0, 0x24, 0x40})
	f.Add("", "", "", []byte{})
	f.Add("u2", "/b", "500", make([]byte, 40))

	f.Fuzz(func(t *testing.T, user, route, status string, raw []byte) {
		agg := NewLastValueAggregator(LastValueConfig[HTTPLabels, float64]{
			Config:  Config[HTTPLabels]{Shards: 4, Interval: time.Hour, Schema: fuzzSchema},
			Measure: fuzzLastMeasure,
		})
		defer agg.Stop()

		k := HTTPLabels{User: user, Route: route, Status: status}
		vals := decodeFloats(raw)
		for _, v := range vals {
			agg.Add(k, v)
		}

		acc, ok := peekLastValueAcc(agg.store, k)
		switch {
		case len(vals) == 0 && ok:
			t.Fatalf("unexpected entry in the store with no Add calls")
		case len(vals) > 0 && !ok:
			t.Fatalf("missing store entry after %d Add", len(vals))
		case ok && !floatsEqual(acc.value, vals[len(vals)-1]):
			t.Fatalf("last value = %v; want %v", acc.value, vals[len(vals)-1])
		}

		agg.flush()
		if n := countStore(agg.store); n != 0 {
			t.Fatalf("store not empty after flush: %d entries", n)
		}
	})
}

func FuzzDistributionAggregator_Add(f *testing.F) {
	f.Add("u1", "/a", "200", uint8(0), []byte{0, 0, 0, 0, 0, 0, 0x24, 0x40})
	f.Add("u2", "/b", "500", uint8(3), make([]byte, 400))
	f.Add("", "", "", uint8(1), []byte{})

	f.Fuzz(func(t *testing.T, user, route, status string, maxSamplesRaw uint8, raw []byte) {
		maxSamples := int(maxSamplesRaw % 21) // cap reservoir size to keep runs fast: 0..20
		agg := NewDistributionAggregator(DistributionConfig[HTTPLabels, float64]{
			Config:           Config[HTTPLabels]{Shards: 4, Interval: time.Hour, Schema: fuzzSchema},
			Measure:          fuzzDistMeasure,
			MaxSamplesPerKey: maxSamples,
		})
		defer agg.Stop()

		k := HTTPLabels{User: user, Route: route, Status: status}
		vals := decodeFloats(raw)
		for _, v := range vals {
			agg.Add(k, v)
		}

		acc, ok := peekDistAcc(agg.store, k)
		if len(vals) == 0 {
			if ok {
				t.Fatalf("unexpected entry in the store with no Add calls")
			}
			agg.flush()
			if n := countStore(agg.store); n != 0 {
				t.Fatalf("store not empty after flush: %d entries", n)
			}
			return
		}
		if !ok {
			t.Fatalf("missing store entry after %d Add", len(vals))
		}
		if acc.seen != int64(len(vals)) {
			t.Fatalf("seen = %d; want %d", acc.seen, len(vals))
		}

		if maxSamples <= 0 {
			if len(acc.samples) != len(vals) {
				t.Fatalf("samples = %d; want %d (exact mode)", len(acc.samples), len(vals))
			}
			for i, v := range vals {
				if !floatsEqual(acc.samples[i], v) {
					t.Fatalf("samples[%d] = %v; want %v", i, acc.samples[i], v)
				}
			}
		} else {
			wantLen := min(len(vals), maxSamples)
			if len(acc.samples) != wantLen {
				t.Fatalf("samples = %d; want %d (reservoir of %d)", len(acc.samples), wantLen, maxSamples)
			}
			counts, nanCount := multisetCount(vals)
			for _, s := range acc.samples {
				if math.IsNaN(s) {
					if nanCount == 0 {
						t.Fatalf("sample NaN not present in the added values")
					}
					nanCount--
					continue
				}
				if counts[s] == 0 {
					t.Fatalf("sample %v not present in the added values", s)
				}
				counts[s]--
			}
		}

		agg.flush()
		if n := countStore(agg.store); n != 0 {
			t.Fatalf("store not empty after flush: %d entries", n)
		}
	})
}
