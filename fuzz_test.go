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

func peekSumAcc(s *shardedStore[HTTPLabels, sumCountAcc], k HTTPLabels) (sumCountAcc, bool) {
	sh := s.shardFor(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	acc, ok := sh.m[k]
	if !ok {
		return sumCountAcc{}, false
	}
	return *acc, true
}

func peekLastValueAcc(s *shardedStore[HTTPLabels, lastValueAcc], k HTTPLabels) (lastValueAcc, bool) {
	sh := s.shardFor(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	acc, ok := sh.m[k]
	if !ok {
		return lastValueAcc{}, false
	}
	return *acc, true
}

func peekDistAcc(s *shardedStore[HTTPLabels, distAcc], k HTTPLabels) (distAcc, bool) {
	sh := s.shardFor(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	acc, ok := sh.m[k]
	if !ok {
		return distAcc{}, false
	}
	return *acc, true
}

func FuzzCountAggregator_Add(f *testing.F) {
	f.Add("u1", "/a", "200", []byte{0, 0, 0, 0, 0, 0, 0x24, 0x40})
	f.Add("", "", "", []byte{})
	f.Add("ключ", "/ünïcödé", strings.Repeat("z", 300), make([]byte, 64))

	f.Fuzz(func(t *testing.T, user, route, status string, raw []byte) {
		agg := NewCountAggregator(CountConfig[HTTPLabels]{
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
			t.Fatalf("entrada inesperada en el store sin llamadas a Add")
		case len(vals) > 0 && !ok:
			t.Fatalf("falta la entrada en el store tras %d Add", len(vals))
		case ok && acc.count != int64(len(vals)):
			t.Fatalf("count = %d; quiero %d", acc.count, len(vals))
		}

		agg.flush() // no debe paniquear pase lo que pase con los tags
		if n := countStore(agg.store); n != 0 {
			t.Fatalf("store no vacío tras flush: %d entradas", n)
		}
	})
}

func FuzzSumAggregator_Add(f *testing.F) {
	f.Add("u1", "/a", "200", []byte{0, 0, 0, 0, 0, 0, 0x24, 0x40})
	f.Add("", "", "", []byte{})
	f.Add("u2", "/b", "500", []byte{0, 0, 0, 0, 0, 0, 0xf0, 0x7f, 0, 0, 0, 0, 0, 0, 0xf0, 0xff}) // +Inf, -Inf

	f.Fuzz(func(t *testing.T, user, route, status string, raw []byte) {
		agg := NewSumAggregator(SumConfig[HTTPLabels]{
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
			t.Fatalf("entrada inesperada en el store sin llamadas a Add")
		case len(vals) > 0 && !ok:
			t.Fatalf("falta la entrada en el store tras %d Add", len(vals))
		case ok && !floatsEqual(acc.sum, want):
			t.Fatalf("sum = %v; quiero %v", acc.sum, want)
		}

		agg.flush()
		if n := countStore(agg.store); n != 0 {
			t.Fatalf("store no vacío tras flush: %d entradas", n)
		}
	})
}

func FuzzLastValueAggregator_Add(f *testing.F) {
	f.Add("u1", "/a", "200", []byte{0, 0, 0, 0, 0, 0, 0x24, 0x40})
	f.Add("", "", "", []byte{})
	f.Add("u2", "/b", "500", make([]byte, 40))

	f.Fuzz(func(t *testing.T, user, route, status string, raw []byte) {
		agg := NewLastValueAggregator(LastValueConfig[HTTPLabels]{
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
			t.Fatalf("entrada inesperada en el store sin llamadas a Add")
		case len(vals) > 0 && !ok:
			t.Fatalf("falta la entrada en el store tras %d Add", len(vals))
		case ok && !floatsEqual(acc.value, vals[len(vals)-1]):
			t.Fatalf("last value = %v; quiero %v", acc.value, vals[len(vals)-1])
		}

		agg.flush()
		if n := countStore(agg.store); n != 0 {
			t.Fatalf("store no vacío tras flush: %d entradas", n)
		}
	})
}

func FuzzDistributionAggregator_Add(f *testing.F) {
	f.Add("u1", "/a", "200", uint8(0), []byte{0, 0, 0, 0, 0, 0, 0x24, 0x40})
	f.Add("u2", "/b", "500", uint8(3), make([]byte, 400))
	f.Add("", "", "", uint8(1), []byte{})

	f.Fuzz(func(t *testing.T, user, route, status string, maxSamplesRaw uint8, raw []byte) {
		maxSamples := int(maxSamplesRaw % 21) // cap reservoir size to keep runs fast: 0..20
		agg := NewDistributionAggregator(DistributionConfig[HTTPLabels]{
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
				t.Fatalf("entrada inesperada en el store sin llamadas a Add")
			}
			agg.flush()
			if n := countStore(agg.store); n != 0 {
				t.Fatalf("store no vacío tras flush: %d entradas", n)
			}
			return
		}
		if !ok {
			t.Fatalf("falta la entrada en el store tras %d Add", len(vals))
		}
		if acc.seen != int64(len(vals)) {
			t.Fatalf("seen = %d; quiero %d", acc.seen, len(vals))
		}

		if maxSamples <= 0 {
			if len(acc.samples) != len(vals) {
				t.Fatalf("samples = %d; quiero %d (modo exacto)", len(acc.samples), len(vals))
			}
			for i, v := range vals {
				if !floatsEqual(acc.samples[i], v) {
					t.Fatalf("samples[%d] = %v; quiero %v", i, acc.samples[i], v)
				}
			}
		} else {
			wantLen := min(len(vals), maxSamples)
			if len(acc.samples) != wantLen {
				t.Fatalf("samples = %d; quiero %d (reservorio de %d)", len(acc.samples), wantLen, maxSamples)
			}
			counts, nanCount := multisetCount(vals)
			for _, s := range acc.samples {
				if math.IsNaN(s) {
					if nanCount == 0 {
						t.Fatalf("sample NaN no presente en los valores añadidos")
					}
					nanCount--
					continue
				}
				if counts[s] == 0 {
					t.Fatalf("sample %v no presente en los valores añadidos", s)
				}
				counts[s]--
			}
		}

		agg.flush()
		if n := countStore(agg.store); n != 0 {
			t.Fatalf("store no vacío tras flush: %d entradas", n)
		}
	})
}
