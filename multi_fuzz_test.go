package opencensus

// Fuzz test for the multi-metric aggregator. A fuzzed byte stream is decoded into a
// sequence of (key, slot, value) Adds over a fixed mix of Count/Sum/LastValue slots,
// then the internal accumulator is checked against a reference model computed in the
// test. Assertions read the store directly (not a view) for the same reason as
// fuzz_test.go: fuzzed tag values are routinely non-ASCII, which OpenCensus rejects.
//
// Invariants verified per key/slot:
//   - Count slot  == number of Adds to it
//   - Sum slot    == float sum of its Adds (NaN-aware)
//   - LastValue   == last value written, or the zero value if never written
//   - touched bit set iff the slot received at least one Add
//   - number of emitted measurements matches the SkipZeros / LastValue-touched rules
//   - flush never panics on arbitrary input and fully drains the store

import (
	"encoding/binary"
	"math"
	"strconv"
	"testing"
	"time"

	"go.opencensus.io/stats"
)

const (
	fuzzMultiSlots = 6
	fuzzMultiKeyN  = 4
)

var (
	fuzzMultiKinds = [fuzzMultiSlots]metricKind{
		kindCount, kindCount, kindSum, kindSum, kindLastValue, kindLastValue,
	}
	fuzzMultiKeys = [fuzzMultiKeyN]HTTPLabels{
		{User: "u0", Route: "/a", Status: "200"},
		{User: "u1", Route: "/b", Status: "404"},
		{User: "u2", Route: "/c", Status: "500"},
		{User: "u3", Route: "/d", Status: "503"},
	}
	fuzzMultiMeasures = func() [fuzzMultiSlots]Measure[float64] {
		var m [fuzzMultiSlots]Measure[float64]
		for i := range m {
			m[i] = stats.Float64("fuzz/multi_"+strconv.Itoa(i), "v", stats.UnitDimensionless)
		}
		return m
	}()
)

func newFuzzMultiAggregator(
	skipZeros bool,
) (*MultiAggregator[HTTPLabels, float64], []MultiHandle[HTTPLabels, float64]) {
	b := NewMultiBuilder[HTTPLabels, float64](MultiConfig[HTTPLabels, float64]{
		Config:    Config[HTTPLabels]{Shards: 4, Interval: time.Hour, Schema: fuzzSchema},
		SkipZeros: skipZeros,
	})
	handles := make([]MultiHandle[HTTPLabels, float64], fuzzMultiSlots)
	for i, kind := range fuzzMultiKinds {
		switch kind {
		case kindCount:
			handles[i] = b.Count(fuzzMultiMeasures[i])
		case kindSum:
			handles[i] = b.Sum(fuzzMultiMeasures[i])
		case kindLastValue:
			handles[i] = b.LastValue(fuzzMultiMeasures[i])
		}
	}
	return b.Build(), handles
}

type fuzzMultiOp struct {
	keyIdx int
	slot   int
	v      float64
}

// decodeMultiOps reads 9-byte records (1 selector byte + 8 float64 bits) so a single
// []byte fuzz input drives a whole Add sequence across keys and slots.
func decodeMultiOps(raw []byte) []fuzzMultiOp {
	const opLen = 9
	n := min(len(raw)/opLen, maxFuzzValues)
	ops := make([]fuzzMultiOp, n)
	for i := range n {
		base := i * opLen
		sel := int(raw[base])
		bits := binary.LittleEndian.Uint64(raw[base+1 : base+9])
		ops[i] = fuzzMultiOp{
			keyIdx: (sel / fuzzMultiSlots) % fuzzMultiKeyN,
			slot:   sel % fuzzMultiSlots,
			v:      math.Float64frombits(bits),
		}
	}
	return ops
}

func peekMultiAcc(s *shardedStore[HTTPLabels, multiAcc[float64]], k HTTPLabels) (multiAcc[float64], bool) {
	sh := s.shardFor(k)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	acc, ok := sh.m[k]
	if !ok {
		return multiAcc[float64]{}, false
	}
	return *acc, true
}

// multiModel is the reference accumulator the fuzz body rebuilds independently of the
// aggregator to cross-check its internal state.
type multiModel struct {
	skipZeros bool
	counts    [fuzzMultiKeyN][fuzzMultiSlots]int
	sums      [fuzzMultiKeyN][fuzzMultiSlots]float64
	lasts     [fuzzMultiKeyN][fuzzMultiSlots]float64
	touched   [fuzzMultiKeyN][fuzzMultiSlots]bool
}

func (m *multiModel) apply(op fuzzMultiOp) {
	m.touched[op.keyIdx][op.slot] = true
	switch fuzzMultiKinds[op.slot] {
	case kindCount:
		m.counts[op.keyIdx][op.slot]++
	case kindSum:
		m.sums[op.keyIdx][op.slot] += op.v
	case kindLastValue:
		m.lasts[op.keyIdx][op.slot] = op.v
	}
}

func (m *multiModel) keyTouched(ki int) bool {
	for s := range fuzzMultiKinds {
		if m.touched[ki][s] {
			return true
		}
	}
	return false
}

func (m *multiModel) slotValue(ki, s int) float64 {
	switch fuzzMultiKinds[s] {
	case kindCount:
		return float64(m.counts[ki][s])
	case kindSum:
		return m.sums[ki][s]
	default:
		return m.lasts[ki][s]
	}
}

// expectedEmitted mirrors MultiAggregator.measurementsFor: LastValue emits only when
// touched; Count/Sum emit always, or only when non-zero under SkipZeros.
func (m *multiModel) expectedEmitted(ki int) int {
	want := 0
	for s := range fuzzMultiKinds {
		if fuzzMultiKinds[s] == kindLastValue {
			if m.touched[ki][s] {
				want++
			}
			continue
		}
		if m.skipZeros && m.slotValue(ki, s) == 0 {
			continue
		}
		want++
	}
	return want
}

func FuzzMultiAggregator(f *testing.F) {
	oneAdd := []byte{0x00, 0, 0, 0, 0, 0, 0, 0x24, 0x40} // slot0 key0, 10.0
	acrossSlots := make([]byte, 9*fuzzMultiSlots)        // one Add per slot on key0
	for s := range fuzzMultiSlots {
		acrossSlots[s*9] = byte(s)
	}
	f.Add(false, oneAdd)
	f.Add(true, oneAdd)
	f.Add(false, acrossSlots)
	f.Add(true, acrossSlots)
	f.Add(false, []byte{})
	f.Add(true, []byte{0x0a, 0, 0, 0, 0, 0, 0, 0, 0}) // sum slot set to 0.0

	f.Fuzz(func(t *testing.T, skipZeros bool, raw []byte) {
		agg, handles := newFuzzMultiAggregator(skipZeros)
		defer agg.Stop()

		ops := decodeMultiOps(raw)
		model := multiModel{skipZeros: skipZeros}
		for _, op := range ops {
			handles[op.slot].Add(fuzzMultiKeys[op.keyIdx], op.v)
			model.apply(op)
		}

		for ki := range fuzzMultiKeys {
			assertKey(t, agg, &model, ki)
		}

		agg.flush() // must never panic regardless of tag validity
		if n := countStore(agg.store); n != 0 {
			t.Fatalf("store no vacío tras flush: %d entradas", n)
		}
	})
}

func assertKey(t *testing.T, agg *MultiAggregator[HTTPLabels, float64], model *multiModel, ki int) {
	t.Helper()
	acc, ok := peekMultiAcc(agg.store, fuzzMultiKeys[ki])
	if want := model.keyTouched(ki); want != ok {
		t.Fatalf("key %d: presencia en store = %v; modelo = %v", ki, ok, want)
	}
	if !ok {
		return
	}

	for s := range fuzzMultiKinds {
		assertSlot(t, acc, model, ki, s)
	}

	got := len(agg.measurementsFor(&acc))
	if want := model.expectedEmitted(ki); got != want {
		t.Fatalf("key %d: measurements = %d; quiero %d (skipZeros=%v)", ki, got, want, model.skipZeros)
	}
}

func assertSlot(t *testing.T, acc multiAcc[float64], model *multiModel, ki, s int) {
	t.Helper()
	bit := acc.touched&(uint64(1)<<s) != 0
	if bit != model.touched[ki][s] {
		t.Fatalf("key %d slot %d: touched = %v; quiero %v", ki, s, bit, model.touched[ki][s])
	}

	got := acc.vals[s]
	switch fuzzMultiKinds[s] {
	case kindCount:
		if got != float64(model.counts[ki][s]) {
			t.Fatalf("key %d count slot %d = %v; quiero %d", ki, s, got, model.counts[ki][s])
		}
	case kindSum:
		if !floatsEqual(got, model.sums[ki][s]) {
			t.Fatalf("key %d sum slot %d = %v; quiero %v", ki, s, got, model.sums[ki][s])
		}
	case kindLastValue:
		if model.touched[ki][s] {
			if !floatsEqual(got, model.lasts[ki][s]) {
				t.Fatalf("key %d lastvalue slot %d = %v; quiero %v", ki, s, got, model.lasts[ki][s])
			}
		} else if got != 0 {
			t.Fatalf("key %d lastvalue slot %d no tocado = %v; quiero 0", ki, s, got)
		}
	}
}
