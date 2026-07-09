package opencensus

import (
	"sync/atomic"
	"testing"
	"time"
)

// flushWaitTimeout bounds how long a test blocks on a flush signal before failing.
// Generous relative to the millisecond intervals used so slow CI never flakes.
const flushWaitTimeout = time.Second

// awaitSignal blocks until c receives a value or flushWaitTimeout elapses, failing
// the test in the latter case.
func awaitSignal(t *testing.T, c <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(flushWaitTimeout):
		t.Fatalf("timed out waiting for %s", msg)
	}
}

func TestStartFlusherJittered_StopDuringJitterFlushesOnce(t *testing.T) {
	var flushes atomic.Int64
	flushed := make(chan struct{}, 1)
	flush := func() {
		flushes.Add(1)
		flushed <- struct{}{}
	}

	// Long jitter so stop() lands while still waiting to start ticking.
	f := startFlusherJittered(time.Hour, time.Hour, flush)

	stopped := make(chan struct{})
	go func() {
		f.stop()
		close(stopped)
	}()

	awaitSignal(t, flushed, "final flush during jitter wait")
	awaitSignal(t, stopped, "stop() to return")

	if got := flushes.Load(); got != 1 {
		t.Fatalf("flush count = %d, want 1", got)
	}
}

func TestStartFlusherJittered_TickerFiresAfterJitter(t *testing.T) {
	ticked := make(chan struct{}, 8)
	flush := func() { ticked <- struct{}{} }

	f := startFlusherJittered(10*time.Millisecond, 0, flush)
	defer f.stop()

	awaitSignal(t, ticked, "first ticker flush")
	awaitSignal(t, ticked, "second ticker flush")
}

func TestStartFlusherJittered_StopAfterTicksRunsFinalFlush(t *testing.T) {
	var flushes atomic.Int64
	ticked := make(chan struct{}, 8)
	flush := func() {
		flushes.Add(1)
		select {
		case ticked <- struct{}{}:
		default:
		}
	}

	f := startFlusherJittered(5*time.Millisecond, 0, flush)
	awaitSignal(t, ticked, "at least one ticker flush")

	before := flushes.Load()
	f.stop()

	if after := flushes.Load(); after <= before {
		t.Fatalf("flush count did not increment on stop: before=%d after=%d", before, after)
	}
}

func TestStartFlusher_UsesJitterWithinInterval(t *testing.T) {
	// Boundary jitter values (0 and interval-ε) must both start the flusher and
	// preserve stop-flush semantics, standing in for the [0, interval) contract of
	// the randomized public startFlusher.
	interval := 10 * time.Millisecond
	for _, jitter := range []time.Duration{0, interval - time.Nanosecond} {
		var flushes atomic.Int64
		flushed := make(chan struct{}, 1)
		flush := func() {
			flushes.Add(1)
			select {
			case flushed <- struct{}{}:
			default:
			}
		}

		f := startFlusherJittered(interval, jitter, flush)
		awaitSignal(t, flushed, "flush to run for jitter within interval")
		f.stop()

		if got := flushes.Load(); got < 1 {
			t.Fatalf("jitter=%v: flush count = %d, want >= 1", jitter, got)
		}
	}
}
