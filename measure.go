package opencensus

import (
	"go.opencensus.io/stats"
)

// Number are the value types supported by OpenCensus measures.
type Number interface{ ~int64 | ~float64 }

// Measure is satisfied by *stats.Float64Measure (N=float64) and
// *stats.Int64Measure (N=int64). A parametrized interface is needed because M has
// a different signature on each concrete measure, so a union constraint alone
// cannot expose it directly.
type Measure[N Number] interface {
	M(v N) stats.Measurement
}
