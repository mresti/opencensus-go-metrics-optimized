package opencensus

// Example of a concrete Schema for the original case (user/route/status). It serves
// as a reference: for a different set of properties, define your own key K and your
// Schema[K] the same way (with the fields you need).

import "go.opencensus.io/tag"

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
