package opencensus

// Schema is the strategy (Strategy pattern) that makes the aggregator generic: the
// user defines their own labels key K (any comparable struct, with the fields they
// need) and a Schema[K] that knows how to project it onto OpenCensus.
//
//   - Hash:     distributes the key across shards. It is called on the HOT PATH, so
//               it must be stable, cheap and non-allocating. Use HashStrings for the
//               string fields.
//   - Mutators: produces the tag.Mutator values to build the context. It is called
//               only on the flush (and once per combination thanks to the ctxCache),
//               so allocating the slice here is acceptable.

import "go.opencensus.io/tag"

// Schema is the strategy that projects a labels key K onto OpenCensus: Hash
// distributes the key across shards on the hot path, and Mutators builds the
// tag.Mutator values used to derive the recording context.
type Schema[K comparable] interface {
	Hash(k K) uint64
	Mutators(k K) []tag.Mutator
}
