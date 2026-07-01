package opencensus

import (
	"time"
)

// Config holds the settings shared by every aggregator variant: the shard count,
// the flush interval and the key projection Schema.
type Config[K comparable] struct {
	Shards   int           // rounded up to a power of 2. Default 16.
	Interval time.Duration // flush cadence. Default 20s.
	Schema   Schema[K]     // key projection strategy
}

func (c *Config[K]) applyDefaults() {
	if c.Shards <= 0 {
		c.Shards = 16
	}
	if c.Interval <= 0 {
		c.Interval = 20 * time.Second
	}
}
