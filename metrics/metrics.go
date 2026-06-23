// Package metrics provides backward-compatible aliases for observe.Collector.
// New code should use github.com/sinamohsenifar/gokafka/observe directly.
package metrics

import "github.com/sinamohsenifar/gokafka/observe"

type Config = observe.MetricsConfig
type Snapshot = observe.Snapshot
type Collector = observe.Collector

func New(cfg Config) *Collector { return observe.NewCollector(cfg) }
