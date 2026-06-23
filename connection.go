package gokafka

import (
	"fmt"
	"time"
)

// ConnectionConfig controls TCP dial, request deadlines, and advertised-listener remapping.
//
// Kafka brokers often advertise internal hostnames (e.g. kafka:29092 in Docker) that are
// unreachable from clients outside the cluster. HostRemap maps metadata addresses to
// reachable bootstrap-style addresses — the same concern Java clients solve with
// advertised.listeners and client-side bootstrap configuration.
type ConnectionConfig struct {
	// DialTimeout is the TCP connect timeout (default 10s).
	DialTimeout time.Duration
	// RequestTimeout is the per-request deadline when ctx has no deadline (default 30s).
	RequestTimeout time.Duration
	// HostRemap maps "advertised_host:port" from metadata to a reachable "host:port".
	// Example: map["kafka:29092"] = "localhost:9092"
	HostRemap map[string]string
	// BrokerAddressMapper overrides HostRemap when set. Receives node ID and metadata host/port.
	BrokerAddressMapper func(nodeID int32, host string, port int32) string
}

func defaultConnection() ConnectionConfig {
	return ConnectionConfig{
		DialTimeout:    10 * time.Second,
		RequestTimeout: 30 * time.Second,
	}
}

// ResolveBrokerAddress returns the dial address for a broker from metadata.
func (c ConnectionConfig) ResolveBrokerAddress(nodeID int32, host string, port int32) string {
	if c.BrokerAddressMapper != nil {
		if addr := c.BrokerAddressMapper(nodeID, host, port); addr != "" {
			return addr
		}
	}
	key := fmt.Sprintf("%s:%d", host, port)
	if c.HostRemap != nil {
		if mapped, ok := c.HostRemap[key]; ok {
			return mapped
		}
	}
	return key
}
