---
title: "Instaclustr — Kafka 4.1.0: rack-aware rebalancing returns"
type: source
category: Research
subcategory: KIP-848
status: reference
tags: [gokafka, source, kip-848, consumer, assignment, research]
url: https://www.instaclustr.com/blog/apache-kafka-4-1-0-advancing-queues-and-rack-aware-rebalancing-returns/
updated: 2026-06-30
---

# Instaclustr — Kafka 4.1.0: rack-aware rebalancing returns

Vendor blog (NetApp/Instaclustr) on the 4.1.0 release. Medium confidence on framing, but corroborates the official 4.1 docs on rack-aware status.

- Rack-aware partition rebalancing under the new protocol "was removed in 4.0 (due to performance concerns), but it's back in 4.1.0."
- Implemented via **KIP-1101 "Trigger rebalance on rack topology changes"** (referenced as **KAFKA-17747** in release notes).
- "KIP-1101 is active by default in Kafka 4.1.0 once the cluster is running the new Next-Generation Group Coordinator (KIP-848)." So rack-aware requires the KIP-848 group coordinator.
- Enablement conditions: all brokers on 4.1.0+, consumer groups using the new protocol, and rack metadata configured on brokers/partitions.
- Says the feature returns "with significant performance improvements" but gives **no specific numbers** for rack-aware.
- Note: this is the rack-aware-rebalance *trigger* piece. The official 4.1 docs still list "Rack-aware assignment strategies are not fully supported (KAFKA-17747)" — i.e. partial, not complete, in 4.1.

## Related
- [[Research: KIP-848 client-side assignors & rack-aware assignment]]
- compatibility/kafka-versions
- features/next-gen-groups
