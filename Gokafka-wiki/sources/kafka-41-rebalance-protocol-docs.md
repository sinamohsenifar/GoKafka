---
title: "Apache Kafka — Consumer Rebalance Protocol (4.1 docs)"
type: source
category: Research
subcategory: KIP-848
status: reference
tags: [gokafka, source, kip-848, consumer, assignment, research]
url: https://kafka.apache.org/41/operations/consumer-rebalance-protocol/
updated: 2026-06-30
---

# Apache Kafka — Consumer Rebalance Protocol (4.1 docs)

Official Kafka 4.1 operations docs page. Authoritative for the current (4.1) state of the protocol.

- "Starting from Apache Kafka 4.0, the Next Generation of the Consumer Rebalance Protocol (KIP-848) is Generally Available (GA)."
- New protocol is enabled by default server-side; consumers opt in via `group.protocol=consumer`.
- Default server-side assignors: `uniform` and `range`. The first in the list is used unless the consumer selects another.
- Custom server-side strategies are pluggable via the `ConsumerGroupPartitionAssignor` interface (runs on the broker).
- `group.remote.assignor` — optional config that "can be used to overwrite the default assignment strategy configured on the server side" (selects which broker-side assignor this consumer wants).
- **Limitation (verbatim):** "Client-side assignors are not supported. (see KAFKA-18327)"
- **Limitation (verbatim):** "Rack-aware assignment strategies are not fully supported. (see KAFKA-17747)"
- Configs that become unusable under the new protocol: `heartbeat.interval.ms`, `session.timeout.ms`, `partition.assignment.strategy` (these are now server-managed / replaced).
- Note: `partition.assignment.strategy` (the classic client-side assignor selector) has no equivalent under the new protocol — selection is server-side via `group.remote.assignor`.

## Related
- [[Research: KIP-848 client-side assignors & rack-aware assignment]]
- features/next-gen-groups
- concepts/server-side-assignor
- concepts/consumergroupheartbeat
