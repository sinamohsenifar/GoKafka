---
title: "Apache Kafka — 4.0.0 Release Announcement"
type: source
category: Research
subcategory: Roadmap
status: reference
tags: [gokafka, source, kafka, roadmap, kip, research]
url: https://kafka.apache.org/blog/2025/03/18/apache-kafka-4.0.0-release-announcement/
updated: 2026-06-30
---

# Apache Kafka — 4.0.0 Release Announcement

Authoritative ASF announcement. Release date **2025-03-18**.

- **KRaft-only**: 4.0 runs entirely without ZooKeeper (KIP-500 transition complete). ZK mode removed; deployment/management simplified.
- **KIP-848 GA**: next-gen consumer rebalance protocol reaches general availability; server-side enabled by default, clients opt in with `group.protocol=consumer`. Ends "stop-the-world" rebalances.
- **KIP-932 (early access)**: Queues for Kafka via **share groups** — cooperative consumption over regular topics for point-to-point/queue use cases. Not GA in 4.0.
- **KIP-890**: transaction server-side defense — reduces "zombie" transactions / hanging txns during producer failures (the Transactions-v2 / TV2 work).
- **KIP-966 (preview)**: Eligible Leader Replicas — KRaft controller tracks replicas safe to elect as leader without data loss. Preview in 4.0.
- **KIP-996**: Pre-Vote mechanism to minimize unnecessary KRaft leader elections.
- **KIP-1076**: application-level metrics collection alongside client metrics (extends KIP-714 to Streams).
- **KIP-1102**: client rebootstrap triggered by timeout or error codes.
- **KIP-1106**: duration-based offset reset option for consumers.
- **KIP-896**: removed old protocol API versions; minimum supported client protocol baseline raised (≈2.1). **KIP-724**: dropped message formats v0/v1 (v2 record batches only).
- **Java**: clients & Streams need Java 11; brokers, Connect, tools need Java 17.

## Related
- [[Research: Apache Kafka 4.x roadmap & upcoming KIPs]]
- [[compatibility/kafka-versions]]
- [[protocol/kip-coverage]]
