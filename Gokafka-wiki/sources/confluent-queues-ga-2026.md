---
title: "Confluent Blog — Kafka Queue Semantics now GA with Share Consumer API"
type: source
category: Research
subcategory: Redpanda
status: reference
tags: [gokafka, source, redpanda, kip-848, kip-932, compatibility, research]
url: https://www.confluent.io/blog/kafka-queue-semantics-share-consumer-ga/
updated: 2026-06-30
---

# Confluent Blog — Queues for Kafka GA

- **Post date: 2026-03-03.** Used here to anchor upstream KIP-932 maturity (the broker-side baseline Redpanda would have to match).
- KIP-932 "Queues for Kafka" / share groups reached **General Availability**, coinciding with the **Apache Kafka 4.2** release. GA first on Confluent Cloud (Enterprise + Dedicated) and Confluent Platform 8.2; "coming soon" to broader Confluent Platform. (high confidence)
- **Only Apache Kafka 4.2+ Java clients are currently supported** for share consumers at GA; non-Java client support is targeted later in 2026.
- Maturity ladder upstream: Early Access (Kafka 4.0) → Preview (Kafka 4.1) → GA (Kafka 4.2). Dead-letter-queue support (KIP-1191) targeted for Apache Kafka 4.4.
- **Redpanda is not mentioned** in the post. It is Confluent/Apache-centric; no third-party broker roadmap.
- For GoKafka: confirms share groups are a recent, server-heavy feature (Share Coordinator, `__share_group_state`); a Kafka-compatible broker like Redpanda must implement the broker side before GoKafka's client-side share APIs can be exercised there.

## Related
- [[Research: Redpanda next-gen group & share-group roadmap]]
- [[features/share-groups]] · [[Research: KIP-932 share groups (Queues for Kafka)]]
