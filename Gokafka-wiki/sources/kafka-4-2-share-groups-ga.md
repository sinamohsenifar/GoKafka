---
title: "Confluent / Apache — Kafka 4.1 & 4.2: Share Groups path to GA"
type: source
category: Research
subcategory: Roadmap
status: reference
tags: [gokafka, source, kafka, roadmap, kip, research]
url: https://kafka.apache.org/blog/2026/02/17/apache-kafka-4.2.0-release-announcement/
updated: 2026-06-30
---

# Confluent / Apache — Kafka 4.1 & 4.2: Share Groups path to GA

Synthesized from the Apache 4.2.0 announcement plus Confluent/Factor-House release writeups.

- **KIP-932 maturity ladder**: early access in **4.0** → preview in **4.1** → **production-ready / GA in 4.2**. The new Share Consumer API is GA with queue semantics.
- **4.2 share-group additions**: **RENEW** acknowledgement type for extended processing time, adaptive batching for share coordinators, soft/strict enforcement of fetched-record quantity, and comprehensive **lag metrics** for share groups.
- **4.1** also shipped "stream groups" (Streams rebalance on the KIP-848 protocol) and the queues preview.
- **KIP-714 client metrics**: extended to Kafka Streams via **KIP-1076** — broker-side plugin can collect both internal-client metrics and the Streams runtime's own metrics.
- **4.3** (2026-05-22) is broker/ops-heavy: KIP-1251 assignment epochs for consumer groups, KIP-1240/KIP-1263 more share-group config + coordinator offload, KIP-1274 deprecation notice for the **classic** (JoinGroup/SyncGroup) rebalance protocol.
- **Roadmap signals**: non-Java client support for queues being developed; **DLQ support (KIP-1191)** targeting **Kafka 4.4**; transactions continue hardening on the KIP-890 (TV2) baseline.

## Related
- [[Research: Apache Kafka 4.x roadmap & upcoming KIPs]]
- [[features/share-groups]]
- [[features/next-gen-groups]]
