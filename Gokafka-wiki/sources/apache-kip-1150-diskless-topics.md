---
title: "Apache cwiki — KIP-1150: Diskless Topics"
type: source
category: Research
subcategory: Roadmap
status: reference
tags: [gokafka, source, kafka, roadmap, kip, research]
url: https://cwiki.apache.org/confluence/display/KAFKA/KIP-1150:+Diskless+Topics
updated: 2026-06-30
---

# Apache cwiki — KIP-1150: Diskless Topics

- **Problem**: replication of active segments across availability zones is the dominant Kafka infra cost on cloud hyperscalers (cross-AZ data-transfer fees). Tiered Storage does not remove the need to durably replicate active segments.
- **Idea**: diskless topics write through to **object storage** (e.g. S3) instead of durable broker disks; local disks become cache only. "Diskless is to 'No Disks' as Serverless is to 'No Servers'" — brokers still use disk for metadata and caching.
- **Status**: **Accepted** (passed discuss + vote on the dev mailing list; reported accepted ~2026-03-02). It is a "meta KIP" — sets directional consensus, not the implementation.
- **Tradeoff**: per-topic choice between cost optimization and latency; eliminates inter-zone transfer fees at the cost of potentially higher produce latency than classic topics.
- **Implementation delegated** to **KIP-1163 (Diskless Core)** and **KIP-1164 (Diskless Coordinator)**, voted separately and (as of mid-2026) still under discussion.
- **Wire protocol**: "Diskless topics are not meant to change the Kafka Storage API" — broker-internal feature; **no client wire-protocol change** required. Clients produce/consume normally.
- No specific target Kafka version stated in the KIP.

## Related
- [[Research: Apache Kafka 4.x roadmap & upcoming KIPs]]
- [[compatibility/kafka-versions]]
- [[competitors/parity-matrix]]
