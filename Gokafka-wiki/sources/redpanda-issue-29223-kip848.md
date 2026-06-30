---
title: "Redpanda GitHub — Issue #29223: Feature request for KIP-848 support"
type: source
category: Research
subcategory: Redpanda
status: reference
tags: [gokafka, source, redpanda, kip-848, kip-932, compatibility, research]
url: https://github.com/redpanda-data/redpanda/issues/29223
updated: 2026-06-30
---

# Redpanda GitHub — Issue #29223

- **Title:** "Feature request: Support KIP-848 (Next-Gen Consumer Group Protocol: ConsumerGroupHeartbeat / ConsumerGroupDescribe)".
- **Opened by** user `vxtls` on **2026-01-11**. Status: **open** (as of this research). Label `kind/enhance` (feature request). No milestone, no assignee. (high confidence — read directly from the issue)
- Core claim by reporter: API version negotiation against Redpanda shows **classic group APIs supported** (JoinGroup / SyncGroup / Heartbeat) but **KIP-848 APIs UNSUPPORTED**: `ConsumerGroupHeartbeat` (API key 68) and `ConsumerGroupDescribe` (API key 69) are not advertised.
- Consequence cited: clients are forced to stay on the legacy (classic) consumer-group protocol; cannot adopt Kafka 4.x next-gen rebalance improvements via `group.protocol=consumer`.
- Request includes a desire for **rolling migration** from classic to consumer protocol with no full-cluster downtime, plus documentation of any limitations.
- **KIP-932 / share groups explicitly marked out of scope** of this issue by the reporter.
- **No maintainer response** with a timeline or roadmap commitment is visible in the issue.
- No specific Redpanda version number appears in the issue body (GoKafka observed the gap on v26.1).

> Note: a generic web-search summarizer claimed Redpanda "implemented" KIP-848. That is contradicted by this primary source — the issue is an open, unanswered feature request asserting the APIs are unsupported. Trust the issue.

## Related
- [[Research: Redpanda next-gen group & share-group roadmap]]
- [[compatibility/redpanda]] · [[features/next-gen-groups]]
