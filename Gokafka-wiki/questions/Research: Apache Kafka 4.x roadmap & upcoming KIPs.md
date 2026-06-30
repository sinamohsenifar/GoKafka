---
title: "Research: Apache Kafka 4.x roadmap & upcoming KIPs"
type: research
category: Research
subcategory: Deep dive
status: complete
tags: [gokafka, research, kafka, roadmap, kip]
updated: 2026-06-30
---

# Research: Apache Kafka 4.x roadmap & upcoming KIPs

What the Kafka 4.x line shipped, what's coming next, and how each item maps to GoKafka — already implemented, a gap, or a non-goal. GoKafka is CI-tested against Kafka **3.9.2 → 4.3.0** ([[compatibility/kafka-versions]]).

## The 4.x release line (high confidence)

| Release | Date | Headline | GoKafka |
|---|---|---|---|
| **4.0** | 2025-03-18 | KRaft-only (ZK removed); KIP-848 **GA**; KIP-932 early access; KIP-890 txn defense; KIP-966 ELR preview | ✅ supported |
| **4.1** | 2025 (mid) | Queues **preview**; "stream groups" (Streams on KIP-848); ELR default-on for new clusters | ✅ supported |
| **4.2** | 2026-02-17 | KIP-932 share groups **GA** (RENEW, lag metrics, adaptive batching) | ✅ supported |
| **4.3** | 2026-05-22 | Ops/broker-heavy: assignment epochs (KIP-1251), share-group configs, classic-protocol deprecation notice (KIP-1274) | ✅ supported |

Sources: [[sources/kafka-4-0-release-announcement]], [[sources/kafka-4-2-share-groups-ga]] (high — multiple authoritative ASF/Confluent sources agree).

## Item-by-item map to GoKafka

**KRaft-only / ZooKeeper removal (KIP-500)** — high. 4.0 is KRaft-only; only v2 record batches remain (KIP-896/724). GoKafka is **already KRaft-only with ZK paths removed** and v2-batch-only (Source: [[sources/kafka-4-0-release-announcement]]). ✅ done / non-issue (client is broker-agnostic to KRaft).

**KIP-848 next-gen consumer groups (GA in 4.0)** — high. Server-driven incremental rebalance; opt-in `group.protocol=consumer`. GoKafka **ships the client side** (APIs 68/69, RE2J regex, server-side assignment) — see [[features/next-gen-groups]], [[Research: KIP-848 next-gen consumer rebalance protocol]]. ✅ implemented. Forward gap: client-side assignors still not implemented (matches upstream maturity).

**KIP-932 share groups / Queues (GA in 4.2)** — high. Cooperative consumption over normal topics with per-record acquisition locks. GoKafka **ships the client side** (APIs 76–79, RENEW via ShareAck v2) — [[features/share-groups]], [[Research: KIP-932 share groups (Queues for Kafka)]], [[Audit: KIP-932 implementation gaps]]. ✅ implemented; track 4.2 GA refinements (lag metrics surface, strict/soft fetch enforcement).

**KIP-890 Transactions v2 (TV2)** — high. Server-side defense against zombie/hanging transactions. GoKafka **implements TV2** with TV1 fallback on older brokers — [[features/exactly-once-tv2]], [[packages/transactions]]. ✅ implemented.

**KIP-966 Eligible Leader Replicas** — high. KRaft controller tracks non-ISR replicas safe to elect without data loss; preview 4.0, default-on for new clusters in 4.1 (Source: [[sources/kafka-4-0-release-announcement]]). **Broker/controller-side feature — no client wire-protocol surface.** → **Non-goal for GoKafka** (transparent to a client). Only indirect effect: fewer unclean-leader data-loss windows.

**KIP-714 client metrics push (GetTelemetrySubscriptions / PushTelemetry, APIs 71/72)** — high. Broker-pluggable client telemetry; extended to Streams via KIP-1076. GoKafka **does not implement** these APIs — declared a **non-goal** under [[decisions/adr-stdlib-only]] (Source: [[protocol/api-coverage]], [[protocol/kip-coverage]]). ❌ non-goal (could revisit if users want broker-side observability).

**KIP-1102 client rebootstrap** — medium. Clients re-bootstrap metadata on timeout or specific error codes (resilience when all known brokers are unreachable). Client-side behavior; **likely a real GoKafka enhancement opportunity**, not currently confirmed in coverage notes.
> [!gap] GoKafka's current rebootstrap behavior vs KIP-1102 is unverified — needs a code check against the client's broker-discovery path.

**KIP-1150 Diskless Topics** — high (status) / medium (timeline). Write active segments to **object storage** instead of replicated broker disk to kill cross-AZ transfer cost; per-topic latency/cost tradeoff. **Accepted as a meta-KIP** (~2026-03); implementation delegated to **KIP-1163 (Diskless Core)** + **KIP-1164 (Diskless Coordinator)**, still under discussion. Crucially: "**not meant to change the Kafka Storage API**" → broker-internal, **no client wire-protocol change** (Source: [[sources/apache-kip-1150-diskless-topics]]). → **Non-goal / transparent for GoKafka**; clients produce/consume normally. Watch in case 1163/1164 add new produce/fetch semantics.

**DLQ support (KIP-1191)** — medium. Auto-copy undeliverable records to dedicated DLQ topics; targeting **Kafka 4.4**. Mostly a share-group/consumer-app concern. → **Future watch** for [[features/share-groups]]; possible client-side ergonomics later (Source: [[sources/kafka-4-2-share-groups-ga]]).

## What it means for prioritizing GoKafka

1. **Everything client-facing in 4.0–4.2 is already shipped** (KIP-848, 932, 890). The 4.x protocol surface is well-covered — see [[competitors/parity-matrix]].
2. **The big forward KIPs (1150 diskless, 966 ELR) are broker-internal** — GoKafka gets the benefits for free with zero client work. Do **not** schedule client work for them.
3. **Real client-side candidates to evaluate**: KIP-1102 rebootstrap (resilience), KIP-1191 DLQ ergonomics (share-group UX), and tracking 4.3 share-group refinements. KIP-714 stays a deliberate non-goal.
4. **Classic rebalance protocol is now on a deprecation notice (KIP-1274, 4.3)** — confirms KIP-848 is the strategic path; GoKafka's investment there is well-placed.

## Open questions
- KIP-1102: does GoKafka already rebootstrap on all-brokers-down / specific error codes, or is this a genuine gap? (code check needed)
- KIP-1163/1164 diskless implementation: will the consume path stay wire-compatible, or introduce new fetch semantics a client must understand? (unresolved upstream)
- KIP-1191 DLQ: is any of it client-API-visible, or purely broker/consumer-runtime? Confirm before scheduling.
- Exact 4.1 release date (sources give "mid-2025" / 4.1 docs but no precise day captured here).
- Client-side assignor + rack-aware assignment timelines for KIP-848 (carried over from prior research; KAFKA-18327 / KAFKA-17747).

## Related
- [[compatibility/kafka-versions]]
- [[protocol/kip-coverage]]
- [[protocol/api-coverage]]
- [[features/share-groups]]
- [[features/next-gen-groups]]
- [[competitors/parity-matrix]]
- [[sources/kafka-4-0-release-announcement]]
- [[sources/apache-kip-1150-diskless-topics]]
