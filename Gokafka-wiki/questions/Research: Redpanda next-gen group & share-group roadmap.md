---
title: "Research: Redpanda next-gen group & share-group roadmap"
type: research
category: Research
subcategory: Deep dive
status: complete
tags: [gokafka, research, redpanda, kip-848, kip-932, compatibility]
updated: 2026-06-30
---

# Research: Redpanda next-gen group & share-group roadmap

**Question:** Does Redpanda support the new consumer group protocol (KIP-848, `group.protocol=consumer`) and share groups / queues (KIP-932) today? What are the timelines, and what should GoKafka's Redpanda compatibility matrix say? GoKafka currently auto-skips both on Redpanda.

## TL;DR

Both KIP-848 and KIP-932 are **not supported by Redpanda today** (current line v26.1.x). KIP-848 has an **open, unanswered feature request** with no committed timeline; KIP-932 has no public Redpanda tracking issue at all. GoKafka's auto-skip is **correct and should stay**. The compat matrix should mark both as "broker-unsupported, client-ready" — GoKafka ships the client side; the gap is entirely broker-side.

## Current state — Redpanda

- **KIP-848 (next-gen consumer groups): UNSUPPORTED.** `ConsumerGroupHeartbeat` (API 68) and `ConsumerGroupDescribe` (API 69) are **not advertised** by Redpanda; only the classic JoinGroup/SyncGroup/Heartbeat path works. (high — primary source: open feature request #29223, filed 2026-01-11) (Source: [[sources/redpanda-issue-29223-kip848]])
- **No committed timeline for KIP-848.** Issue #29223 is **open**, labelled `kind/enhance`, **no milestone, no assignee, no maintainer reply** with a date. (high) (Source: [[sources/redpanda-issue-29223-kip848]])
- **KIP-932 (share groups / queues): no support and no public roadmap entry.** Search of redpanda-data/redpanda surfaced no feature request for `ShareGroupHeartbeat`/`ShareFetch`; #29223 explicitly scopes share groups **out**. (medium — proving a negative; absence of a found issue, not proof none exists) (Source: [[sources/redpanda-issue-29223-kip848]])
- **Not in Redpanda release notes.** The current release-notes index (latest v26.1.12) mentions neither KIP-848 nor KIP-932 / share groups / queues anywhere. (high) (Source: [[sources/redpanda-docs-releases]])

> [!gap] No Redpanda timeline exists. There is **no announced ETA** for KIP-848 on Redpanda and **no public tracking issue** for KIP-932. Any date would be speculation. Re-check #29223 and Redpanda release notes periodically.

> [!caution] Search-summarizer hallucination. Generic web-search summaries claimed Redpanda "implemented KIP-848." This is **false** — it conflates the Apache KIP spec with Redpanda's status. The primary source (#29223) is an open request asserting the APIs are unsupported. Trust the issue, not the summary.

## Upstream baseline (what Redpanda would have to match)

- **KIP-848 GA in Apache Kafka 4.0**, opt-in via `group.protocol=consumer`; server-side group coordinator + incremental heartbeat. (high) (Source: [[Research: KIP-848 next-gen consumer rebalance protocol]])
- **KIP-932 maturity ladder:** Early Access (Kafka 4.0) → Preview (Kafka 4.1) → **GA in Kafka 4.2**; at GA only **Java clients** are supported; DLQ (KIP-1191) targets Kafka 4.4. (high — Confluent blog dated 2026-03-03) (Source: [[sources/confluent-queues-ga-2026]])
- Both are **broker-heavy**: KIP-932 needs a Share Coordinator + `__share_group_state`; KIP-848 needs the new broker-side group coordinator and server-side assignors. A wire-compatible broker must build that machinery, not just decode new API keys. (high)

## What it means FOR GOKAFKA

- **Already implemented (client side), both KIPs.** GoKafka ships `ConsumerGroupHeartbeat`/`ConsumerGroupDescribe` (68/69) with RE2J server-side regex and server-side assignment, and the share-group client APIs (76–79). (high) (Source: [[features/next-gen-groups]], [[features/share-groups]])
- **Auto-skip on Redpanda is correct — keep it.** GoKafka's `skipIfUnsupportedAPI` / capability checks already detect the missing APIs and skip gracefully; with the clear `"broker does not support API key N (Name)"` error, behavior is right. **No code change needed.** (high) (Source: [[compatibility/redpanda]])
- **Classification: broker gap, not a GoKafka gap.** For both KIPs the GoKafka side is done; the limitation is entirely Redpanda's. This is **not a GoKafka roadmap item** — it is a "blocked on upstream broker" note. (high)
- **Compat-matrix wording recommendation:** mark KIP-848 and KIP-932 as **"client-ready; broker-unsupported on Redpanda (v26.1.x); no announced ETA"**, citing #29223. Distinguish from APIs GoKafka itself lacks. The existing [[compatibility/redpanda]] page already lists both under "auto-skipped" — it is accurate; add the #29223 citation and the "no ETA" qualifier.

## Confidence summary

| Claim | Confidence |
|---|---|
| Redpanda does not support KIP-848 today | high |
| No committed Redpanda KIP-848 timeline | high |
| Redpanda does not support KIP-932; no public issue | medium (proving a negative) |
| Upstream Kafka: KIP-848 GA 4.0, KIP-932 GA 4.2 | high |
| GoKafka auto-skip is correct, no change needed | high |

## Open questions

- Will Redpanda assign #29223 a milestone? No maintainer signal yet — watch the issue. (Source: [[sources/redpanda-issue-29223-kip848]])
- Is there a private/internal Redpanda tracking issue for KIP-932 not surfaced by search? Unknown.
- Does Redpanda Cloud differ from Self-Managed on next-gen groups? Not confirmed — the "What's New in Redpanda Cloud" page was not fetched this round.
- When Redpanda does ship KIP-848, does its server-side assignor set match Kafka's (`uniform`/`range`)? Affects GoKafka assignment-path testing on Redpanda.

## Related
- [[compatibility/redpanda]] · [[features/next-gen-groups]] · [[features/share-groups]] · [[compatibility/broker-quirks]]
- [[Research: KIP-848 next-gen consumer rebalance protocol]] · [[Research: KIP-932 share groups (Queues for Kafka)]] · [[Audit: KIP-932 implementation gaps]]
- [[protocol/kip-coverage]] · [[competitors/parity-matrix]]
- [[sources/redpanda-issue-29223-kip848]] · [[sources/redpanda-docs-releases]] · [[sources/confluent-queues-ga-2026]]
