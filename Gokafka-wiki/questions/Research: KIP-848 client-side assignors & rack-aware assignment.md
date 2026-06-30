---
title: "Research: KIP-848 client-side assignors & rack-aware assignment"
type: research
category: Research
subcategory: Deep dive
status: complete
tags: [gokafka, research, kip-848, consumer, assignment]
updated: 2026-06-30
---

# Research: KIP-848 client-side assignors & rack-aware assignment

Follow-up deep dive on two open questions from [[Research: KIP-848 next-gen consumer rebalance protocol]]: where do **client-side assignors** and **rack-aware assignment** stand, and does GoKafka need them?

## TL;DR
KIP-848 ships **server-side** assignment (`uniform`/`range`) as GA since Kafka 4.0. **Client-side assignors are specified but still not implemented** (KAFKA-18327, Open, no fix version). **Rack-aware** assignment is "not fully supported" in the assignor itself, but the rack-aware *rebalance trigger* (KIP-1101 / KAFKA-17747) returned in **Kafka 4.1**. For GoKafka, both remain **non-goals / no client work required** — the thin-client design is already correct. (high)

## Server-side vs client-side: the two assignor modes
KIP-848 defines two ways to compute assignments (Source: [[sources/apache-kip-848-client-assignor-apis]]):
- **Server-side (the default, GA):** the broker's group coordinator runs a pluggable assignor. Defaults are `uniform` and `range`; the first in the list wins unless the client requests another. (high — Source: [[sources/kafka-41-rebalance-protocol-docs]])
- **Client-side:** the coordinator nominates one member via the heartbeat response; that member calls **`ConsumerGroupPrepareAssignment`** (fetch coordinator-supplied group state), runs its assignor, then **`ConsumerGroupInstallAssignment`** (submit result for validation/persistence), all within the rebalance timeout. A new optional `PartitionAssignor` interface (separate from the classic one) is defined for this. (high — Source: [[sources/apache-kip-848-client-assignor-apis]])

The whole point of client-side assignors is to let **power users like Kafka Streams** keep purpose-built assignors. It is not aimed at ordinary consumer apps. (medium — Source: [[sources/apache-kip-848-client-assignor-apis]])

### `group.remote.assignor` vs client-side
`group.remote.assignor` is **not** a client-side assignor. It is an optional consumer config that "can be used to overwrite the default assignment strategy configured on the server side" — i.e. it picks *which broker-side assignor* the coordinator should use for this consumer. The actual computation still runs on the broker. (high — Source: [[sources/kafka-41-rebalance-protocol-docs]]) The classic `partition.assignment.strategy` config has no equivalent under the new protocol and is unusable when `group.protocol=consumer`. (high — same source)

## Status across versions
| Version | Server-side assignors | Client-side assignors | Rack-aware |
|---|---|---|---|
| 4.0 | GA | Not implemented | Removed (perf concerns) |
| 4.1 | GA | Not implemented | Trigger back via KIP-1101/KAFKA-17747; assignor "not fully supported" |
| 4.2 | GA | Still not implemented | Partial |

- KIP-848 server-side path is **GA since Kafka 4.0**, on by default server-side, opt-in client-side via `group.protocol=consumer`. (high — Source: [[sources/kafka-41-rebalance-protocol-docs]])
- The 4.1 docs still list verbatim limitations: "Client-side assignors are not supported. (see KAFKA-18327)" and "Rack-aware assignment strategies are not fully supported. (see KAFKA-17747)". (high — Source: [[sources/kafka-41-rebalance-protocol-docs]])
- **KAFKA-18327 "Client Side Assignors (KIP-848)"** is **Open, no fix version**, created 2024-12-19 with open sub-tasks — specified but not delivered. (medium — single JIRA source) Earlier sub-tasks for the assignor *RPCs* (KAFKA-15279) and *client support* (KAFKA-15282) appear Resolved, so the wire plumbing landed but the user-facing assignor did not. (low — mailing-list titles only)
- **4.2** still has no client-side assignor; topic-id offset fetch/commit landed there as a separate KIP-848 roadmap item. (medium — search synthesis)

## Rack-aware: removed in 4.0, partially back in 4.1
Rack-aware partition rebalancing under the new protocol was **removed in 4.0 due to performance concerns** and **returned in 4.1.0** through **KIP-1101 "Trigger rebalance on rack topology changes"** (referenced as **KAFKA-17747** in release notes). KIP-1101 is **active by default in 4.1.0 once the cluster runs the KIP-848 next-gen group coordinator**; it requires all brokers on 4.1.0+, consumer groups on the new protocol, and rack metadata configured. (high — Sources: [[sources/instaclustr-kafka-41-rack-aware]], [[sources/kafka-41-rebalance-protocol-docs]])

> [!gap]
> KIP-1101 is the *trigger* (rebalance when rack topology changes). It is distinct from a fully *rack-aware assignor*. The 4.1 docs still say rack-aware assignment is "not fully supported," so closure beyond 4.1 is unclear. No authoritative quantified rack-aware perf numbers were found — the vendor blog only says "significant improvements."

## Quantified rebalance-time gains (the open question from prior research)
One concrete, method-disclosed benchmark exists: expanding a topic from **100 → 1,000 partitions** with a **10-consumer** group, classic protocol took **103 s** vs new protocol **5 s — ~20x faster** (polling `kafka-consumer-group.sh` every 1 s; published 2025-07-08). (medium — single vendor source, [[sources/instaclustr-kip848-20x-benchmark]]) This is a synthetic partition-expansion case, not a general guarantee; the win comes from the fully incremental design with no global sync barrier. Separate benchmarks showed "better latencies" on add/remove consumer but with no percentages. (medium — same source)

## What this means for GoKafka
- GoKafka implements the **client** side of KIP-848 (`ConsumerGroupHeartbeat` API 68 + `ConsumerGroupDescribe` 69) and uses **server-side assignment exclusively** — confirmed by existing code-verified notes ([[concepts/server-side-assignor]], [[features/next-gen-groups]]). (high — vault, code-verified)
- **Client-side assignors are a non-goal for GoKafka.** They exist for Kafka Streams-class workloads; an ordinary Go consumer never needs to compute assignments locally. Even the upstream Java client hasn't shipped it. If GoKafka ever wanted a *named* server-side strategy, that's just sending `group.remote.assignor` — no assignor code, no `PrepareAssignment`/`InstallAssignment` RPCs. (high)
- **Rack-aware is broker-side.** With the new protocol the assignor runs on the broker, so rack-aware behavior (incl. KIP-1101 triggering) is a server/cluster concern, not a Go client feature. GoKafka could optionally surface a `rack.id`/`client.rack` in the heartbeat so the broker assignor can use it — worth a small follow-up check of whether the consumer already sends a rack id. (medium)
- Net: the prior research's conclusion holds — GoKafka's thin-client posture is correct and complete for the GA feature set. Both topics here are **future/non-goal**, not gaps. (high)

> [!note]
> Forward signal: **KIP-1274** proposes deprecating and eventually removing the classic rebalance protocol from KafkaConsumer. Direction of travel is fully toward the server-driven model GoKafka already targets. (low — search-surfaced KIP title only)

## Open questions
- Does GoKafka's `ConsumerGroupHeartbeat` request already populate a rack id (`client.rack`) so a future broker rack-aware assignor can place it well? (needs code check)
- When (if ever) does KAFKA-18327 ship a usable client-side assignor, and will Kafka Streams instead use its own protocol path? (mailing-list chatter hints at "on hold," unconfirmed)
- Is rack-aware *assignment* (beyond the KIP-1101 trigger) fully closed in any 4.x release, and is there a quantified perf number?
- Redpanda still lacks `ConsumerGroupHeartbeat` (carried from prior research) — does it offer any server-side assignor knobs once it does?

## Related
- [[Research: KIP-848 next-gen consumer rebalance protocol]]
- concepts/server-side-assignor
- concepts/consumergroupheartbeat
- concepts/epoch-reconciliation
- features/next-gen-groups
- packages/consumer
- compatibility/kafka-versions
- compatibility/redpanda
