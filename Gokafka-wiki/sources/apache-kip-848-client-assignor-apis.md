---
title: "Apache cwiki — KIP-848 client-side assignor APIs"
type: source
category: Research
subcategory: KIP-848
status: reference
tags: [gokafka, source, kip-848, consumer, assignment, research]
url: https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
updated: 2026-06-30
---

# Apache cwiki — KIP-848 client-side assignor APIs

The canonical KIP. This note focuses on the client-side assignor portion of the design (the protocol is specified; the client implementation is still pending — see KAFKA-18327).

- KIP-848 defines TWO assignor modes: server-side (runs in the group coordinator on the broker; `uniform`/`range` ship by default) and client-side (a chosen member computes the assignment).
- Client-side flow uses two RPCs:
  - **ConsumerGroupPrepareAssignment** — the chosen member calls this to fetch current group metadata + the input state needed to compute a new assignment. Input is entirely driven by the group coordinator.
  - **ConsumerGroupInstallAssignment** — the member submits the computed assignment back to the coordinator for validation and persistence.
- Selection workflow: coordinator picks a member → signals it via the heartbeat response → member calls PrepareAssignment → runs its assignor → calls InstallAssignment. Must finish within the rebalance timeout or the coordinator picks another member.
- A new optional `PartitionAssignor` interface exists for the new protocol, deliberately separate from the classic-protocol assignor interface (different semantics, independent evolution).
- Purpose of client-side assignors: let "power users such as Kafka Streams" keep purpose-built assignors.
- Reconciliation (independent of assignor mode): members converge toward target via revoke → ack-revoke → incrementally take new partitions as others release them — no global sync barrier.
- As of the KIP/doc state: client-side assignor implementation was pending.

## Related
- [[Research: KIP-848 client-side assignors & rack-aware assignment]]
- concepts/server-side-assignor
- concepts/epoch-reconciliation
