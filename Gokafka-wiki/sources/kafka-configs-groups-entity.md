---
title: "Apache Kafka — kafka-configs --entity-type groups (GROUP config resource)"
type: source
category: Research
subcategory: KIP-932
status: reference
tags: [gokafka, source, kip-932, share-groups, config, research]
url: https://github.com/apache/kafka/pull/16887
updated: 2026-06-30
---

# Apache Kafka — kafka-configs --entity-type groups (GROUP config resource)

KAFKA-17327 PR: adds group support to `kafka-configs.sh`. This is the canonical mechanism by which a CLIENT alters share-group (and consumer-group) configs in Kafka 4.x.

- `kafka-configs.sh` gains `--entity-type groups` (and a `--group <group-id>` shorthand) for `--describe` and `--alter`.
- This maps to the **`ConfigResource` of type GROUP** and is carried over the **IncrementalAlterConfigs** RPC (GROUP resource-type code = 32). Same RPC as topic/broker dynamic configs, just a new resource type.
- Set/delete/append operations use the standard `AlterConfigOp` op types (SET, DELETE, APPEND, SUBTRACT).
- The GROUP resource type was introduced alongside the new group protocol (KIP-848) so the same surface serves consumer groups and share groups; share-specific keys (`share.isolation.level`, `share.auto.offset.reset`, etc.) are set this way.
- Known limitation: `--describe` without a named group lists only *existing* (active) groups; it cannot show configs for a group config-resource that has no live members yet.
- Background: extending IncrementalAlterConfigs to GROUP configs was done under KAFKA-14511 (PR #15067); this PR wires the CLI through to it.

## Related
- [[Research: KIP-932 share-group configuration & remaining client surface]]
- [[packages/admin]] · [[features/share-groups]]
