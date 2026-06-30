---
title: "Apache Kafka — Group Configs reference (4.2)"
type: source
category: Research
subcategory: KIP-932
status: reference
tags: [gokafka, source, kip-932, share-groups, config, research]
url: https://kafka.apache.org/42/configuration/group-configs/
updated: 2026-06-30
---

# Apache Kafka — Group Configs reference (4.2)

Canonical, version-pinned (Kafka 4.2) list of the **group-level** configs that apply to share groups. These are the GROUP config-resource keys an admin/client alters; not client constructor configs.

- The canonical prefix is **`share.`**, NOT `group.share.` (the `group.share.*` form is the broker-side default/bound for the same setting).
- `share.isolation.level` — string, default **`read_uncommitted`**, valid `[read_committed, read_uncommitted]`. Controls whether the share group delivers uncommitted transactional records.
- `share.auto.offset.reset` — string, default **`latest`**, valid `[latest, earliest, by_duration:PnDTnHnMn.nS]`. The `by_duration:` form is the KIP-1106 duration-based reset. Sets the initial SPSO when a subscription is first added.
- `share.record.lock.duration.ms` — int, default **`30000`** (30 s), min `1000`. Acquisition-lock timeout; a crashed consumer's locked records are auto-released after this.
- `share.session.timeout.ms` — int, default **`45000`** (45 s), min `1`. Member removed if it misses heartbeats this long.
- `share.heartbeat.interval.ms` — int, default **`5000`** (5 s), min `1`. Expected ShareGroupHeartbeat cadence.
- This reference table does NOT list `share.delivery.count.limit` or `share.acknowledgement.mode` — those are documented separately (delivery-count limit has the broker bound `group.share.delivery.count.limit`; ack mode is a client-side share-consumer config).
- All of these are settable per share group via the GROUP config resource (see the kafka-configs `--entity-type groups` source note).

## Related
- [[Research: KIP-932 share-group configuration & remaining client surface]]
- [[features/share-groups]] · [[concepts/share-group-acquisition-lock]]
