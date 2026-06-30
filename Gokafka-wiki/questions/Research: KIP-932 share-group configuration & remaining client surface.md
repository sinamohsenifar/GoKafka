---
title: "Research: KIP-932 share-group configuration & remaining client surface"
type: research
category: Research
subcategory: Deep dive
status: complete
tags: [gokafka, research, kip-932, share-groups, config]
updated: 2026-06-30
---

# Research: KIP-932 share-group configuration & remaining client surface

Deep dive on the *settable configuration* surface of KIP-932 share groups (GA Kafka 4.x) and what a client needs to drive it — the remaining audit gaps for GoKafka. Companion to [[Research: KIP-932 share groups (Queues for Kafka)]] (the protocol/semantics dive) and [[Audit: KIP-932 implementation gaps]].

## The settable config surface

Share-group behaviour is tuned almost entirely through **group-level configs**, not client configs. These live on a **GROUP config resource** and are read/applied server-side by the share coordinator + group coordinator. Canonical names and defaults (Kafka 4.2, high confidence — Source: [[sources/apache-kafka-42-group-configs]], corroborated by [[sources/confluent-share-consumers-platform]]):

| Config | Default | Values / notes |
|---|---|---|
| `share.isolation.level` | `read_uncommitted` | `read_committed` \| `read_uncommitted`. read_committed delivers only committed txn records. |
| `share.auto.offset.reset` | `latest` | `latest` \| `earliest` \| `by_duration:PnDTnHnMn.nS` (KIP-1106). Sets initial SPSO on first subscription. |
| `share.record.lock.duration.ms` | `30000` (30 s) | min 1000. Acquisition-lock TTL; crashed-consumer records auto-release after this. |
| `share.session.timeout.ms` | `45000` (45 s) | member evicted after missing heartbeats this long. |
| `share.heartbeat.interval.ms` | `5000` (5 s) | expected ShareGroupHeartbeat cadence. |
| `share.delivery.count.limit` | `5` | delivery count cap; at the limit a record → Archived, never redelivered (poison-pill cutoff). Broker bound: `group.share.delivery.count.limit`. |

> [!note] Prefix gotcha
> The client-settable keys use the bare **`share.`** prefix. The `group.share.*` form is the **broker-side default/bound** for the same knob (e.g. `group.share.delivery.count.limit`, `group.share.max.record.locks`). Don't send `group.share.…` as a per-group config — send `share.…`. (high — Source: [[sources/apache-kafka-42-group-configs]])

**`share.acknowledgement.mode`** is the one config that is a **client-side share-consumer config** (not a group config): `implicit` (default) | `explicit` (high — Source: [[sources/confluent-share-consumers-platform]]). Implicit = poll/commit auto-accepts the previously delivered batch; explicit = the app must `acknowledge(record, ACCEPT|RELEASE|REJECT)`.

## How a client alters GROUP-resource configs

The GA mechanism: **IncrementalAlterConfigs on a GROUP `ConfigResource`** (resource-type code **32**), the same RPC used for topic/broker dynamic configs (high — Source: [[sources/kafka-configs-groups-entity]]). The CLI surface is `kafka-configs.sh --entity-type groups --group <id> --alter …`. The GROUP resource type was added with the new group protocol (KIP-848) and serves both consumer and share groups. Ops are the standard SET/DELETE/APPEND/SUBTRACT. (Background: KAFKA-14511 extended IncrementalAlterConfigs to group configs; KAFKA-17327 wired the CLI.)

## delivery_count exposure on consumed records

The ShareFetch response carries an **`acquired_records`** structure per partition: `[first_offset, last_offset, delivery_count(int16)]` — the broker tells the consumer how many times each acquired range has been delivered. (high — Source: [[sources/apache-kafka-42-group-configs]] context + GoKafka wire decoder.) The Java `KafkaShareConsumer` exposes this; none of the fetched docs quoted the exact `ConsumerRecord.deliveryCount()` accessor signature.

> [!gap] deliveryCount accessor signature unverified
> Multiple sources confirm delivery_count is sent on the wire (int16 per acquired range) and is used for poison-pill detection, but no fetched page quoted the exact Java accessor (believed `Optional<Short> deliveryCount()` on `ConsumerRecord`). Confirm against Kafka 4.2 javadoc before mirroring the name in GoKafka.

## What it means FOR GOKAFKA

GoKafka ships the share-consumer client (APIs 76–79) and **already touches this surface** — but only partially. State, code-verified:

- **Already implemented (partial):** `applyShareStartOffset` issues IncrementalAlterConfigs on `ConfigResourceGroup` (=32) to set `share.auto.offset.reset=earliest` when `WithConsumeFromBeginning` is set (`share_consumer.go:248`). So GoKafka *does* drive a GROUP config from the client — but hardcoded to one key/value as a side effect, not a general API.
- **Already implemented:** `WithShareAcknowledgementMode(explicit|implicit)` option (`options.go`). **Divergence:** GoKafka's default is **explicit** (`ShareAckExplicit`), whereas Java/Confluent default is **implicit**. (medium — verify whether this is intentional; it changes default at-least-once ergonomics.)
- **Parsed-but-discarded:** the ShareFetch decoder reads the per-range `delivery_count` int16 and throws it away (`internal/protocol/share_fetch.go:197`). The value never reaches `Record`. → **Gap:** surface delivery count on consumed records so callers can do their own poison-pill handling.

> [!gap] Real GoKafka gaps to close
> 1. **No general "alter share-group config" admin API.** Only `share.auto.offset.reset=earliest` is reachable, and only via the consume-from-beginning flag. There is no way for a GoKafka caller to set `share.isolation.level`, `share.record.lock.duration.ms`, `share.session.timeout.ms`, `share.heartbeat.interval.ms`, or `share.delivery.count.limit`. The plumbing exists (`EncodeIncrementalAlterConfigsRequest` + `ConfigResourceGroup`); needs an admin-package wrapper (e.g. `Admin.AlterGroupConfigs(group, map[string]string)`) → [[packages/admin]].
> 2. **delivery_count not exposed** on records (decoded then dropped). → [[packages/consumer]].
> 3. **Default ack mode mismatch** (explicit vs Java implicit) — decide if this is a deliberate GoKafka choice and document it.

None of these are non-goals; all are stdlib-fixable client work that extends an already-shipped feature.

## Open questions

- Exact Java accessor name/type for per-record delivery count (`Optional<Short> deliveryCount()`?) — confirm vs Kafka 4.2 javadoc.
- Is `share.delivery.count.limit` settable as a per-group GROUP config in core Apache (it's absent from the 4.2 group-configs table but present in Confluent's list)? Or is only the broker bound `group.share.delivery.count.limit` settable? (medium uncertainty)
- Does Redpanda accept IncrementalAlterConfigs on a GROUP resource at all? (share groups unsupported there → likely moot; verify) → [[compatibility/redpanda]].
- Intended GoKafka default ack mode — keep explicit or align to Java's implicit?

## Related
- [[features/share-groups]] · [[concepts/share-group-acquisition-lock]] · [[concepts/share-coordinator-state]]
- [[packages/consumer]] · [[packages/admin]]
- [[Audit: KIP-932 implementation gaps]] · [[Research: KIP-932 share groups (Queues for Kafka)]]
- [[sources/apache-kafka-42-group-configs]] · [[sources/confluent-share-consumers-platform]] · [[sources/kafka-configs-groups-entity]] · [[sources/apache-kip-932]]
