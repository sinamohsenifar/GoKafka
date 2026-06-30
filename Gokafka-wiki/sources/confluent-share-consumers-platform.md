---
title: "Confluent — Share Consumers for Confluent Platform"
type: source
category: Research
subcategory: KIP-932
status: reference
tags: [gokafka, source, kip-932, share-groups, config, research]
url: https://docs.confluent.io/platform/current/clients/share-consumers.html
updated: 2026-06-30
---

# Confluent — Share Consumers for Confluent Platform

Vendor docs (Confluent Platform) for the KIP-932 share consumer. Treat config-name details as medium confidence where they diverge from the Apache 4.2 reference; treat the ack-mode and delivery-count-limit facts as high confidence (corroborated).

- Group-level configs listed with defaults: `share.auto.offset.reset` (`latest`), `share.heartbeat.interval.ms` (`5000`), `share.isolation.level` (`read_uncommitted`), `share.record.lock.duration.ms` (`30000`), `share.session.timeout.ms` (`45000`), `share.delivery.count.limit` (`5`), `share.partition.max.record.locks` (`2000`), `share.renew.acknowledge.enable` (`true`).
- **Delivery-count limit default is 5** — when a record's delivery count hits the limit it moves to Archived and is never redelivered (poison-pill cutoff).
- `share.acknowledgement.mode` is a **share-consumer (client) config** with valid values `implicit` | `explicit`; default is **`implicit`**. If unset or `implicit`, commit/poll auto-acknowledges delivered records as processed.
- Explicit mode requires the app to call `acknowledge(record, AcknowledgeType)` per record (ACCEPT / RELEASE / REJECT).
- Group-level properties are settable via Confluent Cloud, Confluent CLI, the Cloud API, **or the `kafka-configs` tool** (exact CLI syntax not quoted on this page).
- The page does NOT document a per-record `deliveryCount()` accessor on the share consumer's records.
- The `group.share.*` prefix appears only on broker-side bounds (e.g. `group.share.max.delivery.count.limit`), not on the client-settable group config keys.

## Related
- [[Research: KIP-932 share-group configuration & remaining client surface]]
- [[features/share-groups]] · [[concepts/share-coordinator-state]]
