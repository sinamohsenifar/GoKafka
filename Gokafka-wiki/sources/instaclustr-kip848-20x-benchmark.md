---
title: "Instaclustr — KIP-848 rebalance up to 20x faster (benchmark)"
type: source
category: Research
subcategory: KIP-848
status: reference
tags: [gokafka, source, kip-848, consumer, assignment, research]
url: https://www.instaclustr.com/blog/rebalance-your-apache-kafka-partitions-with-the-next-generation-consumer-rebalance-protocol/
updated: 2026-06-30
---

# Instaclustr — KIP-848 rebalance up to 20x faster (benchmark)

Vendor blog with a concrete reproducible micro-benchmark. Single-source quantified number (medium confidence); test method is disclosed, so it's usable with the caveat below.

- Headline: classic protocol rebalance took **103 seconds**; new protocol took **5 seconds** — "that's 20x faster."
- Test conditions: topic grown from **100 → 1,000 partitions** (10x), consumer group of **10 consumers**; completion detected by polling `kafka-consumer-group.sh` every second. Published **2025-07-08**.
- Caveat: this is one synthetic partition-expansion scenario, not a general "20x" guarantee; the gain comes from the fully incremental design (no global sync barrier).
- Confirms the new protocol uses **server-side** assignors, not client-side: "The broker-side group coordinator now manages the rebalance process, which simplifies the client-side implementation."
- Server-side options: `range` (co-partitions topics) and `uniform` (default, distributes uniformly); both are "sticky" to minimize partition movement.
- Separate standard benchmarking showed "better latencies than the previous version" when consumers are added/removed, but no percentages given for those scenarios.

## Related
- [[Research: KIP-848 client-side assignors & rack-aware assignment]]
- concepts/server-side-assignor
- [[Research: KIP-848 next-gen consumer rebalance protocol]]
