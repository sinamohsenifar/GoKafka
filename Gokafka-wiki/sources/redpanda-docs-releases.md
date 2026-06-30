---
title: "Redpanda Docs — Release Notes index (current)"
type: source
category: Research
subcategory: Redpanda
status: reference
tags: [gokafka, source, redpanda, kip-848, kip-932, compatibility, research]
url: https://docs.redpanda.com/current/reference/releases/
updated: 2026-06-30
---

# Redpanda Docs — Release Notes index

- Official Redpanda release-notes landing page. Latest version listed at fetch time: **v26.1.12** (date not shown on the index page itself). (medium confidence — index page links out to per-release notes on GitHub)
- **No mention** of KIP-848, "next-gen consumer group", `ConsumerGroupHeartbeat`, `group.protocol`, or "consumer group protocol" anywhere on the index. (high confidence)
- **No mention** of KIP-932, "share groups", or "queues" on the index. (high confidence)
- Implication: as of the current Redpanda release line, neither KIP-848 nor KIP-932 has shipped or been announced in release notes. Absence of a release-note entry is consistent with the open, unanswered feature request (#29223).
- The index is a directory of links; per-release detail lives in linked GitHub release pages, so a future entry could appear there before this index is re-summarized.

## Related
- [[Research: Redpanda next-gen group & share-group roadmap]]
- [[compatibility/redpanda]] · [[compatibility/broker-quirks]]
