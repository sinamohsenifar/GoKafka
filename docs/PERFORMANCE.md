# Performance & Best Practices

A practical guide to tuning GoKafka and using it idiomatically. GoKafka is
stdlib-only (no CGO/librdkafka), so there is no native thread pool — throughput
comes from batching, compression, and per-broker concurrency.

---

## Microbenchmarks

Hot-path encode/serialize costs (`go test -bench=. -benchmem`, Apple-class CPU;
your numbers will differ but the ratios hold):

| Benchmark | ns/op | B/op | allocs/op |
|-----------|------:|-----:|----------:|
| Encode Produce request, 1 record | ~836 | 816 | 6 |
| Encode Produce request, 1000-record batch | ~554,000 | ~1.07 MB | **29** |
| Avro serde encode (Confluent wire) | ~55 | 64 | 1 |
| JSON serde encode | ~243 | 96 | 4 |
| wire UUID write | ~14 | 16 | 1 |
| wire uvarint round-trip | ~10 | 0 | 0 |

The headline: a **1000-record batch encodes in ~0.55 ms with only 29 allocations**
(≈0.03 allocs/record) — record batches are encoded into a pooled buffer with a
back-patched length, not one buffer per record. Run them yourself:

```bash
go test -run='^$' -bench=. -benchmem ./internal/protocol/ ./internal/wire/ ./schema/
```

---

## Producer tuning

| Goal | Settings |
|------|----------|
| **Max throughput** | `WithProducer`: larger `BatchSize` (e.g. 10k–100k), `Linger` 5–50 ms, `Compression` `zstd` or `lz4`; use the **async** (`NewAsyncProducer`) or **batch** (`NewBatchProducer`) producer with several `Concurrency.ProducerWorkers`. |
| **Low latency** | `Linger` 0 (send immediately), small `BatchSize`, `Compression` none or `lz4`. |
| **Durability (EOS-grade)** | `Acks: AcksAll`, `Idempotent: true` (dedup + no reordering), broker `min.insync.replicas >= 2`, topic RF ≥ 3. |
| **Ordering** | Enable idempotence — it preserves per-partition order even with retries and in-flight requests. |

Notes:
- **Compression** is per-batch, so it improves with batch size — combine `Linger`/`BatchSize` with `zstd`/`lz4`. All codecs are pure-Go (gzip, snappy, lz4, zstd).
- **Idempotent producer** is the right default for at-least-once → effectively-once produce; it costs one `InitProducerId` round-trip at startup.
- **Transactions** (`WithTransaction` + `BeginTransaction`) give exactly-once across produce + consumer-offset commit (consume-transform-produce). They force `acks=all` and add per-transaction coordinator round-trips — batch work per transaction.
- **Partitioning**: the default `HashPartitioner` uses **murmur2**, matching the Java/librdkafka default, so keyed records co-partition correctly across mixed-client fleets. Use `ProducerPartitionRoundRobin` for keyless fan-out.

## Consumer tuning

| Goal | Settings |
|------|----------|
| **Throughput** | larger `MaxPollRecords`; the consumer fans out fetches **per leader broker concurrently**. |
| **Rebalance stability** | `Assignor: AssignorCooperativeSticky` (incremental rebalance, no stop-the-world); set `GroupInstanceID` for **static membership** so restarts don't trigger a rebalance. |
| **Next-gen groups** | `GroupProtocol: GroupProtocolNextGen` (KIP-848) — server-driven assignment, lighter rebalances. |
| **Correctness with transactions** | `IsolationLevel: IsolationReadCommitted` — aborted-transaction records are filtered out. |
| **Liveness** | keep `HeartbeatInterval` < `SessionTimeout`/3; do slow work off the poll goroutine so heartbeats keep flowing. |

The consumer poll loop should be tight: `Poll` → process → `Commit` (or rely on
auto-commit). On `Poll` returning empty, just poll again — leader changes
(KIP-320) and share-state initialization (KIP-932) self-heal via metadata refresh.

## Connection & robustness

- One connection per broker is reused and requests are pipelined under a lock; no per-call dial.
- Metadata refresh fails over across seeds; transport errors, coordinator-not-ready, and leader-epoch changes are retried (default `RetryConfig`: 10 attempts, ~13 s) — enough to ride out a leader election or broker restart.
- For multi-broker behavior and leader failover, see `docker-compose.multibroker.yml` and the `-tags=multibroker` tests.

## Observability overhead

- Metrics use static label maps and skip hook dispatch when no hooks are registered — near-zero cost when disabled.
- Route logs into your `log/slog` setup with `WithSlogLoggerFrom`; bridge metrics/traces to Prometheus/OpenTelemetry without adding those as direct dependencies.

---

## Anti-patterns

- **`Linger: 0` + tiny batches at high volume** — kills compression ratio and inflates request count. Batch.
- **`acks=all` without `min.insync.replicas`** — gives a false sense of durability; set the broker-side ISR floor.
- **Blocking the poll goroutine** with slow per-record work — risks session timeout and rebalance storms. Hand records to a worker pool; keep polling.
- **One producer per message / per goroutine** — reuse `client.Producer()` (it's safe for concurrent use); creating producers re-runs `InitProducerId`.
- **Disabling idempotence to "go faster"** — idempotence is nearly free and prevents duplicates/reordering on retry.
- **Polling with `context.Background()`** on an empty topic — pass a bounded context; `Poll` blocks up to the deadline.

---

## Design principles GoKafka follows

- **stdlib-only**, single static binary, no CGO — predictable builds and deploys.
- **`context.Context` everywhere**, functional options, typed `*KafkaError` with `IsRetriable`.
- **Negotiated protocol versions** per broker; only v2 record batches (KIP-896).
- **Bounded resource use** — response-frame, decompression, SCRAM-iteration, and HTTP-body caps guard against malicious/corrupt payloads.
- **Race-clean** (`go test -race`), `gofmt`/`vet`/`staticcheck` enforced in CI, tested across Go 1.22–1.26 and Kafka 3.9.2–4.3.0.
