# GoKafka Capabilities & Use Cases

Reference for what GoKafka supports today, how it maps to Kafka ecosystem features, and how the integration test stack exercises each connection type.

## Connection & security protocols

| Protocol | Encryption | Authentication | GoKafka config | Integration test |
|----------|------------|----------------|----------------|------------------|
| `PLAINTEXT` | No | No | default (`SecurityPlaintext`) | `TestIntegrationSecurityPlaintext` |
| `SSL` | TLS | Optional mTLS (client cert) | `TLSOnlySecurity(tls)` | `TestIntegrationSecuritySSL`, `TestIntegrationSecurityMTLS` |
| `SASL_PLAINTEXT` | No | SASL | `SCRAMPlaintextSecurity(...)` or `WithSecurity` | `TestIntegrationSecuritySASLPlain`, SCRAM-256/512 |
| `SASL_SSL` | TLS + SASL | SASL over TLS | `SCRAMSecurity(...)` / `PlainSecurity(...)` | `TestIntegrationSecuritySASLSSL` |

### SASL mechanisms

| Mechanism | Status | Typical use case |
|-----------|--------|------------------|
| `PLAIN` | ✅ | Dev/test; always pair with TLS in production |
| `SCRAM-SHA-256` | ✅ | Production password auth |
| `SCRAM-SHA-512` | ✅ | Production password auth (recommended) |
| `OAUTHBEARER` | ✅ wire | Cloud IdP / OIDC token auth |
| `GSSAPI` (Kerberos) | 🔜 | Enterprise AD/Kerberos SSO |

### TLS options

- CA trust (`TLSConfig.CAFile`)
- Client certificate mTLS (`CertFile`, `KeyFile`)
- `ServerName` / `InsecureSkipVerify` for local dev

## Data & serialization

| Format | API | Use case |
|--------|-----|----------|
| Raw bytes | `Record.Value []byte` | Opaque payloads, protobuf, Avro bytes you encode yourself |
| UTF-8 string | `StringPayload` | Logs, simple text |
| JSON | `JSONPayload` | Events, REST-style documents |
| Confluent wire | `EncodeSchemaWire` / `DecodeSchemaWire` | Schema Registry–managed schemas |
| Record headers | `Record.Headers` | Trace ids, content-type, metadata |

Compression on produce: **none**, **gzip**, **snappy**, **lz4** (stdlib / pure Go). **zstd** is not supported (no stdlib implementation).

## Client roles & use cases

### Producer patterns

| Pattern | API | Use case |
|---------|-----|----------|
| Sync produce | `Producer.ProduceSync` / `ProduceSyncResult` | Request/response, low volume, must know offset |
| Async produce | `NewAsyncProducer().Run` | High throughput, delivery callbacks |
| Batch / linger | `NewBatchProducer()` | Micro-batching, throughput tuning |
| Idempotent | default `Idempotent: true` | No duplicate sequences on retry |
| Transactional | `BeginTransaction`, `ProduceWithinTxn`, `Commit` | Exactly-once consume-transform-produce |

### Consumer patterns

| Pattern | API | Use case |
|---------|-----|----------|
| Poll loop | `Consumer.Poll` | Custom processing, backpressure control |
| Worker pool | `Consumer.Run` + `Concurrency.ConsumerWorkers` | Parallel handlers with commit-after-success |
| Consumer groups | `WithConsumerGroup` | Scalable consumption, partition assignment |
| Static membership | `WithGroupInstanceID` | Faster rebalance on rolling restarts |
| Cooperative sticky | `AssignorCooperativeSticky` | Incremental rebalance (KIP-429 style) |
| Pause / resume | `Pause` / `Resume` | Maintenance, drain control |
| read_committed | `IsolationReadCommitted` | Read only committed transactional records |
| Seek / offsets | `Seek`, `SeekToBeginning`, `SeekToEnd` | Replay, skip, testing |

### Admin & operations

| Operation | API |
|-----------|-----|
| Topics CRUD | `CreateTopic`, `DeleteTopics`, `ListTopics` |
| Partitions | `CreatePartitions`, describe metadata |
| Configs | `DescribeTopicConfigs`, `AlterTopicConfigs`, `IncrementalAlterTopicConfigs` |
| Groups | `ListConsumerGroups`, `DescribeConsumerGroups`, `DeleteConsumerGroups` |
| Offsets | `DeleteConsumerGroupOffsets` |
| Cluster | `DescribeCluster` (metadata-based) |
| ACLs | `CreateACLs`, `DescribeACLs`, `DeleteACLs` |

## Listener layout (integration Docker stack)

The `docker-compose.yml` stack exposes:

| Port | Listener | Purpose |
|------|----------|---------|
| 9092 | `PLAINTEXT` | Unencrypted dev / baseline tests |
| 9093 | `SSL` | TLS encryption |
| 9094 | `SASL_PLAINTEXT` | PLAIN + SCRAM without TLS |
| 9095 | `SASL_SSL` | PLAIN + SCRAM over TLS |
| 8081 | Schema Registry | Confluent REST API for JSON schemas |

Internal broker traffic uses `INTERNAL://kafka:29092` (PLAINTEXT). Controller uses `CONTROLLER://kafka:29093`.

### Test credentials (local only)

| User | Password | Mechanisms |
|------|----------|--------------|
| `gokafka` | `gokafka-secret` | PLAIN (JAAS), SCRAM-SHA-256/512 (init script) |
| `alice` | `alice-secret` | PLAIN (JAAS) |
| `admin` | `admin-secret` | Inter-broker PLAIN |

## Running the full integration matrix

```bash
bash scripts/gen-test-certs.sh   # once, or in CI
docker compose up -d
export KAFKA_BROKERS=localhost:9092
export KAFKA_BROKERS_PLAINTEXT=localhost:9092
export KAFKA_BROKERS_SSL=localhost:9093
export KAFKA_BROKERS_SASL_PLAINTEXT=localhost:9094
export KAFKA_BROKERS_SASL_SSL=localhost:9095
export SCHEMA_REGISTRY_URL=http://localhost:8081/apis/ccompat/v6
go test -tags=integration -count=1 -timeout=5m ./...
```

## Gaps & roadmap (not yet in GoKafka)

- Kerberos / GSSAPI
- zstd compression
- Share consumer groups (KIP-932)
- Full flexible protocol for all API versions (v9+ metadata, join, fetch flex paths)
- DescribeCluster wire API (API 60) — metadata fallback today
- OAuth token refresh / OIDC helper utilities
- Kafka Connect, ksqlDB, Flink — external systems; Schema Registry REST client is supported

## Related ecosystem services

| Service | In compose? | GoKafka support |
|---------|-------------|-----------------|
| Apache Kafka broker | ✅ | Full client |
| Confluent Schema Registry | ✅ | REST client + wire encoding |
| Kafka Connect | ❌ | N/A (separate product) |
| ksqlDB | ❌ | N/A |

Add Connect or ksqlDB to compose when testing those pipelines end-to-end; GoKafka remains the client library for application code talking to Kafka directly.
