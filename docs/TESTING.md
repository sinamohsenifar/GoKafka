# Testing policy

GoKafka uses a tiered test pyramid. **Core integration tests must not skip** unless an optional profile is explicitly disabled via environment.

## Test tiers

| Tier | Command | When |
|------|---------|------|
| **L0 Unit** | `go test -race ./...` | Every PR and push |
| **L1 Integration core** | `go test -tags=integration -run 'Produce|Transaction|Static|Alter|AdminTopic' ./...` | Every PR (CI) |
| **L2 Security** | `go test -tags=integration -run Security ./...` | Every PR (CI) |
| **L3 Compatibility** | See [COMPATIBILITY.md](COMPATIBILITY.md) matrix | Nightly + pre-release |
| **L4 Benchmarks** | `go test -bench=. -benchmem ./internal/... ./schema/...` | Weekly / before minor release |

## Local integration stack

```bash
bash scripts/gen-test-certs.sh
docker compose up -d
# wait ~30s for coordinator + SCRAM init
export KAFKA_BROKERS=localhost:9092
export KAFKA_BROKERS_PLAINTEXT=localhost:9092
export KAFKA_BROKERS_SSL=localhost:9093
export KAFKA_BROKERS_SASL_PLAINTEXT=localhost:9094
export KAFKA_BROKERS_SASL_SSL=localhost:9095
export GOKAFKA_SECRETS_DIR=$PWD/docker/secrets
export SCHEMA_REGISTRY_URL=http://localhost:8081/apis/ccompat/v6
go test -tags=integration -race -timeout=5m ./...
```

## Environment variables

| Variable | Purpose |
|----------|---------|
| `KAFKA_BROKERS` | Primary PLAINTEXT bootstrap |
| `KAFKA_BROKERS_*` | Per-listener profiles for security tests |
| `GOKAFKA_SECRETS_DIR` | TLS certs (`docker/secrets`) |
| `SCHEMA_REGISTRY_URL` | Schema Registry / Apicurio ccompat API |
| `KAFKA_BROKERS_OAUTH` | Optional OAuth listener for `go test -tags="integration,oauth"` |

## Multi-version compatibility (local)

```bash
KAFKA_IMAGE=apache/kafka:4.3.0 docker compose up -d
# re-run integration tests with same env vars
```

## Release gate

Before tagging a minor release:

1. `go test -race ./...` passes on Go 1.22, 1.23, 1.24
2. Integration suite passes on **3.9.2** and **4.3.0**
3. `CHANGELOG.md` and `version.go` updated
4. No unconditional skip on produce, consume, txn, ACL, or alter-config tests
