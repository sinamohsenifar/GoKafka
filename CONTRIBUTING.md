# Contributing to GoKafka

## Dependencies

**Do not add third-party modules.** GoKafka must remain stdlib-only. If you need compression or crypto, use:

- `compress/gzip`, `crypto/tls`, `crypto/hmac`, `encoding/binary`, etc.

Reject PRs that add `require (` entries to `go.mod` (except the module line itself).

## Releases

1. Bump `Version` in `version.go`
2. Update `CHANGELOG.md` under `[Unreleased]` → new version section
3. Merge to `main`
4. Tag: `git tag vX.Y.Z && git push origin vX.Y.Z`
5. Create GitHub Release with changelog notes

Pre-release tags: `v0.2.0-rc.1`

## Branching

- `main` — stable development
- `v0.x` maintenance branches optional for patch releases

## Testing

- Unit tests for `internal/wire` and `internal/protocol`
- **Integration tests are required** for protocol, consumer, producer, and admin changes
- See [docs/TESTING.md](docs/TESTING.md) for the full test pyramid

```bash
docker compose up -d
export KAFKA_BROKERS=localhost:9092
go test -tags=integration -timeout=5m ./...
```

## Code style

- Match existing naming and error wrapping (`fmt.Errorf("gokafka: ...: %w", err)`)
- Keep public API minimal; put protocol details in `internal/`
