# ZSTD compression status

GoKafka is **stdlib-only** (`go.mod` has zero third-party dependencies). The Go standard library does not include ZSTD (RFC 8878).

## Kafka usage

- Record batch attribute `0x04` indicates ZSTD compression
- Brokers and most Java clients enable ZSTD by default in modern versions

## Current behavior

- `CompressionZstd` is **rejected at config validation** with a clear error (`ErrUnsupportedCompression`)
- `internal/compress` returns an explicit error if ZSTD attribute is seen on fetch

## Implementation roadmap

1. **v0.21** — Pure-Go ZSTD decompressor for consumer fetch (read path first)
2. **v0.22** — ZSTD compressor for produce when payload size reduction justifies CPU cost
3. Benchmark against gzip/snappy/lz4 on representative payloads

A full ZSTD implementation is ~several thousand lines; it will live under `internal/compress/zstd/` without adding `go.mod` dependencies.

## Workaround

Use **gzip**, **snappy**, or **lz4** (all supported in pure Go today), or broker-side compression with client `compression=none` if CPU on brokers is cheaper.
