# GSSAPI / Kerberos SASL

## Status

**Not yet implemented** in stdlib-only builds. Config types exist (`KerberosConfig`, `SASLGSSAPI`) but handshake returns `auth.ErrGSSAPINotSupported`.

## Why it is hard in pure Go

Kafka GSSAPI SASL uses **SPNEGO** over SASL, which typically requires:

- ASN.1 DER encoding of Kerberos tickets (GSS-API tokens)
- Integration with a KDC (MIT Kerberos, Active Directory, etc.)
- Channel binding considerations for TLS + SASL_SSL

The Go standard library provides `crypto` primitives but not a full Kerberos/GSS-API stack.

## Alternatives used in production

| Approach | Notes |
|----------|-------|
| **franz-go** / **sarama** | Mature GSSAPI via build tags or cgo |
| **confluent-kafka-go** | librdkafka handles SPNEGO |
| **OAuthBearer** | Supported in GoKafka wire; use for managed Kafka (Azure Event Hubs, AWS MSK IAM patterns vary) |

## Roadmap

1. Document OAuthBearer integration test (docker JAAS) — **v0.20**
2. Evaluate minimal SPNEGO token pass-through for pre-acquired tickets — research spike
3. Full KDC integration is **out of scope** for stdlib-only v1; may require optional build tag in future major version

## Configuration (future)

```go
gokafka.WithSecurity(gokafka.SecurityConfig{
    SASL: gokafka.SASLConfig{
        Mechanism: gokafka.SASLGSSAPI,
        Kerberos: gokafka.KerberosConfig{
            Principal: "kafka/client@REALM",
            Keytab:    "/path/to/client.keytab",
        },
    },
})
```

Until implemented, the client fails fast at handshake with a clear error pointing to this document.
