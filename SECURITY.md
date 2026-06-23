# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.20.x  | ✅        |
| 0.19.x  | ✅        |
| 0.18.x  | ✅        |
| 0.17.x  | ✅        |
| < 0.17  | ❌        |

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security-sensitive reports.

Email **sinamohsenifar@gmail.com** with:

- Description and impact
- Steps to reproduce
- Affected version(s)

We aim to acknowledge within 72 hours and provide a fix or mitigation timeline when confirmed.

## Scope

In scope: protocol parsing, TLS/SASL handling, authentication flows, and dependency-free crypto in this repository.

Out of scope: Kafka broker configuration, Docker test certificates under `docker/secrets/` (development only).
