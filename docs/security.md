# Security Posture

This document states what **simplequeue** protects against, what it does **not**
protect against, and how to use it responsibly. Read it before treating this
project as a production message broker.

## Scope

**simplequeue** is a local SQLite-backed queue **library** and **CLI**. It is
intended for academic, local, and portfolio use — not as a hardened,
multi-tenant, network-facing broker.

There is **no network service**: no HTTP API, no gRPC endpoint, no remote
admin port. All access is in-process (Python API) or via a local CLI process
reading and writing a SQLite file on disk.

## What this project does not provide

| Capability | Status |
| ---------- | ------ |
| Authentication | **Not provided** — any process with filesystem access can read or mutate the database |
| Authorization | **Not provided** — no roles, ACLs, or per-queue permissions |
| Encryption at rest | **Not provided** — message payloads and metadata are stored as plain JSON/text in SQLite |
| Encryption in transit | **Not applicable** — no network protocol |
| Secret management | **Not provided** — no vault integration, key rotation, or credential storage |
| Tenant isolation | **Not provided** — multiple queue names share one database file with no hard boundary |
| Network hardening | **Not provided** — nothing is exposed to listen on a port |
| Audit logging to a tamper-evident store | **Not provided** — events are written to local SQLite and stdout/stderr logs |

Do **not** store secrets, credentials, payment data, or other sensitive payloads
in queue messages unless **your environment** already enforces the filesystem,
database, and process controls required for that data class.

## What the implementation does protect against

### SQL injection

All SQL uses **parameterized queries**. User-supplied queue names, payloads,
idempotency keys, and receipt handles are bound as parameters — never
interpolated into SQL strings.

### Unsafe payload execution

Payloads are serialized with `json.dumps` / `json.loads` and stored as text.
They are **never** passed to `eval`, `exec`, or dynamic import machinery.

### Stale or forged acknowledgements

At-least-once delivery requires a valid **receipt handle**, active **lease**,
and matching **status** in a single guarded `UPDATE`. A worker that lost a race
or held an expired lease gets `success=False` with a reason such as
`stale_receipt` or `lease_expired` instead of silently corrupting queue state.

### Accidental schema corruption from version skew

Opening a database whose `schema_meta.version` is **newer** than the library
raises `StorageError` rather than proceeding with an incompatible schema.

## Host and filesystem responsibilities

SQLite file permissions, directory ACLs, disk encryption, backups, and process
isolation are entirely **outside** this library:

- Anyone who can read the `.db` file can read queued payloads.
- Anyone who can write the `.db` file can enqueue, ack, nack, or purge messages.
- WAL/SHM sidecar files (`*.db-wal`, `*.db-shm`) follow the same trust model as
  the main database file.
- On shared machines, restrict the database path to trusted users and use OS-level
  permissions (for example `chmod 600` on Unix) where appropriate.

The library does not set or enforce file modes after creating a database.

## CLI and process trust model

The CLI runs with the **same privileges** as the invoking user. Config files
(`--config`) and `--db` paths are trusted input: a malicious config file is
equivalent to arbitrary local code configuration, not remote code execution.

Graceful shutdown (Ctrl-C / SIGTERM on Unix) stops workers cooperatively; it is
not a security boundary.

## Teaching and demo code

Modules under `src/simplequeue/teaching/` and `unsafe-*` demos **deliberately**
show incorrect patterns (for example ack-by-message-id without receipt handles).
They exist for learning only and must not be copied into production integrations.

## Recommended use

| Use case | Appropriate? |
| -------- | -------------- |
| Local dev, coursework, portfolio demos | Yes |
| Single-user automation on a trusted machine | Yes, with filesystem permissions |
| Multi-user server without per-user DB isolation | No |
| Internet-facing job queue | No — use a broker designed for that threat model |
| Storing API keys or PII without external encryption | No |

## Reporting issues

If you discover a security bug in this library (for example a SQL injection or
memory-safety issue in native code — there is no native code today), please
open a GitHub issue on the repository. This is an academic/portfolio project;
response times are best-effort.

## Summary

**simplequeue** prioritizes **correct queue semantics on a trusted local host**
over **defense against untrusted callers or network attackers**. Treat the
SQLite database as sensitive data at rest, run it only in environments you
control, and choose a production-grade broker if you need authentication,
encryption, tenancy, or network exposure.
