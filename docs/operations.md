# Deployment and Operating Model

This document explains how to **install**, **run**, and **operate** simplequeue
outside a one-off development session. The honest answer for this project is:
**local library and CLI only** — there is no hosted service, container image, or
orchestration layer to deploy.

## What this project is not

- **Not a hosted queue service** — no SaaS endpoint, no managed broker.
- **Not a network daemon** — nothing listens on a port; workers are ordinary
  OS processes you start and stop.
- **Not Docker-required** — a Python install and a SQLite file are sufficient.
  You *may* wrap the CLI in a container if your environment standardizes on that,
  but the project does not ship a Dockerfile or compose stack.

## Installation

### Runtime only (library + CLI)

From a checkout or sdist:

```powershell
pip install -r requirements.txt
```

Equivalent editable install:

```powershell
pip install -e .
```

Confirm:

```powershell
simplequeue --version
```

**Requirements:** Python 3.11+, no third-party runtime dependencies (stdlib +
SQLite only).

### Development (tests, mypy, ruff)

```powershell
pip install -r requirements-dev.txt
```

## Operating model overview

```text
┌─────────────────┐     produce      ┌──────────────────┐
│  Your app /     │ ───────────────► │  queue.db        │
│  cron / script  │                  │  (SQLite file)   │
└─────────────────┘                  └────────┬─────────┘
                                              │
                    consume / sweep           │
┌─────────────────┐ ◄─────────────────────────┘
│  Worker process │   (same host, same file path)
│  (CLI or lib)   │
└─────────────────┘
```

1. **One SQLite file** is the source of truth (for example `queue.db`).
2. **Producers** enqueue via the CLI (`simplequeue produce`) or Python API
   (`queue.enqueue(...)`).
3. **Consumers** run as long-lived processes (`simplequeue consume`) or embedded
   `WorkerPool` instances in your application.
4. **Sweepers** reclaim expired leases (`simplequeue sweep` or `--sweeper` on
   consume — at most **one** sweeper per database file).
5. **Operators** inspect, purge, and manage DLQ rows with the other CLI
   commands (`stats`, `peek`, `dlq`, `purge`, etc.).

All components must agree on the **same `--db` path** (or config
`database_path`).

## First-time database setup

Initialize schema before first use (safe to re-run):

```powershell
simplequeue init-db --db queue.db
```

Or rely on lazy init: most commands call `init_schema()` automatically on first
access. Explicit `init-db` is useful in setup scripts and documentation.

Schema versioning is documented in [migrations.md](migrations.md). **Keep the
installed package version aligned with the database** — opening a file created
by a newer schema than your library raises `StorageError`.

## Running workers

### CLI (recommended for ops scripts)

Long-running consumer with background sweeper:

```powershell
simplequeue consume `
  --db queue.db `
  --config queue.toml `
  --queue emails `
  --mode at-least-once `
  --workers 4 `
  --visibility-timeout 30 `
  --sweeper `
  --sweeper-interval 5
```

Bounded batch (exits after 100 messages or idle drain):

```powershell
simplequeue consume --db queue.db --queue emails --limit 100 --sweeper
```

Helper scripts in `scripts/` wrap common flags:

```powershell
.\scripts\run-worker.ps1 -Db queue.db -Queue emails -Workers 4
```

```bash
./scripts/run-worker.sh --db queue.db --queue emails --workers 4
```

### Python library (embedded in your app)

```python
from simplequeue import BackgroundSweeper, QueueConfig, WorkerPool, create_queue

config = QueueConfig(database_path="queue.db")
queue = create_queue(config, "emails")
queue.init_schema()

with WorkerPool(queue, lambda d: True, workers=4):
    with BackgroundSweeper(queue, interval=5.0):
        input("Press Enter to stop...\n")
```

Run this process under your application supervisor the same way you would any
other Python worker.

## Sweepers and lease recovery

At-least-once delivery depends on **reclaiming expired leases**:

| Approach | When to use |
| -------- | ----------- |
| `simplequeue consume --sweeper` | Single consumer process also runs a background sweeper |
| `simplequeue sweep --db queue.db` | Cron/Task Scheduler job every minute |
| `BackgroundSweeper` in library code | Embedded workers in your service |

Rules:

- **At most one** background sweeper per SQLite **database file** (not per queue
  name). A second sweeper on the same file raises a domain error.
- Without periodic sweeping, messages stay `leased` after worker crashes until
  another `dequeue`/`claim_next` triggers inline lease release.

## Long-term operation

### Process supervision

The CLI does not daemonize itself. For production-like local operation, use:

- **Windows:** Task Scheduler, NSSM, or a PowerShell loop in `scripts/`
- **Linux/macOS:** systemd unit, supervisord, or cron for bounded `--limit` jobs
- **Any OS:** Your orchestrator (Kubernetes Job, etc.) if you wrap the CLI — the
  project does not ship manifests

Stop gracefully with Ctrl-C or SIGTERM (Unix); workers honor `ShutdownMode` from
config.

### Configuration management

Prefer a checked-in TOML/JSON config plus environment-specific paths:

```powershell
simplequeue consume --config /etc/myapp/queue.toml --db /var/lib/myapp/queue.db
```

See [README.md](../README.md#configuration) for override order (defaults → file →
CLI flags).

### Backup

Back up the SQLite file using normal SQLite practices:

- **Offline copy** when no writer is active (stop consumers briefly), or
- **SQLite backup API** / `sqlite3 .backup` while online (WAL mode is enabled
  by schema)

Also copy `-wal` / `-shm` sidecars if present, or checkpoint WAL before copy.
See [security.md](security.md) for filesystem permission guidance.

### Retention and disk growth

Terminal rows (`acked`, `deleted`, optionally `dead_lettered`) accumulate until
purged:

```powershell
simplequeue purge --db queue.db --queue emails --older-than-days 7 --dry-run
simplequeue purge --db queue.db --queue emails --older-than-days 7
simplequeue purge --db queue.db --all-queues --older-than-days 30 --include-dead-lettered
```

Preview with `--dry-run` first; live purge deletes immediately. Default library retention when `older_than` is omitted is **7 days** for
`purge_terminal()`.

### Upgrades

1. Stop workers (or drain queues).
2. Upgrade the package: `pip install -e .` or install a new version.
3. Run any command against the database (or `init-db`) so migrations apply.
4. Restart workers on the **same** `database_path`.

If the database schema is newer than the library, upgrade the package before
resuming.

## Monitoring and inspection

Operational commands (no separate metrics server):

```powershell
simplequeue verify --db queue.db
simplequeue stats --db queue.db --queue emails --no-cache
simplequeue peek --db queue.db --queue emails --limit 20
simplequeue dlq --db queue.db --queue emails
simplequeue list-queues --db queue.db
```

`verify` runs `PRAGMA integrity_check`, confirms required tables and schema
version, performs safe row counts, and returns exit code **1** when unhealthy.
For deeper corruption analysis, also use SQLite's own backup and
`.recover` tooling alongside normal backup practices.

Structured logs go to stderr/stdout from workers and the CLI; there is no
built-in log aggregation.

## Packaging for distribution

| Goal | Approach |
| ---- | -------- |
| Use in one project | `pip install -e .` or vend the `src/simplequeue` package |
| Share internally | Build sdist/wheel with `python -m build`, install with pip |
| Publish to PyPI | Optional; project is portfolio-oriented, not a published broker |

There is no separate “server” artifact — the wheel contains library + CLI entry
point (`simplequeue`).

## Summary

**Deploy** simplequeue by installing Python, initializing a SQLite file, and
running consumer processes you supervise. **Operate** it with CLI inspection
commands, periodic `sweep`/`purge`, filesystem backups, and package upgrades
that stay in sync with schema migrations. No container platform or cloud service
is required — or provided.
