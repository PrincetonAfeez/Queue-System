# Queue System Library

[![CI](https://github.com/PrincetonAfeez/Queue-System/actions/workflows/ci.yml/badge.svg)](https://github.com/PrincetonAfeez/Queue-System/actions/workflows/ci.yml)
![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![Version](https://img.shields.io/badge/version-0.2.1-green)
![Tests](https://img.shields.io/badge/tests-566-blue)
![Coverage](https://img.shields.io/badge/coverage-%7E99%25-brightgreen)

A durable SQLite-backed queue library and CLI that demonstrates at-most-once delivery, at-least-once delivery, worker pools, visibility timeouts, retries, dead-letter handling, stats caching, and operational inspection.

The library and CLI are the product. The database is the source of truth for queue correctness; caching and CLI displays are read-side conveniences only.

## Design decisions

- **SQLite + `BEGIN IMMEDIATE`** — single-writer durability with guarded updates instead of row-level locks; appropriate for an academic/local queue.
- **Partial idempotency index** — uniqueness applies to live (`available`/`leased`) rows only, so keys become reusable after terminal ack/DLQ.
- **Ack/nack return results** — stale receipts and races surface as `success=False` with a `reason`, not exceptions, so workers can log and continue.
- **`create_queue(config)`** — library entry point wires backend, stats-cache TTL, and validated config in one call.

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

Run the test and type checks:

```powershell
pytest
python -m mypy src
ruff check src tests
```

The suite currently collects **566 tests** across unit, integration, CLI smoke,
worker, and concurrency modules.

With dev dependencies installed, generate a coverage report (currently **~99%** line
coverage on `src/simplequeue`):

```powershell
pytest --cov=simplequeue --cov-report=term-missing
```

For a reproducible toolchain, install the pinned versions:

```powershell
pip install -r requirements-dev.txt
```

Confirm the install with `simplequeue --version`.

## Tests

The suite lives under `tests/` and is organized by concern:

| Directory | Focus |
| --------- | ----- |
| `tests/unit/` | Config, serializers, cache, clock, CLI handlers, module inventory, maximum coverage |
| `tests/integration/` | End-to-end queue semantics, regressions, backend edge cases |
| `tests/cli/` | Subprocess smoke tests for the `simplequeue` entry point |
| `tests/workers/` | Worker pools, shutdown modes, background sweeper |
| `tests/concurrency/` | Atomic claims and concurrent idempotency |
| `tests/teaching/` | Unsafe examples that demonstrate known failure modes |

Shared fixtures (including a `queue_factory` for SQLite-backed queues) are in
`tests/conftest.py`. Run `pytest --co -q` to list all collected tests (566 at
last count).

## CLI Quick Start

```powershell
simplequeue init-db --db queue.db

simplequeue produce `
  --db queue.db `
  --queue emails `
  --payload '{"to":"user@example.com"}'

simplequeue produce `
  --db queue.db `
  --queue emails `
  --count 100 `
  --payload-template '{"job":"{n}"}'

simplequeue consume `
  --db queue.db `
  --queue emails `
  --mode at-least-once `
  --workers 4 `
  --visibility-timeout 10 `
  --limit 100

simplequeue stats --db queue.db --queue emails --no-cache
simplequeue peek --db queue.db --queue emails --limit 10
simplequeue list-queues --db queue.db
simplequeue sweep --db queue.db --queue emails
simplequeue dlq --db queue.db --queue emails
simplequeue purge --db queue.db --queue emails --older-than-days 7
```

Every command supports `--help`, which lists all flags. The `consume` and
`produce` commands have several beyond those shown above (for example
`--shutdown-mode`, `--idempotent`, `--duration`, `--idle-timeout`,
`--poll-interval`, `--fail-every`).

Without `--limit` or `--duration`, `consume` runs until interrupted (Ctrl-C).
When either bound is set, the consumer also exits after `--idle-timeout` seconds
with an empty queue and no in-flight work (idle drain), even if the limit has
not been reached. Use at most one `--sweeper` per SQLite database file.

## Configuration

Settings come from defaults, then an optional config file, then CLI flags — each
layer overrides the previous. Pass a JSON or TOML file with `--config`:

```toml
# queue.toml  (a top-level [queue] table is also accepted)
queue_name = "emails"
delivery_mode = "at-least-once"
visibility_timeout = 30.0
max_attempts = 3
backend = "sqlite"
worker_count = 4
cache_ttl = 1.0
database_path = "queue.db"
logging_level = "INFO"
sweeper_interval = 1.0
poll_interval = 0.25
idle_timeout = 1.0
shutdown_mode = "finish-current"
```

Config files may use `.json`, `.toml`, or `.tml` extensions.

```powershell
simplequeue consume --config queue.toml --queue emails --limit 100
```

Unknown keys are ignored. Invalid types or values (a non-numeric timeout, an
unknown `delivery_mode` or `backend`, non-finite floats such as `NaN`, or
out-of-range numbers such as `max_attempts = 0` on produce/consume) are rejected
with a clear message. Worker-related fields (`worker_count`, `poll_interval`,
`idle_timeout`, `sweeper_interval`, `visibility_timeout`) are validated only for
commands that use them (for example `consume`); `init-db` accepts a config file
with `worker_count = 0` or `max_attempts = 0` because it never starts workers
or enqueues messages.

The CLI always uses wall-clock time (`SystemClock`). Inject `FakeClock` through
`Queue(..., clock=...)` in library code and tests.

## Exit Codes

| Code | Meaning |
| ---- | ------- |
| 0 | success |
| 1 | unexpected runtime error (including storage/I/O failures) |
| 2 | usage or configuration error (bad arguments, invalid config file) |
| 3 | `inspect` target message not found (or wrong queue scope) |
| 4 | domain error (`QueueError`, including `IdempotencyConflict` and duplicate sweeper) |
| 130 | interrupted (Ctrl-C; SIGTERM on Unix maps to the same cleanup path) |

## Python API

See [docs/api.md](docs/api.md) for the full public surface. Core usage:

```python
from simplequeue import (
    BackgroundSweeper,
    DeliveryMode,
    QueueConfig,
    ShutdownMode,
    WorkerPool,
    create_queue,
)

config = QueueConfig(database_path="queue.db", cache_ttl=2.0)
queue = create_queue(config, "emails")
queue.init_schema()

queue.enqueue({"to": "user@example.com"}, max_attempts=3)

delivery = queue.dequeue(
    delivery_mode=DeliveryMode.AT_LEAST_ONCE,
    visibility_timeout=10,
    worker_id="worker-1",
)

if delivery:
    try:
        # Do the side effect here.
        queue.ack(delivery.receipt_handle)
    except Exception as exc:
        queue.nack(delivery.receipt_handle, reason=str(exc))

with WorkerPool(queue, lambda d: True, workers=2, shutdown_mode=ShutdownMode.FINISH_CURRENT):
    with BackgroundSweeper(queue, interval=1.0):
        pass  # process messages
```

Worker shutdown and join semantics: [docs/workers.md](docs/workers.md).

## Delivery Guarantees

At-most-once deletes the message at delivery time. If the worker crashes after
dequeue and before processing finishes, the work is lost and will not be
redelivered. This is acceptable for low-value or disposable work.

At-least-once leases the message and only marks it acked after a valid
`ack(receipt_handle)`. If the worker crashes, the lease expires and the message
can be redelivered. This protects work, but consumers must be idempotent because
the same message can be processed more than once.

Long-running `consume` in at-least-once mode does **not** reclaim expired leases
automatically unless you pass `--sweeper` or run `simplequeue sweep` separately.
Without periodic sweeping, crashed workers leave messages stuck in `leased`.

Demo commands:

```powershell
simplequeue demo --db demo.db at-most-once-loss
simplequeue demo --db demo.db at-least-once-redelivery
```

## Receipt Handles

Every delivery gets a unique receipt handle. `ack()` and `nack()` validate the
current receipt handle, message status, and active lease in one guarded storage
operation. A stale worker cannot ack or nack a message after the lease expires
or after another worker receives a new lease.

```powershell
simplequeue demo --db demo.db receipt-handle-stale-ack
```

## SQLite Correctness

SQLite does not provide Postgres-style row locks. This implementation uses
short `BEGIN IMMEDIATE` write transactions with guarded updates:

```sql
UPDATE messages
SET status = 'leased', receipt_handle = ?, version = version + 1
WHERE id = ?
  AND status = 'available'
  AND version = ?;
```

The transaction serializes writers, and the status/version guard means only one
consumer can win a claim.

## Retry And DLQ

Attempts increment when a message is delivered. `nack()` makes the message
available again until `max_attempts` is exhausted. After that, the message is
marked `dead_lettered` and copied into `dead_letters` with the payload, queue
name, failure reason, attempts, and timestamps. DLQ messages are not consumed
again unless explicitly requeued.

```powershell
simplequeue demo --db demo.db retry-dlq
simplequeue dlq --db demo.db --queue retry
simplequeue dlq-requeue --db demo.db --queue retry --message-id 1
```

## Workers And Graceful Shutdown

`WorkerPool` runs threaded consumers. On `stop()` workers stop accepting new
work and the in-flight message is handled per the configured `ShutdownMode`:

- `finish-current` (default): finish processing, then ack/nack.
- `nack-current`: nack a message that was claimed but not yet processed; if stop
  arrives during processing, the handler runs to completion before nack.
- `abandon-current`: leave the lease to expire so the sweeper redelivers it.

```powershell
simplequeue consume --db queue.db --queue emails --limit 100 --shutdown-mode finish-current
```

Omit both `--limit` and `--duration` to run until Ctrl-C. With `--sweeper`, use
at most one background sweeper per SQLite database, even across queue names.
Without `--sweeper`, plan on periodic `simplequeue sweep` for lease recovery.

At-most-once messages are deleted at delivery time. A later `ack()` or `nack()`
on the same receipt returns `not_leased` because the row is already terminal.

Worker ack/nack calls are guarded, so a transient storage error is logged as a
`worker_failure` rather than silently killing a worker thread.

## Idempotency

`enqueue(payload, idempotency_key=...)` dedupes only against **live**
(available or leased) messages, so a key becomes reusable once its previous
message reaches a terminal state. The CLI can derive a key from the payload:

```powershell
simplequeue produce --db queue.db --queue emails --idempotent --payload '{"to":"user@example.com"}'
```

With `--count` greater than 1, an explicit `--idempotency-key` is suffixed per
message (`my-key:1`, `my-key:2`, …) so each enqueued row gets a distinct key.
With `--idempotent` and identical payloads, the derived key is the same for
every message, so only one row is created unless you use `--payload-template`.

DLQ requeue fails with a domain error if a **live** message already holds the
same idempotency key (for example after re-enqueueing with the same key while
the original row is still `dead_lettered`).

## Stats Cache

`Queue.stats(cached=True)` uses an in-process TTL cache and is computed with the
queue's injected clock. Mutations invalidate the relevant entry (a sweep clears
all entries because it spans queues), but cached stats are still a read-side
optimization: delivery correctness never depends on cached counts. `Queue`
defaults to `shared_stats_cache()` for SQLite backends so multiple queue names
on the same database stay coherent; pass `cache_ttl_seconds=` to `Queue(...)` or
use `create_queue(config)` so `config.cache_ttl` is wired automatically. The
`delivered` field counts delivery attempts (including redeliveries); `to_dict()`
also exposes `delivery_attempts` with the same value. `current_depth` counts
claimable messages; `scheduled_count` counts future-scheduled rows not yet due.

## Demos

```powershell
simplequeue demo --db demo.db basic
simplequeue demo --db demo.db concurrent-workers
simplequeue demo --db demo.db at-most-once-loss
simplequeue demo --db demo.db at-least-once-redelivery
simplequeue demo --db demo.db retry-dlq
simplequeue demo --db demo.db receipt-handle-stale-ack
simplequeue demo --db demo.db unsafe-double-claim
simplequeue demo --db demo.db unsafe-stale-ack
simplequeue demo --db demo.db unsafe-no-visibility-timeout
simplequeue demo --db demo.db unsafe-cache-correctness
simplequeue demo --db demo.db all
```

Use the same `--db` path when running multiple demos in sequence so later
commands (for example `stats`) can inspect data left by earlier demos.
Run `demo all` to execute every safe demo against one database file.
Without `--db` or `--config`, safe demos use a temporary database; unsafe teaching
demos always ignore `--db` and run in isolated teaching setups.

## Limitations

- **Single SQLite writer** — throughput is capped by database-level write locks; see [docs/sqlite_atomic_claims.md](docs/sqlite_atomic_claims.md).
- **Terminal row retention** — `acked` and `deleted` messages remain until you call `Queue.purge_terminal()` or `simplequeue purge`. By default the library keeps the last 7 days; pass `older_than` explicitly for custom retention. Use `--include-dead-lettered` / `include_dead_lettered=True` to prune old DLQ rows.
- **No network layer** — local library and CLI only; see Security below.

## Security

This is a local, single-trust library and CLI, not a networked service. It does
not authenticate callers, authorize operations, or encrypt data at rest: the
SQLite database is whatever path the caller supplies and is readable by anyone
with filesystem access. All SQL uses parameterized queries (no string
interpolation), so payloads cannot inject SQL, and payloads are stored as JSON
and never evaluated. Treat the database file and message payloads as trusted,
and protect them with filesystem permissions if needed.

## Project Layout

```text
src/simplequeue/
  core/          public queue API, models, states, results
  storage/       StorageBackend contract and SQLite implementation
  workers/       threaded worker and worker pool
  scheduling/    clock, fake clock, scheduler, sweeper
  reliability/   retry, DLQ, lease, idempotency, receipt helpers
  cache/         TTL stats cache
  observability/ stats, events, and structured logging helpers
  cli/           argparse entry point, per-command modules, and demos
  teaching/      unsafe examples kept out of production code
```

Further notes live in `docs/`:

- [docs/api.md](docs/api.md) — public Python API, ack/nack reasons, exit codes
- [docs/workers.md](docs/workers.md) — worker pools, shutdown, join semantics
- [docs/delivery_guarantees.md](docs/delivery_guarantees.md)
- [docs/architecture.md](docs/architecture.md)
- [docs/state_transitions.md](docs/state_transitions.md)

## License

[MIT](LICENSE) — Copyright (c) 2026 Princeton Afeez.
