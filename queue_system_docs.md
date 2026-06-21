# Architecture Decision Record
## App — Queue System
**Queue Infrastructure Group | Document 1 of 5**
**Status: Accepted**

---

## Context

The Queue Infrastructure group requires a durable Python queue library and CLI that demonstrates real queueing semantics: at-most-once delivery, at-least-once delivery, visibility timeouts, retries, dead-letter queues, receipt handles, worker pools, background sweeping, stats caching, idempotency, operational inspection, and concurrency-safe storage.

The application is named **Queue System Library** and is packaged as `simplequeue-system` with the console command `simplequeue`. The product is the library and CLI. SQLite is the durable source of truth. Read-side conveniences such as stats caching must not determine correctness.

The project is intentionally scoped as a local/academic queue, not a production distributed broker. It uses SQLite because it makes durability, transactions, indexes, and race-safe updates visible without requiring a separate server such as RabbitMQ, Redis, SQS, or Postgres.

---

## Decisions

### Decision 1 — Use SQLite as the durable source of truth

**Chosen:** Persist queue state in SQLite tables for messages, dead letters, queue events, and schema metadata.

**Rejected:** In-memory-only queue, JSON file queue, Redis, Postgres, RabbitMQ, or SQS.

**Reason:** SQLite gives a durable, inspectable database and supports transactions in a local development environment. It is appropriate for a capstone queue while keeping deployment simple.

---

### Decision 2 — Use short `BEGIN IMMEDIATE` transactions

**Chosen:** Every mutating storage operation opens a short-lived connection, starts `BEGIN IMMEDIATE`, performs guarded updates, commits, and closes.

**Rejected:** Long-lived transactions or optimistic reads followed by unguarded writes.

**Reason:** SQLite has database-level write locks rather than row-level locks. `BEGIN IMMEDIATE` serializes writers early, and guarded `WHERE` clauses ensure races fail safely.

---

### Decision 3 — Use guarded status/version updates for claims

**Chosen:** Claim operations update a candidate row only when its status and version still match the row read earlier.

**Rejected:** Selecting a message and then updating it without a version/status guard.

**Reason:** Multiple consumers may race to claim the same message. Guarded updates mean only one consumer can win.

---

### Decision 4 — Support both at-most-once and at-least-once delivery

**Chosen:** `DeliveryMode` supports `at-most-once` and `at-least-once`.

**Rejected:** Implementing only one delivery model.

**Reason:** The two modes teach different trade-offs. At-most-once deletes the message at delivery time and may lose work on worker crash. At-least-once leases the message and requires ack/nack, allowing redelivery after failure.

---

### Decision 5 — Use receipt handles for ack/nack

**Chosen:** Every delivery receives a unique receipt handle, and `ack()` / `nack()` validate the current handle, status, and lease.

**Rejected:** Acking by message id alone.

**Reason:** Message ids are stable across redeliveries, but a stale worker must not ack a message after another worker has received a newer lease. Receipt handles represent the current lease instance.

---

### Decision 6 — Return structured ack/nack results

**Chosen:** `ack()` and `nack()` return `AckResult` / `NackResult` with `success`, `message_id`, `status`, `reason`, and DLQ indicators.

**Rejected:** Raising exceptions for common stale-receipt races.

**Reason:** Stale receipts are expected in distributed-style worker systems. Workers should log and continue rather than crash for normal race outcomes.

---

### Decision 7 — Keep message states explicit

**Chosen:** Store messages as `available`, `leased`, `acked`, `deleted`, or `dead_lettered`, with a documented legal transition table.

**Rejected:** Using booleans such as `processed` / `failed` only.

**Reason:** Queues are state machines. Explicit states make delivery guarantees, terminal retention, retry, and DLQ behavior auditable.

---

### Decision 8 — Implement retry-to-DLQ behavior

**Chosen:** Delivery attempts increment on delivery. `nack()` returns a message to available until attempts are exhausted, then moves it to a dead-letter table.

**Rejected:** Deleting failed messages permanently or retrying forever.

**Reason:** Queues need bounded failure behavior. DLQ preserves payload, reason, attempts, timestamps, and final status for operational inspection and optional requeue.

---

### Decision 9 — Use partial idempotency uniqueness

**Chosen:** The SQLite schema enforces uniqueness of `(queue_name, idempotency_key)` only for live `available` and `leased` rows.

**Rejected:** Global uniqueness forever.

**Reason:** Idempotency should prevent duplicate live work, but once a message is terminal, the same idempotency key can be reused for new work.

---

### Decision 10 — Make stats cache read-side only

**Chosen:** `Queue.stats(cached=True)` may use an in-process TTL cache. Mutations invalidate relevant entries, and sweeps invalidate all queue stats for that database.

**Rejected:** Making cached stats part of delivery correctness.

**Reason:** Counts are useful for CLI/operator display, but queue correctness must depend only on SQLite state and guarded mutations.

---

### Decision 11 — Share stats cache per SQLite database

**Chosen:** SQLite-backed queues default to `shared_stats_cache(db_path, ttl, clock)`.

**Rejected:** Isolated per-Queue stats caches by default.

**Reason:** Multiple queue names may share one database. A shared read-side cache keeps stats coherent across Queue instances in the same process.

---

### Decision 12 — Provide a worker abstraction and worker pool

**Chosen:** `Worker` polls the Queue API, processes deliveries, and finishes via ack/nack based on delivery mode and handler result. `WorkerPool` creates named worker threads.

**Rejected:** CLI-only consumer loops.

**Reason:** The library should expose reusable worker primitives, not just a command-line consume loop.

---

### Decision 13 — Model shutdown modes explicitly

**Chosen:** Worker shutdown supports `finish-current`, `nack-current`, and `abandon-current`.

**Rejected:** Always killing worker threads or always finishing work.

**Reason:** Shutdown is part of delivery semantics. Different consumers may prefer graceful completion, immediate release, or lease-expiry-based redelivery.

---

### Decision 14 — Use a background sweeper for lease recovery

**Chosen:** `BackgroundSweeper` periodically calls `Queue.sweep()` and enforces at most one sweeper per SQLite database path.

**Rejected:** Implicit lease recovery on every worker loop only.

**Reason:** Long-running at-least-once consumers need expired leases reclaimed. A single database-wide sweeper avoids duplicate maintenance loops.

---

### Decision 15 — Keep unsafe demos separate

**Chosen:** Unsafe teaching demos demonstrate double-claim, stale ack, missing visibility timeout, and stats-cache correctness failures outside production code.

**Rejected:** Mixing unsafe paths into the main queue implementation.

**Reason:** Unsafe examples are valuable for explaining the safe design, but must remain isolated from production semantics.

---

## Consequences

**Positive:**
- Queue correctness is durable and inspectable through SQLite.
- At-most-once and at-least-once trade-offs are explicit.
- Receipt handles prevent stale workers from acking newer leases.
- Guarded updates make concurrent claims race-safe.
- Retry and DLQ behavior are operationally visible.
- Idempotency protects live duplicate work without blocking future reuse forever.
- Worker pool and sweeper abstractions are reusable from Python.
- CLI gives practical operational commands.
- Stats cache improves displays without affecting delivery correctness.
- Unsafe demos explain why the safe implementation exists.

**Negative / Trade-offs:**
- SQLite has a single-writer bottleneck.
- This is not a distributed broker.
- No network protocol or authentication layer is included.
- Long-running at-least-once consumers need a sweeper or periodic `sweep` command for lease recovery.
- Terminal rows remain until purged.
- Multiple process coordination is limited to SQLite locking semantics.
- Stats cache is process-local.

---

## Alternatives Not Explored

- Redis streams.
- RabbitMQ / AMQP.
- AWS SQS.
- Postgres `FOR UPDATE SKIP LOCKED`.
- HTTP/gRPC queue server.
- Distributed consumer heartbeats.
- Priority queues.
- Delay queue wheel.
- Message TTL eviction.
- Multi-tenant authorization.
- Encryption at rest.
- Exactly-once delivery claims.

---

*Constitution reference: Article 1 (Python fundamentals and architectural thinking), Article 3.3 (scope discipline), Article 4 (quality proportional to scope), Article 5 (trade-off documentation), Article 6 (verification), and Article 7 (progressive complexity).*

---


# Technical Design Document
## App — Queue System
**Queue Infrastructure Group | Document 2 of 5**

---

## Overview

Queue System Library is a durable SQLite-backed message queue. It exposes both a Python API and a CLI. Its core is the `Queue` class, which owns queue semantics and delegates persistence to a `StorageBackend`. The default backend is `SQLiteBackend`.

**Package:** `simplequeue-system`  
**Import package:** `simplequeue`  
**CLI:** `simplequeue`  
**Version:** `0.2.1`  
**Python:** `>=3.11`  
**Runtime dependencies:** none  
**Backend:** SQLite  
**Test baseline:** 566 tests per README, CI with pytest/ruff/mypy

---

## System Context

```text
CLI / Library Caller
  │
  ▼
Queue
  ├── enqueue
  ├── dequeue
  ├── ack
  ├── nack
  ├── sweep
  ├── stats
  ├── peek / inspect
  ├── DLQ requeue
  └── purge terminal rows
       │
       ▼
StorageBackend
       │
       ▼
SQLiteBackend
  ├── messages
  ├── dead_letters
  ├── queue_events
  └── schema_meta
```

---

## Main Package Areas

```text
src/simplequeue/
  __init__.py
  config.py
  defaults.py

  core/
    queue.py
    delivery.py
    message.py
    modes.py
    results.py
    states.py
    validation.py
    exceptions.py

  storage/
    base.py
    sqlite_backend.py
    schema.sql
    serializers.py
    factory.py
    migrations.py

  workers/
    worker.py
    worker_pool.py
    shutdown.py
    processor.py
    claim_budget.py

  scheduling/
    clock.py
    scheduler.py
    sweeper.py

  reliability/
    dlq.py
    idempotency.py
    leases.py
    receipt_handles.py
    retry.py

  cache/
    ttl_cache.py
    stats_cache.py

  observability/
    events.py
    logging.py
    stats.py

  cli/
    main.py
    commands/
    shutdown.py

  teaching/
    unsafe examples
```

---

## Core Data Models

### `Message`

Fields:
- id
- queue_name
- payload
- status
- attempts
- max_attempts
- created_at
- updated_at
- available_at
- leased_at
- lease_expires_at
- acked_at
- dead_lettered_at
- idempotency_key
- last_error
- version
- receipt_handle
- worker_id
- redeliveries

Purpose:
- represents one queue row.

---

### `Delivery`

Fields:
- message_id
- receipt_handle
- queue_name
- payload
- attempt
- delivery_mode
- leased_at
- lease_expires_at

Purpose:
- represents one delivery attempt. The receipt handle is required for ack/nack.

---

### `DeadLetter`

Fields:
- id
- original_message_id
- queue_name
- payload
- failure_reason
- attempts
- created_at
- dead_lettered_at
- final_status
- requeued_at
- requeued_message_id

---

### Result Objects

`ClaimResult`:
- delivery
- mutated

`AckResult`:
- success
- message_id
- status
- reason

`NackResult`:
- success
- message_id
- status
- reason
- moved_to_dlq

`LeaseReleaseResult`:
- redelivered
- dead_lettered
- total property

---

## Message State Machine

Statuses:

```text
available
leased
acked
deleted
dead_lettered
```

Legal transitions:

```text
available → leased
leased → acked
leased → available
leased → dead_lettered
available → dead_lettered
available → deleted
dead_lettered → available
```

There is no `expired` message status. Expiration in this project means lease expiration, not message TTL removal.

---

## Delivery Modes

### At-most-once

```text
available → deleted at delivery time
```

Properties:
- message is terminal before processing starts
- worker crash after delivery loses work
- later ack/nack returns `not_leased`

Use case:
- disposable or low-value work.

---

### At-least-once

```text
available → leased → acked
available → leased → available
available → leased → dead_lettered
```

Properties:
- delivery creates a lease and receipt handle
- ack marks terminal success
- nack returns available or dead-letters when attempts exhausted
- expired leases can be swept and redelivered
- consumer must be idempotent

---

## SQLite Schema

Tables:
- `schema_meta`
- `messages`
- `dead_letters`
- `queue_events`

Important indexes:
- partial unique idempotency index on live messages
- claim index by queue/status/available time/creation/id
- lease-expiry index
- receipt-handle index
- dead-letter queue index
- queue-event indexes

---

## SQLite Backend Design

### Connection behavior

Each operation:
1. opens a new SQLite connection,
2. enables foreign keys,
3. sets busy timeout,
4. sets WAL journal mode,
5. starts `BEGIN IMMEDIATE` for mutations,
6. commits or rolls back,
7. closes the connection.

This keeps file handles short-lived and makes concurrency behavior clear.

---

### Enqueue

```text
enqueue(queue, payload, idempotency_key, available_at, max_attempts)
  ├── validate queue name
  ├── serialize payload as JSON
  ├── BEGIN IMMEDIATE
  ├── INSERT message as available
  ├── if idempotency key exists on live row:
  │     ├── return existing id if payload matches
  │     └── raise IdempotencyConflict if payload differs
  ├── record enqueue event
  └── commit
```

Idempotent enqueue retries bounded races up to a configured retry count.

---

### Claim next

```text
claim_next(queue, mode, visibility_timeout, worker_id, now)
  ├── BEGIN IMMEDIATE
  ├── release expired leases database-wide
  ├── find earliest available due message
  ├── if none: return ClaimResult(None, mutated)
  ├── generate receipt handle
  ├── increment attempt
  ├── if at-most-once:
  │     ├── guarded UPDATE available/version → deleted
  │     └── return Delivery with no lease expiry
  └── if at-least-once:
        ├── guarded UPDATE available/version → leased
        ├── set receipt, worker id, lease expiry
        └── return Delivery
```

Ordering:
```text
available_at <= now, attempts < max_attempts
ORDER BY created_at ASC, id ASC
```

---

### Ack

```text
ack(receipt_handle)
  ├── find message by receipt handle
  ├── if missing: success=False, reason=receipt_handle_not_found
  ├── if not leased: success=False, reason=not_leased
  ├── if lease expired: success=False, reason=lease_expired
  ├── guarded UPDATE leased + active lease → acked
  ├── clear receipt/worker/lease columns
  ├── record ack event
  └── success=True
```

---

### Nack

```text
nack(receipt_handle, reason)
  ├── find message by receipt handle
  ├── validate status/lease
  ├── if attempts exhausted:
  │     ├── move to DLQ
  │     └── success=True, moved_to_dlq=True
  └── otherwise:
        ├── guarded UPDATE leased → available
        ├── clear lease fields
        ├── store last_error
        ├── increment redeliveries
        └── success=True
```

---

### Sweep

`Queue.sweep()` does database-wide maintenance:
- release expired leases
- move exhausted available messages to DLQ
- invalidate the entire stats cache when anything changes

---

## Idempotency

`idempotency_key` dedupes only live messages:

```sql
CREATE UNIQUE INDEX ...
WHERE idempotency_key IS NOT NULL
  AND status IN ('available', 'leased')
```

Implications:
- duplicate live payload with same key returns existing id
- duplicate live key with different payload raises conflict
- terminal rows free the key for future work
- DLQ requeue checks for live conflicts

---

## Stats Cache

`Queue.stats(cached=True)`:
1. looks up queue stats in `StatsCache`,
2. returns cached snapshot on hit,
3. computes backend stats on miss,
4. stores snapshot for TTL.

Invalidation:
- enqueue invalidates one queue
- ack/nack success invalidates one queue
- requeue invalidates one queue
- purge invalidates affected queue
- sweep invalidates all queues because it acts database-wide

Correctness rule:
- delivery behavior never depends on cached stats.

---

## Workers

### Worker loop

```text
while not stopped:
  ├── respect claim budget if configured
  ├── dequeue
  ├── if no delivery: sleep poll interval
  ├── process delivery
  ├── if at-most-once: no ack/nack needed
  ├── if at-least-once and handler succeeds: ack
  └── if at-least-once and handler fails: nack
```

Worker failures are logged. Ack/nack exceptions are caught so a transient storage error does not silently kill the worker thread.

---

### WorkerPool

Creates N named workers and exposes:
- `start()`
- `stop()`
- `join(timeout)`
- context-manager behavior

Workers must be at least 1.

---

### Shutdown modes

- `finish-current`: complete current processing, then ack/nack.
- `nack-current`: release a claimed but unprocessed message or nack after processing during shutdown.
- `abandon-current`: leave lease to expire and let sweeper redeliver.

---

## BackgroundSweeper

`BackgroundSweeper` wraps a `RepeatingScheduler` that calls `Queue.sweep()` on an interval.

Rules:
- interval must be positive
- at most one sweeper per SQLite database path
- stop + join releases registry slot
- scheduler logs and continues after callback errors

---

## Configuration

`QueueConfig` supports:
- queue_name
- delivery_mode
- visibility_timeout
- max_attempts
- backend
- worker_count
- cache_ttl
- database_path
- logging_level
- sweeper_interval
- poll_interval
- idle_timeout
- shutdown_mode

Config load precedence:
```text
defaults → config file → CLI flags
```

Formats:
- JSON
- TOML / TML

Unknown keys are ignored. Invalid types and invalid values are rejected with clear messages.

---

## CLI Architecture

`simplequeue.cli.main`:
1. installs graceful shutdown handlers,
2. builds argparse parser,
3. merges config and CLI overrides,
4. configures logging,
5. dispatches to command module handler,
6. maps errors to exit codes.

Commands are registered by command modules.

---

## Known Limits

- SQLite single-writer throughput limit.
- Local library/CLI only; no network layer.
- No authentication or authorization.
- No encryption at rest.
- Long-running at-least-once consumers need sweeper support.
- Terminal messages remain until purge.
- Stats cache is process-local.
- No exactly-once delivery guarantee.

---

## Verification Summary

The repository configures:
- Python 3.11+
- zero runtime dependencies
- package data for `storage/schema.sql` and `py.typed`
- pytest test path under `tests`
- coverage source `simplequeue`
- coverage fail-under 95 in `pyproject.toml`
- strict mypy over `simplequeue`
- ruff over `src` and `tests`
- GitHub Actions on Python 3.11 and 3.12
- README states 566 tests and roughly 99% coverage

---

*Constitution reference: Article 4 (engineering quality), Article 6 (behavior verification), Article 7 (progressive complexity), and Article 8 (valid learner work).*

---


# Interface Design Specification
## App — Queue System
**Queue Infrastructure Group | Document 3 of 5**

---

## Public Python API

### Import

```python
from simplequeue import (
    BackgroundSweeper,
    DeliveryMode,
    QueueConfig,
    ShutdownMode,
    WorkerPool,
    create_queue,
)
```

Additional exports include:
- `Queue`
- `SQLiteBackend`
- `create_backend`
- `Delivery`
- `Message`
- `DeadLetter`
- `MessageDetails`
- `AckResult`
- `NackResult`
- `LeaseReleaseResult`
- `QueueStatsSnapshot`
- `StatsCache`
- `shared_stats_cache`
- `FakeClock`
- `Worker`
- `ClaimBudget`
- `payload_idempotency_key`
- domain exceptions

---

## Queue Construction

### Config factory

```python
config = QueueConfig(database_path="queue.db", cache_ttl=2.0)
queue = create_queue(config, "emails")
queue.init_schema()
```

Behavior:
- validates library config
- creates SQLite backend
- wires shared stats cache for SQLite backend
- uses config cache TTL

---

### Direct construction

```python
from simplequeue import Queue, SQLiteBackend

backend = SQLiteBackend("queue.db", default_max_attempts=3)
queue = Queue(backend, "emails")
queue.init_schema()
```

---

## `Queue.enqueue`

```python
message_id = queue.enqueue(
    {"to": "user@example.com"},
    idempotency_key="email:user@example.com",
    max_attempts=3,
)
```

Parameters:
- `payload`: JSON-serializable value
- `idempotency_key`: optional string
- `available_at`: optional schedule datetime
- `max_attempts`: optional positive integer

Returns:
- integer message id

Errors:
- `ValueError` for invalid max attempts
- `IdempotencyConflict` when live key exists with different payload
- `StorageError` for SQLite failures

Side effects:
- inserts message
- records enqueue event
- invalidates stats cache for queue

---

## `Queue.dequeue`

```python
delivery = queue.dequeue(
    delivery_mode=DeliveryMode.AT_LEAST_ONCE,
    visibility_timeout=10,
    worker_id="worker-1",
)
```

Parameters:
- `delivery_mode`: `at-most-once` or `at-least-once`
- `visibility_timeout`: positive seconds or timedelta
- `worker_id`: optional id

Returns:
- `Delivery` or `None`

Behavior:
- validates positive finite visibility timeout
- may release expired leases before claiming
- returns earliest available due message
- increments attempts
- writes receipt handle and worker id

---

## `Queue.ack`

```python
result = queue.ack(delivery.receipt_handle)
```

Returns `AckResult`:
- `success`
- `message_id`
- `status`
- `reason`

Common failure reasons:
- `receipt_handle_not_found`
- `not_leased`
- `lease_expired`
- `stale_receipt`

---

## `Queue.nack`

```python
result = queue.nack(delivery.receipt_handle, reason="processor failed")
```

Returns `NackResult`:
- `success`
- `message_id`
- `status`
- `reason`
- `moved_to_dlq`

Behavior:
- returns message to available when retry remains
- moves to DLQ when max attempts exhausted
- records failure reason

---

## `Queue.sweep`

```python
summary = queue.sweep()
```

Returns:
```python
{"expired": 3, "dead_lettered": 1}
```

Behavior:
- database-wide lease recovery
- moves exhausted messages to DLQ
- invalidates entire stats cache when mutated

---

## `Queue.requeue_dead_letter`

```python
new_id = queue.requeue_dead_letter(message_id)
```

Behavior:
- requeues a dead-lettered message into the same queue
- fails if message is not found in the DLQ
- fails on live idempotency conflict

---

## Stats / Inspection

### `stats`

```python
snapshot = queue.stats(cached=True)
```

Use `cached=False` for a fresh read.

### `peek`

```python
messages = queue.peek(limit=10)
```

Requires `limit >= 1`.

### `inspect`

```python
details = queue.inspect(message_id)
```

Returns `MessageDetails` with message, events, and optional dead-letter record.

### `list_queues`

```python
names = queue.list_queues()
```

### `list_dead_letters`

```python
queue.list_dead_letters()
queue.list_dead_letters(all_queues=True)
```

### `purge_terminal`

```python
removed = queue.purge_terminal(older_than=cutoff, include_dead_lettered=True)
```

Default retention is 7 days when `older_than` is omitted.

---

## Worker API

### Single worker

```python
from simplequeue import Worker, DeliveryMode

worker = Worker(
    queue,
    lambda delivery: True,
    delivery_mode=DeliveryMode.AT_LEAST_ONCE,
    visibility_timeout=10,
)
worker.start()
worker.stop()
worker.join(5)
```

---

### Worker pool

```python
with WorkerPool(
    queue,
    lambda delivery: True,
    workers=4,
    shutdown_mode=ShutdownMode.FINISH_CURRENT,
):
    ...
```

Rules:
- workers must be at least 1
- join timeout must be valid
- poll interval must be positive

---

### Background sweeper

```python
with BackgroundSweeper(queue, interval=1.0):
    ...
```

Rules:
- interval must be positive
- one sweeper per SQLite database path

---

## Public CLI Interface

### Console command

```powershell
simplequeue <command> [options]
```

Global flags:
- `--version`
- `--config`
- `--db`

---

## Commands

### `init-db`

```powershell
simplequeue init-db --db queue.db
```

Creates/updates schema.

---

### `produce`

```powershell
simplequeue produce --db queue.db --queue emails --payload '{"to":"user@example.com"}'
simplequeue produce --db queue.db --queue emails --count 100 --payload-template '{"job":"{n}"}'
simplequeue produce --db queue.db --queue emails --idempotent --payload '{"to":"user@example.com"}'
```

Important options:
- `--queue`
- `--payload`
- `--payload-template`
- `--count`
- `--idempotent`
- `--idempotency-key`
- `--max-attempts`

---

### `consume`

```powershell
simplequeue consume --db queue.db --queue emails --mode at-least-once --workers 4 --visibility-timeout 10 --limit 100
```

Important options:
- `--mode at-most-once|at-least-once`
- `--workers`
- `--visibility-timeout`
- `--limit`
- `--duration`
- `--idle-timeout`
- `--poll-interval`
- `--shutdown-mode finish-current|nack-current|abandon-current`
- `--sweeper`
- `--fail-every`

Behavior:
- without `--limit` or `--duration`, runs until interrupted
- with bounds, exits after idle timeout when queue is drained and no work is in flight

---

### `stats`

```powershell
simplequeue stats --db queue.db --queue emails
simplequeue stats --db queue.db --queue emails --no-cache
```

---

### `peek`

```powershell
simplequeue peek --db queue.db --queue emails --limit 10
```

---

### `inspect`

```powershell
simplequeue inspect --db queue.db --queue emails --message-id 1
```

Exit code `3` when target message is not found or wrong queue scope.

---

### `list-queues`

```powershell
simplequeue list-queues --db queue.db
```

---

### `sweep`

```powershell
simplequeue sweep --db queue.db --queue emails
```

---

### `dlq`

```powershell
simplequeue dlq --db queue.db --queue emails
```

---

### `dlq-requeue`

```powershell
simplequeue dlq-requeue --db queue.db --queue emails --message-id 1
```

---

### `purge`

```powershell
simplequeue purge --db queue.db --queue emails --older-than-days 7
simplequeue purge --db queue.db --queue emails --include-dead-lettered
```

---

### `demo`

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

---

## Config File Interface

TOML example:

```toml
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

A top-level `[queue]` table is also accepted.

Supported extensions:
- `.json`
- `.toml`
- `.tml`

---

## CLI Exit Codes

| Code | Meaning |
|---:|---|
| `0` | Success |
| `1` | Unexpected runtime/storage error |
| `2` | Usage/configuration error |
| `3` | Inspect target not found or wrong queue scope |
| `4` | Domain error such as `QueueError`, idempotency conflict, duplicate sweeper |
| `130` | Interrupted / SIGTERM cleanup path |

---

## Side Effects

| Operation | Side Effect |
|---|---|
| `init-db` | Creates SQLite schema |
| `produce` | Inserts message rows and queue events |
| `consume` | Claims/deletes/leases messages, starts workers, may ack/nack |
| `sweep` | Releases expired leases and dead-letters exhausted messages |
| `dlq-requeue` | Creates new available row from DLQ message |
| `purge` | Deletes terminal rows older than cutoff |
| `stats` | May populate in-process stats cache |
| `demo` | Writes demo database unless temporary/unsafe isolated demo |

---

*Constitution reference: Article 4 (input/output boundaries), Article 6 (verification), and Article 8 (understandable and verifiable work).*

---


# Runbook
## App — Queue System
**Queue Infrastructure Group | Document 4 of 5**

---

## Requirements

### Runtime

- Python 3.11+
- SQLite through Python standard library
- No required third-party runtime dependencies

### Development

- pytest
- pytest-cov
- mypy
- ruff

---

## Installation

### Editable development install

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
```

### Reproducible dev toolchain

```powershell
pip install -r requirements-dev.txt
```

### Confirm install

```powershell
simplequeue --version
```

---

## First Smoke Test

```powershell
simplequeue init-db --db queue.db
simplequeue produce --db queue.db --queue emails --payload '{"to":"user@example.com"}'
simplequeue peek --db queue.db --queue emails --limit 10
simplequeue stats --db queue.db --queue emails --no-cache
```

Expected:
- database file is created
- message is inserted
- peek shows the message
- stats show available depth

---

## At-Least-Once Demo

```powershell
simplequeue init-db --db queue.db
simplequeue produce --db queue.db --queue emails --payload '{"to":"user@example.com"}'
simplequeue consume --db queue.db --queue emails --mode at-least-once --workers 1 --limit 1
simplequeue stats --db queue.db --queue emails --no-cache
```

Expected:
- message is leased, processed, and acked
- terminal row remains until purge

---

## At-Most-Once Demo

```powershell
simplequeue produce --db queue.db --queue disposable --payload '{"job":"low-value"}'
simplequeue consume --db queue.db --queue disposable --mode at-most-once --workers 1 --limit 1
```

Expected:
- message is deleted at delivery time
- later ack/nack is not required

---

## Retry and DLQ Demo

```powershell
simplequeue produce --db queue.db --queue retry --payload '{"job":"fail"}' --max-attempts 2
simplequeue consume --db queue.db --queue retry --mode at-least-once --workers 1 --limit 2 --fail-every 1
simplequeue dlq --db queue.db --queue retry
```

Expected:
- first failure nacks/redelivers
- exhausted attempts move message to DLQ
- DLQ command shows failed payload and reason

---

## Receipt Handle Stale Ack Demo

```powershell
simplequeue demo --db demo.db receipt-handle-stale-ack
```

Expected:
- stale receipt cannot ack a message after lease expiry/redelivery

---

## Sweeper Operation

### Manual sweep

```powershell
simplequeue sweep --db queue.db --queue emails
```

### Background sweeper during consume

```powershell
simplequeue consume --db queue.db --queue emails --mode at-least-once --workers 4 --sweeper
```

Rule:
- use at most one sweeper per SQLite database file.

---

## Purging Terminal Rows

```powershell
simplequeue purge --db queue.db --queue emails --older-than-days 7
simplequeue purge --db queue.db --queue emails --older-than-days 7 --include-dead-lettered
```

Purpose:
- remove old `acked` and `deleted` rows
- optionally remove old `dead_lettered` rows

---

## Idempotency

### Derived idempotency key

```powershell
simplequeue produce --db queue.db --queue emails --idempotent --payload '{"to":"user@example.com"}'
```

### Explicit idempotency key

```powershell
simplequeue produce --db queue.db --queue emails --idempotency-key email:user@example.com --payload '{"to":"user@example.com"}'
```

Behavior:
- duplicate live message with same payload returns existing row behavior
- duplicate live key with different payload raises domain error
- terminal state frees key for reuse

---

## Library Usage

```python
from simplequeue import DeliveryMode, QueueConfig, create_queue

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
        # Perform side effect.
        queue.ack(delivery.receipt_handle)
    except Exception as exc:
        queue.nack(delivery.receipt_handle, reason=str(exc))
```

---

## WorkerPool Usage

```python
from simplequeue import BackgroundSweeper, QueueConfig, ShutdownMode, WorkerPool, create_queue

config = QueueConfig(database_path="queue.db")
queue = create_queue(config, "emails")
queue.init_schema()

def handler(delivery):
    print(delivery.payload)
    return True

with WorkerPool(queue, handler, workers=2, shutdown_mode=ShutdownMode.FINISH_CURRENT):
    with BackgroundSweeper(queue, interval=1.0):
        ...
```

---

## Config File

Create `queue.toml`:

```toml
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

Use:

```powershell
simplequeue consume --config queue.toml --limit 100
```

---

## Quality Checks

### Tests

```powershell
pytest
```

### Coverage

```powershell
pytest --cov=simplequeue --cov-report=term-missing
```

### Type check

```powershell
python -m mypy src
```

### Ruff

```powershell
ruff check src tests
```

---

## CI Parity

GitHub Actions runs:
- Ubuntu latest
- Python 3.11 and 3.12
- install `requirements-dev.txt`
- ruff over `src tests`
- mypy over `src`
- pytest with coverage report

---

## Troubleshooting

### Message stuck as leased

Cause:
- worker crashed or stopped in at-least-once mode
- no sweeper reclaimed expired lease

Fix:
```powershell
simplequeue sweep --db queue.db --queue emails
```

or run consume with:
```powershell
--sweeper
```

---

### Duplicate sweeper error

Cause:
- more than one `BackgroundSweeper` is running for the same SQLite database.

Fix:
- stop and join the existing sweeper before starting another.

---

### Ack returns `lease_expired` or `stale_receipt`

Cause:
- receipt handle is no longer current.

Fix:
- do not retry stale ack forever
- log result and allow redelivery/sweeper path

---

### Idempotency conflict

Cause:
- live message already has same idempotency key with different payload.

Fix:
- use a distinct key
- wait for terminal state
- inspect live messages

---

### Stats look stale

Cause:
- cached stats are enabled.

Fix:
```powershell
simplequeue stats --db queue.db --queue emails --no-cache
```

---

### `inspect` exits code 3

Cause:
- message id does not exist or is outside queue scope.

Fix:
- check `peek`, `dlq`, or `list-queues`
- confirm queue name and database path

---

### SQLite locked / slow writes

Cause:
- SQLite single-writer model under concurrent producers/consumers.

Fix:
- reduce concurrency
- use one database per independent workload
- consider a server database for production scaling

---

## Maintenance Notes

- Keep SQLite backend behind `StorageBackend`.
- Keep delivery semantics in `Queue`, not CLI handlers.
- Add tests before changing ack/nack reasons.
- Add tests before changing idempotency behavior.
- Add tests before changing state transitions.
- Preserve receipt-handle validation.
- Preserve `BEGIN IMMEDIATE` + guarded update pattern.
- Keep stats cache read-side only.
- Keep unsafe demos isolated from production code.
- Do not claim exactly-once delivery.

---

*Constitution reference: Article 6 (behavior verification), Article 5 (constraints and trade-offs), and Article 8 (verifiable learner work).*

---


# Lessons Learned
## App — Queue System
**Queue Infrastructure Group | Document 5 of 5**

---

## Why This Design Was Chosen

This design was chosen because queues are deceptively simple. A basic queue can be a list, but a useful durable queue must answer harder questions: what happens if a worker crashes, how does a message become visible again, how are duplicate producers handled, how does a stale worker fail safely, and how are poison messages isolated?

SQLite was a strong fit for the learning goal. It exposes real persistence, transactions, indexes, and contention while avoiding infrastructure overhead. The use of `BEGIN IMMEDIATE` and guarded updates makes the concurrency story tangible.

The `Queue` API is also intentionally stable. CLI handlers and workers call the same public queue methods, which keeps semantics centralized.

---

## What Was Intentionally Omitted

**Exactly-once delivery:** Not promised. At-least-once requires idempotent consumers.

**Distributed broker behavior:** Out of scope for a local SQLite library.

**Network service:** No HTTP/gRPC server, no auth, no multi-tenant boundary.

**Priority queues:** Deferred to keep state transitions and claim order simple.

**Message TTL deletion:** Only lease expiration is implemented.

**Postgres-style row locking:** SQLite is the chosen backend, so guarded updates replace row locks.

**Streaming payloads:** Payloads are JSON-serialized values.

**Encryption:** Database contents are plaintext and protected only by filesystem permissions.

---

## Biggest Weakness

The biggest weakness is SQLite’s single-writer behavior. It is excellent for local durability and learning, but it is not a high-throughput distributed broker. Heavy concurrent producers and consumers can bottleneck on database-level writes.

The second weakness is that long-running at-least-once processing depends on a sweeper. Without periodic sweeping, expired leases remain `leased` and are not redelivered.

The third weakness is operational scope. The tool has good CLI inspection, but no network API, central operator dashboard, access control, encryption, or distributed observability.

---

## Scaling Considerations

**If throughput grows:**
- use Postgres with row-level locking or `SKIP LOCKED`
- split queues across database files
- add connection pooling
- batch produce and batch claim
- measure lock wait times

**If deployment becomes multi-user:**
- add network server layer
- add authentication and authorization
- add audit identity
- encrypt payloads or integrate KMS

**If reliability demands increase:**
- add heartbeat/lease extension
- add poison-message policy controls
- add per-queue retention policies
- add operator alerts for DLQ growth

**If features expand:**
- add priority queues
- add delayed/scheduled queue commands
- add payload schema validation hooks
- add metrics export

---

## What the Next Refactor Would Be

1. **Lease extension API** — allow long-running workers to renew leases safely.

2. **Batch operations** — add batch enqueue/claim/ack for fewer SQLite writes.

3. **Postgres backend prototype** — keep `StorageBackend` contract while demonstrating row-level locks.

4. **Operational metrics export** — expose Prometheus-style counters for CLI/library users.

5. **Retention policy object** — make purge behavior configurable per queue.

---

## What This Project Taught

- **Queues are state machines.** The message table is not just a list; each row moves through controlled states.

- **Receipt handles matter.** Message ids alone cannot protect against stale workers.

- **At-least-once requires idempotency.** Redelivery is a feature, not a bug, but consumers must be safe to repeat.

- **SQLite concurrency needs guarded writes.** Transactions serialize writers, and status/version conditions ensure only valid mutations apply.

- **DLQ is an operational feature.** Failed work should remain inspectable, not disappear.

- **Sweeping is part of reliability.** Expired leases need a maintenance path.

- **Stats are not correctness.** Cached stats are useful, but delivery behavior must be grounded in durable state.

- **Unsafe demos sharpen understanding.** Double claims, stale ack, no visibility timeout, and cache correctness demos explain the architecture better than assertions alone.

---

*Constitution v2.0 checklist: This document satisfies Article 5 (trade-off documentation), Article 6 (verification), and Article 7 (progressive complexity) for Queue System.*
