# Python API Reference

Public exports from `simplequeue` (`__all__`):

| Symbol | Role |
|--------|------|
| `Queue` | Main queue API (enqueue, dequeue, ack, nack, sweep, stats, purge) |
| `SQLiteBackend` | Durable SQLite storage |
| `Worker` / `WorkerPool` | Threaded consumers |
| `BackgroundSweeper` | Periodic `Queue.sweep()` (one per database) |
| `Processor` | Callable protocol: `(Delivery) -> bool \| None` |
| `ClaimBudget` | Limits concurrent dequeues (CLI `--limit`) |
| `DeliveryMode` | `AT_LEAST_ONCE` / `AT_MOST_ONCE` |
| `ShutdownMode` | Worker shutdown behavior |
| `QueueConfig` | Configuration dataclass |
| `shared_stats_cache` / `StatsCache` | Per-DB stats TTL cache |
| `Clock` / `FakeClock` | Injectable time for tests |
| `AckResult` / `NackResult` | Ack/nack outcomes with `reason` |
| `Message` / `MessageDetails` / `DeadLetter` | Inspection types |
| `QueueStatsSnapshot` | Stats counters (`delivered` = delivery attempts) |
| `QueueError` | Domain errors (base) |
| `IdempotencyConflict` | Live idempotency key collision |
| `DeadLetterNotFound` | Invalid DLQ requeue target |
| `StorageError` | Backend / serialization failures |
| `payload_idempotency_key` | Derive key from payload hash |
| `create_backend` / `create_queue` | Build storage / queue from `QueueConfig` (exported from `simplequeue`) |
| `validate_library_config` | Validate a `QueueConfig` for library factory entry points |

## `Queue` constructor

``Queue(backend, queue_name, *, clock=None, stats_cache=None, cache_ttl_seconds=1.0, logger=None)``

- ``clock`` — inject ``FakeClock`` in tests; defaults to ``SystemClock``.
- ``stats_cache`` — optional shared ``StatsCache``; when omitted, SQLite backends
  use ``shared_stats_cache(db_path, ttl_seconds=cache_ttl_seconds, clock=clock)``.
- ``cache_ttl_seconds`` — TTL for auto-created stats caches (must be finite and
  ``> 0``); ignored when ``stats_cache`` is supplied.

Prefer ``create_queue(config)`` when building from ``QueueConfig``.

## `validate_library_config`

``validate_library_config(config)`` checks queue name, finite positive timing
fields, and ``worker_count >= 1``. Unlike CLI command-scoped validation,
``max_attempts < 1`` is allowed (``create_backend()`` clamps it). ``create_queue``
calls this automatically; use it directly when constructing ``Queue`` manually
from a ``QueueConfig``.

When ``stats_cache`` is supplied to ``Queue(...)``, ``cache_ttl_seconds`` is
ignored (the injected cache owns TTL semantics).

## Input bounds

- ``queue_name`` — non-empty, trimmed, at most 256 characters.
- Timing fields (``visibility_timeout``, ``poll_interval``, ``cache_ttl``, etc.)
  must be finite; positive where enforced by validation helpers.
- Payloads and idempotency keys are stored as JSON/SQLite text with no library-level
  max length; very large values are limited by SQLite and filesystem constraints.

## `list_dead_letters`

- ``list_dead_letters()`` — dead letters for this queue instance's name.
- ``list_dead_letters("other")`` — dead letters for a named queue.
- ``list_dead_letters(all_queues=True)`` — all unrequeued dead letters in the database.

Passing both ``queue_name`` and ``all_queues=True`` raises ``ValueError``.

CLI: ``simplequeue dlq --all-queues`` lists unrequeued dead letters across the
whole database. Do not combine ``--queue`` and ``--all-queues``.

## `create_queue`

``create_queue(config, queue_name=None, *, clock=None, stats_cache=None)`` builds a
``Queue`` with ``create_backend(config)``, wires ``config.cache_ttl`` into
``shared_stats_cache``, and validates the queue name. Prefer this over manual
``Queue(create_backend(...), ...)`` when using ``QueueConfig``.

## `purge_terminal`

``Queue.purge_terminal(older_than=...)`` deletes terminal rows on or before the
cutoff. When ``older_than`` is omitted, the default retention is
``DEFAULT_PURGE_RETENTION_DAYS`` (7 days) relative to the queue clock.

Pass ``all_queues=True`` to purge every queue in the database file (mutually
exclusive with an explicit ``queue_name``).

Set ``include_dead_lettered=True`` to also remove old ``dead_lettered`` rows and
their ``dead_letters`` records. CLI: ``simplequeue purge --older-than-days 7
--include-dead-lettered``. Use ``--older-than-days 0`` or ``--older-than
2026-01-01T00:00:00+00:00`` for an explicit cutoff; ``--all-queues`` purges every
queue in the database file.

## `shared_stats_cache`

Caches are keyed by ``(database_path, ttl_seconds, id(clock))``. Pass the same
clock object when sharing a cache in tests.

## CLI vs library time

CLI commands always use wall-clock time (``SystemClock``). Inject ``FakeClock``
through ``Queue(..., clock=...)`` in tests and library code.

## Ack / nack reasons

Operations return results; they do not raise for stale receipts.

| Reason | Meaning |
|--------|---------|
| `receipt_handle_not_found` | Unknown receipt |
| `not_leased` | Message not in `leased` (includes at-most-once after delete) |
| `lease_expired` | Ack rejected; lease past deadline |
| `stale_receipt` | Guarded update lost a race |

Expired-lease **nack** releases the message (redelivery or DLQ) instead of
returning `lease_expired`.

## Idempotency errors

| Operation | Error |
|-----------|-------|
| Enqueue, live key + different payload | `IdempotencyConflict` |
| DLQ requeue, live key collision | `IdempotencyConflict` |

## CLI exit codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Storage / unexpected error |
| 2 | Config / validation (includes malformed JSON payload) |
| 3 | Inspect miss |
| 4 | `QueueError` (includes `IdempotencyConflict`, duplicate sweeper) |
| 130 | Interrupted (Ctrl-C; SIGTERM on Unix uses the same cleanup path) |

## Related docs

- [delivery_guarantees.md](delivery_guarantees.md)
- [workers.md](workers.md)
- [architecture.md](architecture.md)
- [state_transitions.md](state_transitions.md)
- [migrations.md](migrations.md)
