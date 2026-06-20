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
| `create_backend` | Build a `StorageBackend` from `QueueConfig` (exported from `simplequeue`) |

## `list_dead_letters`

- ``list_dead_letters()`` — dead letters for this queue instance's name.
- ``list_dead_letters("other")`` — dead letters for a named queue.
- ``list_dead_letters(all_queues=True)`` — all unrequeued dead letters in the database.

Passing both ``queue_name`` and ``all_queues=True`` raises ``ValueError``.

## `purge_terminal`

``Queue.purge_terminal(older_than=...)`` deletes terminal rows on or before the
cutoff. When ``older_than`` is omitted, the default retention is
``DEFAULT_PURGE_RETENTION_DAYS`` (7 days) relative to the queue clock.

Set ``include_dead_lettered=True`` to also remove old ``dead_lettered`` rows and
their ``dead_letters`` records. CLI: ``simplequeue purge --older-than-days 7
--include-dead-lettered``.

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
