# SQLite Atomic Claims

SQLite serializes writers with database-level locks. The implementation keeps
write transactions short and uses `BEGIN IMMEDIATE` for mutations that claim,
ack, nack, expire, DLQ, or requeue messages.

The claim flow is:

1. Begin an immediate transaction.
2. Release expired leases while holding the write lock.
3. Select the oldest available message for the queue.
4. Update it with a guard on `status` and `version`.
5. Commit immediately.

The guarded update is the important part:

```sql
UPDATE messages
SET status = 'leased',
    attempts = ?,
    leased_at = ?,
    lease_expires_at = ?,
    receipt_handle = ?,
    worker_id = ?,
    version = version + 1
WHERE id = ?
  AND status = 'available'
  AND version = ?;
```

If two consumers race, only one update can affect the row. The loser retries or
finds no available work.

The teaching demo `unsafe-double-claim` shows why an unguarded
select-then-update design is unsafe.

## Deadlock and Starvation

Deadlock is impossible by construction: every mutation acquires exactly one
lock — the single database write lock via `BEGIN IMMEDIATE` — so there is no
lock ordering to violate and no lock-acquisition cycle. Each write transaction
is short (a guarded select plus a guarded update, then commit), so the lock is
held briefly.

Starvation is bounded rather than prevented. Concurrent writers serialize on
the write lock; a waiter blocks for at most `busy_timeout` (30s) before raising,
instead of waiting forever. Readers never block writers and writers never block
readers because the database runs in WAL mode, so read-side operations
(`stats`, `peek`, `inspect`) proceed concurrently with claims. Under sustained
write contention throughput is capped by the single-writer model — an accepted
trade-off of using SQLite rather than a row-locking database such as Postgres.
