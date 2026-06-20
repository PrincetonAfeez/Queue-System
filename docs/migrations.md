# Schema Migrations

The SQLite schema version is stored in ``schema_meta`` (key ``version``). Each
library release that changes ``schema.sql`` should bump ``SCHEMA_VERSION`` in
``simplequeue.storage.migrations`` and add an ``if from_version < N`` block in
``_upgrade_schema()``.

## Policy

1. ``init_schema()`` is idempotent: safe to run on every process start.
2. Opening a database with a **newer** schema than the library raises
   ``StorageError`` (upgrade the package).
3. Opening a database with an **older** schema runs incremental upgrades, then
   updates ``schema_meta``.

## Idempotency concurrency

Concurrent ``enqueue`` calls with the same idempotency key use ``INSERT OR
IGNORE`` plus a follow-up select on the live row. A bounded retry loop
(``IDEMPOTENCY_ENQUEUE_MAX_RETRIES``, 25) remains for races where the unique
index blocks on a terminal row or concurrent writers interleave. Under extreme
contention, backoff at the caller may still be needed for pathological load
tests.
