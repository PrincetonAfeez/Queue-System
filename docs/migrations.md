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

Concurrent ``enqueue`` calls with the same idempotency key retry the insert up
to ``IDEMPOTENCY_ENQUEUE_MAX_RETRIES`` (25) times before raising
``StorageError``. Under extreme contention, backoff at the caller or a short
sleep between retries may still be needed for pathological load tests.
