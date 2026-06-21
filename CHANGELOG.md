# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.1] - 2026-06-20

### Added
- `simplequeue verify` — SQLite integrity check, schema version validation, required tables, and safe row counts (exit **1** when unhealthy).
- `VerifyResult` and `SQLiteBackend.verify_database()` for programmatic health checks.
- `tests/unit/test_module_inventory.py` — parametrised import smoke tests for every package module, safe demos, and public API surfaces.
- `tests/unit/test_storage_coverage_polish.py` — scoped lease release, redeliver events, purge scope, and validation helpers.
- `tests/integration/test_sqlite_polish.py` — stale-receipt concurrency, DLQ sweep paths, and purge-with-DLQ integration cases.
- README design-decisions section, CI/coverage badges, and `docs/api.md` input-bounds documentation.
- `docs/security.md` — explicit security posture (auth, encryption, tenancy, network scope).
- `docs/operations.md` — deployment and operating model; `scripts/run-worker.sh` and `scripts/run-worker.ps1` examples.
- `simplequeue verify` — integrity check, schema version, and table readability (exit **1** when unhealthy).
- `simplequeue purge --dry-run` — preview terminal-row deletions without removing data.
- Live purge selects deletion candidates inside `BEGIN IMMEDIATE` and guards deletes by terminal status.

### Changed
- Test suite expanded to **566** tests with **~99%** line coverage on `src/simplequeue`.
- Validation helpers use field-centric error messages (for example `'cache_ttl' must be > 0`).
- Project metadata refreshed (`.gitignore`, `LICENSE`, `pyproject.toml`, requirements files).

### Fixed
- Removed unreachable pre-claim DLQ branch in `SQLiteBackend.claim_next` (exhausted available rows are handled by `sweep()` / `move_exhausted_to_dlq()`).

## [0.2.0] - 2026-06-20

### Added
- `create_queue()` factory wiring `QueueConfig` to `Queue` (backend, stats cache TTL, queue name).
- `validate_library_config()` for library-side config validation.
- `tests/unit/test_round5_fixes.py` for cache TTL validation and idempotency exhaustion coverage.
- `Queue.purge_terminal(all_queues=True)` for database-wide terminal row cleanup.
- `validate_join_timeout()` shared helper for finite join timeouts.
- `simplequeue purge` CLI (`--all-queues`, `--older-than`, `--older-than-days 0`, `--include-dead-lettered`).
- `simplequeue dlq --all-queues`.
- `Queue(..., cache_ttl_seconds=...)` for library stats-cache TTL control.
- `Queue.purge_terminal(include_dead_lettered=True)` and 7-day default retention when `older_than` is omitted.
- `create_backend()` exported from top-level `simplequeue`; schema version tracking in `schema_meta`.
- `docs/migrations.md` for schema upgrade policy and idempotency concurrency notes.
- GitHub Actions CI (pytest, mypy, ruff) and regression test modules for Rounds 1–5.

### Changed
- `BackgroundSweeper`, `RepeatingScheduler`, `Worker`, and `WorkerPool` reject non-finite timing parameters.
- `Worker.join()`, `WorkerPool.join()`, and scheduler `join()` validate ad-hoc timeout arguments.
- `SQLiteBackend` validates `queue_name` on all queue-scoped public methods.
- Idempotency enqueue uses `INSERT OR IGNORE` plus live-row select.
- `dequeue()` releases expired leases database-wide (aligned with `sweep()`).
- `shared_stats_cache()` keyed by `(path, ttl, id(clock))`.
- Config validates `queue_name`; command-scoped validation for worker/cache fields.
- CLI `make_queue()` delegates to `create_queue()`.
- `TTLCache` and library queue construction reject non-finite `cache_ttl` values.

### Fixed
- `dequeue(visibility_timeout=timedelta(inf))` no longer raises `OverflowError`.
- Library auto stats cache respects `cache_ttl_seconds` / `config.cache_ttl`.
- `init-db` with `max_attempts = 0` in config no longer fails at backend construction.
- `purge_terminal()` no longer deletes all terminal rows when `older_than` is omitted.
- `list_dead_letters(all_queues=True, queue_name=...)` raises `ValueError`.
- `Queue.list_dead_letters(None)` incorrectly scoped to the instance queue name.
- Empty/whitespace queue names rejected at `Queue` construction.
- Opening a database with a newer schema than the library raises `StorageError`.

## [0.1.0] - 2026-01-01

### Added
- Durable SQLite-backed queue library and `simplequeue` CLI.
- At-most-once and at-least-once delivery with receipt-handle validation.
- Race-safe atomic claim (`BEGIN IMMEDIATE` + status/version-guarded update).
- Visibility-timeout redelivery, retries, and a dead-letter queue with requeue.
- Threaded `WorkerPool` with graceful shutdown modes and a background sweeper.
- Per-queue stats with an in-process TTL cache and structured JSON logging.
- Injectable clock (`SystemClock`/`FakeClock`) for deterministic time tests.
- Teaching-only "unsafe" examples and a reproducible demo suite.
