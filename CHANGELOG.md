# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html). While the
version is `0.x`, the public API may change between releases.

## [Unreleased]

### Added
- `Queue.purge_terminal()` to delete old `acked`/`deleted` rows and cap database growth.
- `Queue.list_dead_letters(all_queues=True)` for database-wide DLQ listing.
- `create_backend()` factory and `schema_meta` version tracking.
- GitHub Actions CI (pytest, mypy, ruff) and `ruff` in dev dependencies.
- `tests/unit/test_implemented_fixes.py` covering validation, cross-queue lease release, and purge.

### Changed
- Config rejects non-finite floats (`NaN`, `inf`); `max_attempts` validated only for produce/consume.
- `dequeue()` releases expired leases database-wide (aligned with `sweep()`).
- `shared_stats_cache()` keyed by `(database_path, ttl_seconds)`.
- Idempotency enqueue retries increased to 10; `list_queues()` includes `dead_letters` names.
- Fixed `docs/workers.md` shutdown-mode table; expanded API and delivery-guarantee docs.

### Fixed
- `Queue.list_dead_letters(None)` incorrectly scoped to the instance queue name.
- Empty/whitespace queue names rejected at `Queue` construction.

## [0.1.0]

### Added
- Durable SQLite-backed queue library and `simplequeue` CLI.
- At-most-once and at-least-once delivery with receipt-handle validation.
- Race-safe atomic claim (`BEGIN IMMEDIATE` + status/version-guarded update).
- Visibility-timeout redelivery, retries, and a dead-letter queue with requeue.
- Threaded `WorkerPool` with graceful shutdown modes and a background sweeper.
- Per-queue stats with an in-process TTL cache and structured JSON logging.
- Injectable clock (`SystemClock`/`FakeClock`) for deterministic time tests.
- Teaching-only "unsafe" examples and a reproducible demo suite.
