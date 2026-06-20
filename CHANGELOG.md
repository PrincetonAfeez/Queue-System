# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html). While the
version is `0.x`, the public API may change between releases.

## [Unreleased]

### Added
- `simplequeue purge` CLI for terminal row maintenance (`--older-than-days`, `--include-dead-lettered`).
- `Queue.purge_terminal(include_dead_lettered=True)` and 7-day default retention when `older_than` is omitted.
- `create_backend` exported from top-level `simplequeue`; schema errors raise `StorageError`.
- `docs/migrations.md` for schema upgrade policy and idempotency retry notes.
- `tests/unit/test_round2_fixes.py` for Round 2 regression coverage.

### Changed
- `create_backend()` ignores invalid `max_attempts` in config (uses library default 3).
- `list_dead_letters(all_queues=True, queue_name=...)` raises `ValueError`.
- `shared_stats_cache()` keyed by `(path, ttl, id(clock))`.
- Config validates `queue_name`; `cache_ttl` / `max_attempts` scoped by command.
- Consume warning clarifies lease reclaim requires active dequeuers without `--sweeper`.
- README coverage figure updated to ~96%.

### Fixed
- `init-db` with `max_attempts = 0` in config no longer fails at backend construction.
- `purge_terminal()` no longer deletes all terminal rows when `older_than` is omitted.
- Unclosed SQLite connection in schema version test (ResourceWarning).

## [Unreleased — Round 1]

### Added
- `Queue.purge_terminal()` to delete old `acked`/`deleted` rows and cap database growth.
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
