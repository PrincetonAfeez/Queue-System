"""Round 3 fixes tests."""

from __future__ import annotations

import argparse
import json
from contextlib import closing
from datetime import UTC, datetime, timedelta

import pytest

from simplequeue.cache.stats_cache import _CACHE_BY_KEY
from simplequeue.config import QueueConfig
from simplequeue.defaults import DEFAULT_CACHE_TTL
from simplequeue.storage.migrations import SCHEMA_VERSION, apply_schema_version
from simplequeue.storage.sqlite_backend import SQLiteBackend
from simplequeue.workers.worker import Worker
from simplequeue.workers.worker_pool import WorkerPool


def test_dequeue_rejects_infinite_timedelta_visibility_timeout(queue_factory) -> None:
    class InfiniteDelta(timedelta):
        def total_seconds(self) -> float:
            return float("inf")

    queue = queue_factory("timedelta-inf")
    queue.enqueue({"x": 1})
    with pytest.raises(ValueError, match="visibility_timeout"):
        queue.dequeue(visibility_timeout=InfiniteDelta())


def test_worker_rejects_nan_poll_interval(queue_factory) -> None:
    queue = queue_factory("worker-nan")
    with pytest.raises(ValueError, match="poll_interval"):
        Worker(queue, lambda _d: True, poll_interval=float("nan"))


def test_worker_rejects_nan_join_timeout(queue_factory) -> None:
    queue = queue_factory("worker-join")
    with pytest.raises(ValueError, match="join_timeout"):
        Worker(queue, lambda _d: True, join_timeout=float("nan"))


def test_worker_pool_rejects_nan_poll_interval(queue_factory) -> None:
    queue = queue_factory("pool-nan")
    with pytest.raises(ValueError, match="poll_interval"):
        WorkerPool(queue, lambda _d: True, poll_interval=float("inf"))


def test_queue_auto_stats_cache_respects_cache_ttl(tmp_path) -> None:
    from simplequeue.core.queue import Queue

    _CACHE_BY_KEY.clear()
    db = tmp_path / "ttl.db"
    backend = SQLiteBackend(db)
    custom_ttl = 5.5
    queue = Queue(backend, "jobs", cache_ttl_seconds=custom_ttl)
    key = (str(db.resolve()), custom_ttl, id(queue.clock))
    assert key in _CACHE_BY_KEY
    assert queue.stats_cache._cache.ttl_seconds == custom_ttl  # noqa: SLF001


def test_queue_default_cache_ttl_matches_library_default(tmp_path) -> None:
    from simplequeue.core.queue import Queue

    _CACHE_BY_KEY.clear()
    db = tmp_path / "default-ttl.db"
    queue = Queue(SQLiteBackend(db), "jobs")
    key = (str(db.resolve()), DEFAULT_CACHE_TTL, id(queue.clock))
    assert key in _CACHE_BY_KEY


def test_cli_dlq_all_queues(queue_factory, tmp_path, capsys) -> None:
    from simplequeue.cli.commands import dlq

    db = str(tmp_path / "dlq-all.db")
    config = QueueConfig(database_path=db)
    alpha = queue_factory("alpha", db_name="dlq-all.db")
    beta = queue_factory("beta", db_name="dlq-all.db")
    for queue in (alpha, beta):
        queue.enqueue({"x": 1}, max_attempts=1)
        delivery = queue.dequeue(worker_id="w")
        assert delivery is not None
        queue.nack(delivery.receipt_handle, reason="fail")
    assert dlq.run_list(
        argparse.Namespace(
            db=db,
            config=None,
            queue=None,
            all_queues=True,
        ),
        config,
    ) == 0
    payload = json.loads(capsys.readouterr().out)
    assert len(payload["dead_letters"]) == 2


def test_cli_dlq_rejects_queue_and_all_queues(queue_factory, tmp_path) -> None:
    from simplequeue.cli.commands import dlq

    db = str(tmp_path / "dlq-conflict.db")
    config = QueueConfig(database_path=db)
    queue_factory("jobs", db_name="dlq-conflict.db")
    with pytest.raises(ValueError, match="only one"):
        dlq.run_list(
            argparse.Namespace(
                db=db,
                config=None,
                queue="jobs",
                all_queues=True,
            ),
            config,
        )


def test_cli_purge_all_queues_and_zero_day_cutoff(queue_factory, tmp_path, capsys) -> None:
    from simplequeue.cli.commands import purge
    from simplequeue.scheduling.clock import FakeClock

    db = str(tmp_path / "purge-all.db")
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    config = QueueConfig(database_path=db)
    alpha = queue_factory("alpha", db_name="purge-all.db", clock=clock)
    beta = queue_factory("beta", db_name="purge-all.db", clock=clock)
    for queue in (alpha, beta):
        queue.enqueue({"x": 1})
        delivery = queue.dequeue(worker_id="w")
        assert delivery is not None
        queue.ack(delivery.receipt_handle)
    assert purge.run(
        argparse.Namespace(
            db=db,
            config=None,
            queue=None,
            all_queues=True,
            older_than_days=0,
            older_than=None,
            include_dead_lettered=False,
        ),
        config,
    ) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["removed_total"] == 2
    assert {entry["queue"] for entry in payload["queues"]} == {"alpha", "beta"}


def test_cli_purge_older_than_iso(queue_factory, tmp_path, capsys) -> None:
    from simplequeue.cli.commands import purge
    from simplequeue.scheduling.clock import FakeClock

    db = str(tmp_path / "purge-iso.db")
    clock = FakeClock.starting_at(datetime(2026, 6, 1, tzinfo=UTC))
    config = QueueConfig(database_path=db)
    queue = queue_factory("jobs", db_name="purge-iso.db", clock=clock)
    queue.enqueue({"x": 1})
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    queue.ack(delivery.receipt_handle)
    assert purge.run(
        argparse.Namespace(
            db=db,
            config=None,
            queue="jobs",
            all_queues=False,
            older_than_days=None,
            older_than="2026-06-01T12:00:00+00:00",
            include_dead_lettered=False,
        ),
        config,
    ) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["removed_total"] == 1


def test_apply_schema_version_upgrades_from_zero(tmp_path) -> None:
    backend = SQLiteBackend(tmp_path / "upgrade.db")
    backend.init_schema()
    with closing(backend._connect()) as conn:  # noqa: SLF001
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("UPDATE schema_meta SET value = '0' WHERE key = 'version'")
        conn.execute("DROP TABLE IF EXISTS schema_meta_backup")
        conn.commit()
        conn.execute("BEGIN IMMEDIATE")
        apply_schema_version(conn)
        row = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'version'"
        ).fetchone()
        conn.commit()
    assert int(str(row["value"])) == SCHEMA_VERSION


def test_enqueue_reuses_idempotency_key_after_terminal_ack(queue_factory) -> None:
    queue = queue_factory("idemp-reuse")
    first = queue.enqueue({"x": 1}, idempotency_key="held")
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    queue.ack(delivery.receipt_handle)
    second = queue.enqueue({"x": 1}, idempotency_key="held")
    assert second != first
