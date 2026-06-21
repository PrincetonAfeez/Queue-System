from __future__ import annotations

from datetime import UTC, datetime

import pytest

from simplequeue.config import QueueConfig
from simplequeue.defaults import DEFAULT_CACHE_TTL
from simplequeue.scheduling.scheduler import RepeatingScheduler
from simplequeue.scheduling.sweeper import BackgroundSweeper
from simplequeue.storage.factory import create_queue
from simplequeue.storage.sqlite_backend import SQLiteBackend
from simplequeue.workers.worker import Worker
from simplequeue.workers.worker_pool import WorkerPool


def test_background_sweeper_rejects_nan_interval(queue_factory) -> None:
    queue = queue_factory("sweeper-val")
    with pytest.raises(ValueError, match="interval"):
        BackgroundSweeper(queue, interval=float("nan"))


def test_background_sweeper_rejects_nan_join_timeout(queue_factory) -> None:
    queue = queue_factory("sweeper-join")
    with pytest.raises(ValueError, match="join_timeout"):
        BackgroundSweeper(queue, join_timeout=float("nan"))


def test_repeating_scheduler_rejects_nan_interval() -> None:
    with pytest.raises(ValueError, match="interval"):
        RepeatingScheduler(lambda: None, interval=float("inf"))


def test_worker_join_rejects_nan_timeout(queue_factory) -> None:
    queue = queue_factory("join-val")
    worker = Worker(queue, lambda _d: True, poll_interval=0.05, visibility_timeout=30)
    worker.start()
    worker.stop()
    with pytest.raises(ValueError, match="join_timeout"):
        worker.join(float("nan"))
    worker.join(2)


def test_worker_pool_join_rejects_nan_timeout(queue_factory) -> None:
    queue = queue_factory("pool-join")
    pool = WorkerPool(queue, lambda _d: True, workers=1, poll_interval=0.05, visibility_timeout=30)
    pool.start()
    pool.stop()
    with pytest.raises(ValueError, match="join_timeout"):
        pool.join(float("nan"))
    pool.join(2)


def test_create_queue_wires_cache_ttl(tmp_path) -> None:
    from simplequeue.cache.stats_cache import _CACHE_BY_KEY

    _CACHE_BY_KEY.clear()
    db = str(tmp_path / "factory.db")
    config = QueueConfig(database_path=db, cache_ttl=4.25)
    queue = create_queue(config, "jobs")
    key = (str((tmp_path / "factory.db").resolve()), 4.25, id(queue.clock))
    assert key in _CACHE_BY_KEY
    assert queue.stats_cache._cache.ttl_seconds == 4.25  # noqa: SLF001


def test_create_queue_default_cache_ttl(tmp_path) -> None:
    from simplequeue.cache.stats_cache import _CACHE_BY_KEY

    _CACHE_BY_KEY.clear()
    db = str(tmp_path / "factory-default.db")
    queue = create_queue(QueueConfig(database_path=db), "jobs")
    key = (str((tmp_path / "factory-default.db").resolve()), DEFAULT_CACHE_TTL, id(queue.clock))
    assert key in _CACHE_BY_KEY


def test_purge_terminal_all_queues(queue_factory) -> None:
    from simplequeue.scheduling.clock import FakeClock

    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    alpha = queue_factory("alpha", clock=clock, db_name="purge-all.db")
    beta = queue_factory("beta", db_name="purge-all.db", clock=clock)
    for queue in (alpha, beta):
        queue.enqueue({"x": 1})
        delivery = queue.dequeue(worker_id="w")
        assert delivery is not None
        queue.ack(delivery.receipt_handle)
    removed = alpha.purge_terminal(older_than=clock.now(), all_queues=True)
    assert removed == 2


def test_purge_terminal_rejects_all_queues_and_queue_name(queue_factory) -> None:
    queue = queue_factory("conflict")
    with pytest.raises(ValueError, match="only one"):
        queue.purge_terminal(queue_name="other", all_queues=True)


def test_sqlite_backend_rejects_invalid_queue_name(tmp_path) -> None:
    backend = SQLiteBackend(tmp_path / "guard.db")
    backend.init_schema()
    with pytest.raises(ValueError, match="queue_name"):
        backend.enqueue("  ", {"x": 1})


def test_sqlite_backend_nack_unknown_receipt(tmp_path) -> None:
    backend = SQLiteBackend(tmp_path / "nack-miss.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    result = backend.nack("missing-handle", now, reason="fail")
    assert result.success is False
    assert result.reason == "receipt_handle_not_found"


def test_sqlite_backend_claim_next_rejects_blank_queue_name(tmp_path) -> None:
    from datetime import timedelta

    from simplequeue.core.modes import DeliveryMode

    backend = SQLiteBackend(tmp_path / "claim-guard.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    with pytest.raises(ValueError, match="queue_name"):
        backend.claim_next(" ", DeliveryMode.AT_LEAST_ONCE, timedelta(seconds=30), "w", now)


def test_sqlite_backend_ack_unknown_receipt(tmp_path) -> None:
    backend = SQLiteBackend(tmp_path / "ack-miss.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    result = backend.ack("missing-handle", now)
    assert result.success is False
    assert result.reason == "receipt_handle_not_found"
