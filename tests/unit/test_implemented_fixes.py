from __future__ import annotations

from datetime import UTC, datetime

import pytest

from simplequeue.config import QueueConfig, _validate_ranges, load_config
from simplequeue.storage.factory import create_backend
from simplequeue.storage.migrations import SCHEMA_VERSION
from simplequeue.storage.sqlite_backend import SQLiteBackend
from simplequeue.workers.shutdown import ShutdownMode
from simplequeue.workers.worker import Worker


def test_config_rejects_nan_visibility_timeout() -> None:
    with pytest.raises(ValueError, match="finite"):
        _validate_ranges(QueueConfig(visibility_timeout=float("nan")), command="consume")


def test_config_rejects_inf_visibility_timeout() -> None:
    with pytest.raises(ValueError, match="finite"):
        _validate_ranges(QueueConfig(visibility_timeout=float("inf")), command="consume")


def test_dequeue_rejects_nan_visibility_timeout(queue_factory) -> None:
    queue = queue_factory("nan-vt")
    queue.enqueue({"x": 1})
    with pytest.raises(ValueError, match="finite"):
        queue.dequeue(visibility_timeout=float("nan"))


def test_queue_rejects_empty_queue_name(queue_factory) -> None:
    with pytest.raises(ValueError, match="queue_name"):
        queue_factory("")


def test_list_dead_letters_all_queues(queue_factory) -> None:
    queue_a = queue_factory("alpha", db_name="dlq-all.db")
    queue_b = queue_factory("beta", db_name="dlq-all.db")
    queue_a.enqueue({"x": 1}, max_attempts=1)
    delivery = queue_a.dequeue(worker_id="w")
    assert delivery is not None
    queue_a.nack(delivery.receipt_handle, reason="fail")
    queue_b.enqueue({"y": 1}, max_attempts=1)
    delivery_b = queue_b.dequeue(worker_id="w2")
    assert delivery_b is not None
    queue_b.nack(delivery_b.receipt_handle, reason="fail")
    assert len(queue_a.list_dead_letters()) == 1
    assert len(queue_a.list_dead_letters(all_queues=True)) == 2


def test_dequeue_releases_expired_leases_on_other_queues(queue_factory) -> None:
    from simplequeue.scheduling.clock import FakeClock

    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue_a = queue_factory("alpha", db_name="cross-lease.db", clock=clock)
    queue_b = queue_factory("beta", db_name="cross-lease.db", clock=clock)
    queue_b.enqueue({"x": 1})
    delivery = queue_b.dequeue(visibility_timeout=5, worker_id="w")
    assert delivery is not None
    clock.advance(6)
    assert queue_a.dequeue(worker_id="w2") is None
    redelivered = queue_b.dequeue(worker_id="w3")
    assert redelivered is not None
    assert redelivered.message_id == delivery.message_id


def test_purge_terminal_removes_old_acked_messages(queue_factory) -> None:
    from simplequeue.scheduling.clock import FakeClock

    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("purge", clock=clock)
    queue.enqueue({"x": 1})
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    queue.ack(delivery.receipt_handle)
    clock.advance(3600)
    removed = queue.purge_terminal(older_than=clock.now())
    assert removed == 1
    assert queue.inspect(delivery.message_id) is None


def test_create_backend_sqlite(tmp_path) -> None:
    config = QueueConfig(database_path=str(tmp_path / "q.db"), max_attempts=5)
    backend = create_backend(config)
    assert isinstance(backend, SQLiteBackend)
    assert backend.default_max_attempts == 5


def test_create_backend_zero_max_attempts_uses_library_default() -> None:
    backend = create_backend(QueueConfig(max_attempts=0))
    assert backend.default_max_attempts == 3


def test_create_backend_unknown_raises() -> None:
    config = QueueConfig(backend="postgres")
    with pytest.raises(ValueError, match="unsupported backend"):
        create_backend(config)


def test_init_schema_records_schema_version(tmp_path) -> None:
    from contextlib import closing

    backend = SQLiteBackend(tmp_path / "ver.db")
    backend.init_schema()
    with closing(backend._connect()) as conn:  # noqa: SLF001
        row = conn.execute(
            "SELECT value FROM schema_meta WHERE key = 'version'"
        ).fetchone()
    assert row is not None
    assert int(row["value"]) == SCHEMA_VERSION


def test_load_config_init_db_allows_max_attempts_zero(tmp_path) -> None:
    path = tmp_path / "init.json"
    path.write_text('{"max_attempts": 0}', encoding="utf-8")
    config = load_config(str(path), command="init-db")
    assert config.max_attempts == 0


def test_shutdown_finish_current_runs_processor_after_stop_before_handler(queue_factory) -> None:
    queue = queue_factory("finish-before")
    queue.enqueue({"x": 1})
    claimed: list[str] = []

    def processor(delivery) -> bool:
        claimed.append("ran")
        return True

    worker = Worker(
        queue,
        processor,
        shutdown_mode=ShutdownMode.FINISH_CURRENT,
        visibility_timeout=30,
        poll_interval=0.01,
    )
    delivery = queue.dequeue(worker_id="manual")
    assert delivery is not None
    worker.stop()
    worker._finish(delivery, processor(delivery))  # noqa: SLF001
    assert claimed == ["ran"]
    assert queue.stats(cached=False).acked == 1
