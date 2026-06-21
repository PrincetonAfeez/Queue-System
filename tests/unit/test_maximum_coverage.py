from __future__ import annotations

import argparse
import sqlite3
import threading
import time
from contextlib import closing
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest

from simplequeue.cli.commands import consume, produce, purge
from simplequeue.config import QueueConfig, validate_library_config
from simplequeue.core.modes import DeliveryMode
from simplequeue.core.states import MessageStatus
from simplequeue.scheduling.clock import Clock, FakeClock, SystemClock
from simplequeue.storage import sqlite_backend as sb
from simplequeue.storage.factory import create_backend
from simplequeue.storage.migrations import SCHEMA_VERSION, apply_schema_version
from simplequeue.storage.sqlite_backend import SQLiteBackend
from simplequeue.workers.claim_budget import ClaimBudget
from simplequeue.workers.shutdown import ShutdownMode
from simplequeue.workers.worker import Worker


class _OverflowTimedelta(timedelta):
    def total_seconds(self) -> float:
        raise OverflowError("too large")


def _consume_args(tmp_path, **overrides: Any) -> argparse.Namespace:
    base = dict(
        limit=None,
        duration=None,
        fail_every=0,
        process_time=0.0,
        db=str(tmp_path / "consume.db"),
        config=None,
        queue="jobs",
        mode=None,
        workers=1,
        visibility_timeout=30.0,
        idle_timeout=0.15,
        poll_interval=0.05,
        sweeper=False,
        sweeper_interval=1.0,
        shutdown_mode="finish-current",
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def test_clock_base_sleep_and_fake_monotonic() -> None:
    clock = Clock()
    clock.sleep(0.001)
    fake = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    assert fake.monotonic() == 0.0


def test_dequeue_rejects_timedelta_overflow(queue_factory) -> None:
    queue = queue_factory("overflow")
    queue.enqueue({"x": 1})
    with pytest.raises(ValueError, match="visibility_timeout"):
        queue.dequeue(visibility_timeout=_OverflowTimedelta(days=1))


def test_validate_library_config_rejects_zero_workers() -> None:
    with pytest.raises(ValueError, match="worker_count"):
        validate_library_config(QueueConfig(worker_count=0))


def test_create_backend_rejects_unknown_backend() -> None:
    with pytest.raises(ValueError, match="unsupported backend"):
        create_backend(QueueConfig(backend="redis"))


def test_apply_schema_version_rejects_newer_database(tmp_path) -> None:
    backend = SQLiteBackend(tmp_path / "newer-schema.db")
    backend.init_schema()
    with closing(backend._connect()) as conn:  # noqa: SLF001
        conn.execute(
            "UPDATE schema_meta SET value = ? WHERE key = 'version'",
            (str(SCHEMA_VERSION + 1),),
        )
        conn.commit()
        with pytest.raises(Exception, match="newer than library"):
            apply_schema_version(conn)


def test_apply_schema_version_upgrades_missing_meta_table(tmp_path) -> None:
    backend = SQLiteBackend(tmp_path / "upgrade.db")
    with closing(backend._connect()) as conn:  # noqa: SLF001
        conn.executescript(
            """
            CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO schema_meta (key, value) VALUES ('version', '0');
            """
        )
        apply_schema_version(conn)
        row = conn.execute("SELECT value FROM schema_meta WHERE key = 'version'").fetchone()
        assert int(row["value"]) == SCHEMA_VERSION


def test_consume_parse_helpers() -> None:
    assert consume._parse_shutdown_mode("nack-current") is ShutdownMode.NACK_CURRENT
    assert consume._parse_delivery_mode("at-most-once") is DeliveryMode.AT_MOST_ONCE


def test_consume_warns_on_fail_every_in_at_most_once_mode(tmp_path, capsys) -> None:
    config = QueueConfig(database_path=str(tmp_path / "warn.db"))
    args = _consume_args(tmp_path, mode=DeliveryMode.AT_MOST_ONCE, fail_every=2, duration=0.01)
    consume.run(args, config)
    assert "fail-every has no effect" in capsys.readouterr().err


def test_consume_warns_without_sweeper_for_unbounded_at_least_once(tmp_path, capsys, monkeypatch) -> None:
    config = QueueConfig(database_path=str(tmp_path / "warn2.db"), poll_interval=0.01)

    class _BreakLoop(Exception):
        pass

    monkeypatch.setattr(consume.time, "sleep", lambda _secs: (_ for _ in ()).throw(_BreakLoop()))
    args = _consume_args(tmp_path, mode=DeliveryMode.AT_LEAST_ONCE, duration=None, limit=None)
    with pytest.raises(_BreakLoop):
        consume.run(args, config)
    assert "without --sweeper" in capsys.readouterr().err


def test_consume_idle_drain_exits_on_empty_bounded_run(tmp_path, capsys) -> None:
    config = QueueConfig(
        database_path=str(tmp_path / "idle.db"),
        poll_interval=0.05,
        idle_timeout=0.1,
    )
    from simplequeue.cli.commands import init_db

    init_db.run(argparse.Namespace(db=config.database_path, config=None, queue=None), config)
    args = _consume_args(tmp_path, duration=2.0, idle_timeout=0.1)
    assert consume.run(args, config) == 0
    assert "consume_finished" in capsys.readouterr().out


def test_consume_process_time_respects_stop_processing(tmp_path, capsys, monkeypatch) -> None:
    db = str(tmp_path / "stop-process.db")
    config = QueueConfig(database_path=db, poll_interval=0.05, idle_timeout=0.2)
    from simplequeue.cli.commands import init_db

    init_db.run(argparse.Namespace(db=db, config=None, queue=None), config)
    produce.run(
        argparse.Namespace(
            db=db,
            config=None,
            queue="jobs",
            payload='{"x":1}',
            payload_template=None,
            count=1,
            idempotency_key=None,
            idempotent=False,
            max_attempts=None,
        ),
        config,
    )
    capsys.readouterr()

    real_sleep = time.sleep

    def fake_sleep(seconds: float) -> None:
        real_sleep(min(seconds, 0.01))

    monkeypatch.setattr(consume.time, "sleep", fake_sleep)
    args = _consume_args(tmp_path, db=db, limit=1, process_time=0.05)
    assert consume.run(args, config) == 0


def test_produce_suffixes_idempotency_key_for_multi_count(tmp_path, capsys) -> None:
    db = str(tmp_path / "produce-key.db")
    config = QueueConfig(database_path=db)
    from simplequeue.cli.commands import init_db

    init_db.run(argparse.Namespace(db=db, config=None, queue=None), config)
    capsys.readouterr()
    args = argparse.Namespace(
        db=db,
        config=None,
        queue="jobs",
        payload='{"x":1}',
        payload_template=None,
        count=2,
        idempotency_key="batch",
        idempotent=False,
        max_attempts=None,
    )
    assert produce.run(args, config) == 0
    payload = __import__("json").loads(capsys.readouterr().out)
    assert payload["count"] == 2
    assert payload["message_ids"] == [1, 2]


def test_purge_rejects_conflicting_cutoff_flags(tmp_path) -> None:
    config = QueueConfig(database_path=str(tmp_path / "purge.db"))
    args = argparse.Namespace(
        db=str(tmp_path / "purge.db"),
        config=None,
        queue="jobs",
        all_queues=False,
        older_than="2026-01-01T00:00:00+00:00",
        older_than_days=1.0,
        include_dead_lettered=False,
    )
    with pytest.raises(ValueError, match="only one"):
        purge.run(args, config)


def test_purge_rejects_queue_and_all_queues(tmp_path) -> None:
    config = QueueConfig(database_path=str(tmp_path / "purge2.db"))
    args = argparse.Namespace(
        db=str(tmp_path / "purge2.db"),
        config=None,
        queue="jobs",
        all_queues=True,
        older_than=None,
        older_than_days=None,
        include_dead_lettered=False,
    )
    with pytest.raises(ValueError, match="only one"):
        purge.run(args, config)


def test_purge_older_than_without_timezone(tmp_path, capsys) -> None:
    db = str(tmp_path / "purge3.db")
    config = QueueConfig(database_path=db)
    from simplequeue.cli.commands import init_db

    init_db.run(argparse.Namespace(db=db, config=None, queue=None), config)
    args = argparse.Namespace(
        db=db,
        config=None,
        queue="jobs",
        all_queues=False,
        older_than="2026-01-01T00:00:00",
        older_than_days=None,
        include_dead_lettered=False,
    )
    assert purge.run(args, config) == 0
    assert "removed_total" in capsys.readouterr().out


def test_purge_older_than_days_zero_uses_now(tmp_path, capsys) -> None:
    db = str(tmp_path / "purge4.db")
    config = QueueConfig(database_path=db)
    from simplequeue.cli.commands import init_db

    init_db.run(argparse.Namespace(db=db, config=None, queue=None), config)
    args = argparse.Namespace(
        db=db,
        config=None,
        queue="jobs",
        all_queues=False,
        older_than=None,
        older_than_days=0.0,
        include_dead_lettered=False,
    )
    assert purge.run(args, config) == 0


def test_main_storage_error_and_queue_error_exit_codes(tmp_path, monkeypatch) -> None:
    from simplequeue.cli.main import main
    from simplequeue.core.exceptions import QueueError, StorageError

    def storage_boom(*args: object, **kwargs: object) -> int:
        raise StorageError("disk full")

    def queue_boom(*args: object, **kwargs: object) -> int:
        raise QueueError("bad requeue")

    monkeypatch.setattr("simplequeue.cli.commands.init_db.run", storage_boom)
    assert main(["init-db", "--db", str(tmp_path / "a.db")]) == 1

    monkeypatch.setattr("simplequeue.cli.commands.init_db.run", queue_boom)
    assert main(["init-db", "--db", str(tmp_path / "b.db")]) == 4


def test_main_no_command_prints_help() -> None:
    from simplequeue.cli.main import main

    assert main([]) == 2


def test_main_merged_config_applies_numeric_overrides(tmp_path) -> None:
    from simplequeue.cli.main import _merged_config, build_parser

    parser = build_parser()
    args = parser.parse_args(
        [
            "consume",
            "--db",
            str(tmp_path / "cfg.db"),
            "--queue",
            "jobs",
            "--workers",
            "2",
            "--visibility-timeout",
            "12",
        ]
    )
    config = _merged_config(args)
    assert config.worker_count == 2
    assert config.visibility_timeout == 12.0


def test_worker_claim_budget_release_unused_on_empty_queue(queue_factory) -> None:
    queue = queue_factory("budget-empty")
    budget = ClaimBudget(3)
    worker = Worker(
        queue,
        lambda _d: True,
        claim_budget=budget,
        poll_interval=0.01,
        visibility_timeout=30,
    )
    worker.start()
    time.sleep(0.05)
    worker.stop()
    worker.join(2)
    assert budget.try_acquire() is True
    assert budget.try_acquire() is True
    assert budget.try_acquire() is True


def test_worker_abandon_current_interrupts_before_processor(queue_factory) -> None:
    queue = queue_factory("abandon-before")
    queue.enqueue({"x": 1})
    started = threading.Event()
    release = threading.Event()

    def processor(_delivery) -> bool:
        started.set()
        release.wait(timeout=2)
        return True

    worker = Worker(
        queue,
        processor,
        shutdown_mode=ShutdownMode.ABANDON_CURRENT,
        poll_interval=0.01,
        visibility_timeout=30,
    )
    worker.start()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and not started.is_set():
        time.sleep(0.01)
    worker.stop()
    worker.join(2)
    assert queue.stats(cached=False).in_flight_count == 1
    release.set()


def test_worker_nack_current_on_processor_exception_during_shutdown(queue_factory) -> None:
    queue = queue_factory("nack-exc-shutdown")
    queue.enqueue({"x": 1})
    started = threading.Event()
    release = threading.Event()

    def processor(_delivery) -> bool:
        started.set()
        release.wait(timeout=2)
        raise RuntimeError("boom")

    worker = Worker(
        queue,
        processor,
        shutdown_mode=ShutdownMode.NACK_CURRENT,
        poll_interval=0.01,
        visibility_timeout=30,
    )
    worker.start()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and not started.is_set():
        time.sleep(0.01)
    worker.stop()
    release.set()
    worker.join(2)
    again = queue.dequeue(worker_id="after")
    assert again is not None


def test_worker_handle_interrupted_at_most_once_is_noop(queue_factory) -> None:
    from simplequeue.core.delivery import Delivery

    queue = queue_factory("amo-interrupt")
    worker = Worker(queue, lambda _d: True, delivery_mode=DeliveryMode.AT_MOST_ONCE)
    delivery = Delivery(
        message_id=1,
        receipt_handle="rh",
        queue_name="amo-interrupt",
        payload={},
        attempt=1,
        delivery_mode=DeliveryMode.AT_MOST_ONCE,
        leased_at=datetime(2026, 1, 1, tzinfo=UTC),
        lease_expires_at=None,
    )
    worker._handle_interrupted_delivery(delivery)  # noqa: SLF001


def test_concurrent_claim_at_least_once_retries_after_lost_race(queue_factory) -> None:
    queue = queue_factory("claim-race")
    queue.enqueue({"x": 1})
    results: list = []
    barrier = threading.Barrier(2)

    def contender() -> None:
        barrier.wait()
        results.append(queue.dequeue(worker_id=threading.current_thread().name))

    threads = [threading.Thread(target=contender, name=f"w{i}") for i in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    deliveries = [item for item in results if item is not None]
    assert len(deliveries) == 1


def test_concurrent_claim_at_most_once_only_one_delivery(queue_factory) -> None:
    queue = queue_factory("amo-race")
    queue.enqueue({"x": 1})
    results: list = []
    barrier = threading.Barrier(2)

    def contender() -> None:
        barrier.wait()
        results.append(
            queue.dequeue(
                delivery_mode=DeliveryMode.AT_MOST_ONCE,
                worker_id=threading.current_thread().name,
            )
        )

    threads = [threading.Thread(target=contender, name=f"w{i}") for i in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    deliveries = [item for item in results if item is not None]
    assert len(deliveries) == 1
    assert queue.stats(cached=False).delivered == 1


def test_ack_stale_receipt_when_lease_raced_to_nack(queue_factory) -> None:
    queue = queue_factory("ack-nack-race")
    queue.enqueue({"x": 1})
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    results: list = []
    barrier = threading.Barrier(2)

    def acker() -> None:
        barrier.wait()
        results.append(queue.ack(delivery.receipt_handle))

    def nacker() -> None:
        barrier.wait()
        results.append(queue.nack(delivery.receipt_handle, reason="race"))

    threads = [threading.Thread(target=acker), threading.Thread(target=nacker)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sum(1 for result in results if result.success) == 1
    failure = next(result for result in results if not result.success)
    assert failure.reason in ("stale_receipt", "not_leased", "receipt_handle_not_found")


def test_nack_stale_receipt_on_concurrent_nack(queue_factory) -> None:
    queue = queue_factory("nack-race")
    queue.enqueue({"x": 1})
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    results: list = []
    barrier = threading.Barrier(2)

    def contender() -> None:
        barrier.wait()
        results.append(queue.nack(delivery.receipt_handle, reason="race"))

    threads = [threading.Thread(target=contender) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sum(1 for result in results if result.success) == 1
    failure = next(result for result in results if not result.success)
    assert failure.reason in ("stale_receipt", "not_leased", "receipt_handle_not_found")


def test_backend_ack_rolls_back_on_record_event_failure(tmp_path, monkeypatch) -> None:
    backend = SQLiteBackend(tmp_path / "ack-rollback.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    backend.enqueue("q", {"x": 1}, now=now)
    claim = backend.claim_next("q", DeliveryMode.AT_LEAST_ONCE, timedelta(seconds=30), "w", now)
    assert claim.delivery is not None

    def boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("event write failed")

    monkeypatch.setattr(backend, "_record_event", boom)
    with pytest.raises(RuntimeError, match="event write failed"):
        backend.ack(claim.delivery.receipt_handle, now)
    details = backend.inspect(claim.delivery.message_id)
    assert details is not None
    assert details.message.status is MessageStatus.LEASED


def test_backend_nack_rolls_back_on_record_event_failure(tmp_path, monkeypatch) -> None:
    backend = SQLiteBackend(tmp_path / "nack-rollback.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    backend.enqueue("q", {"x": 1}, now=now)
    claim = backend.claim_next("q", DeliveryMode.AT_LEAST_ONCE, timedelta(seconds=30), "w", now)
    assert claim.delivery is not None

    def boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("event write failed")

    monkeypatch.setattr(backend, "_record_event", boom)
    with pytest.raises(RuntimeError, match="event write failed"):
        backend.nack(claim.delivery.receipt_handle, now, reason="fail")
    details = backend.inspect(claim.delivery.message_id)
    assert details is not None
    assert details.message.status is MessageStatus.LEASED


def test_backend_release_expired_leases_rolls_back_on_failure(tmp_path, monkeypatch) -> None:
    backend = SQLiteBackend(tmp_path / "release-rollback.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    expired = now - timedelta(seconds=30)
    backend.enqueue("q", {"x": 1}, now=now)
    claim = backend.claim_next("q", DeliveryMode.AT_LEAST_ONCE, timedelta(seconds=5), "w", now)
    assert claim.delivery is not None
    with closing(backend._connect()) as conn:  # noqa: SLF001
        conn.execute(
            "UPDATE messages SET lease_expires_at = ? WHERE id = ?",
            (sb._dt(expired), claim.delivery.message_id),
        )
        conn.commit()

    def boom(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("release failed")

    monkeypatch.setattr(backend, "_release_expired_leases_locked", boom)
    with pytest.raises(RuntimeError, match="release failed"):
        backend.release_expired_leases(now)


def test_backend_move_exhausted_rolls_back_on_failure(tmp_path, monkeypatch) -> None:
    backend = SQLiteBackend(tmp_path / "dlq-rollback.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    message_id = backend.enqueue("q", {"x": 1}, max_attempts=1, now=now)
    with closing(backend._connect()) as conn:  # noqa: SLF001
        conn.execute("UPDATE messages SET attempts = 1 WHERE id = ?", (message_id,))
        conn.commit()

    def boom(*args: Any, **kwargs: Any) -> bool:
        raise RuntimeError("dlq failed")

    monkeypatch.setattr(backend, "_move_to_dlq_locked", boom)
    with pytest.raises(RuntimeError, match="dlq failed"):
        backend.move_exhausted_to_dlq(now)


def test_backend_purge_rolls_back_on_failure(tmp_path, monkeypatch) -> None:
    backend = SQLiteBackend(tmp_path / "purge-rollback.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    message_id = backend.enqueue("q", {"x": 1}, now=now)
    claim = backend.claim_next("q", DeliveryMode.AT_LEAST_ONCE, timedelta(seconds=30), "w", now)
    assert claim.delivery is not None
    backend.ack(claim.delivery.receipt_handle, now)

    def boom(*args: Any, **kwargs: Any) -> bool:
        raise RuntimeError("purge failed")

    monkeypatch.setattr(backend, "_purge_message_locked", boom)
    with pytest.raises(RuntimeError, match="purge failed"):
        backend.purge_terminal_messages("q", now, include_dead_lettered=False)
    assert backend.inspect(message_id) is not None


def test_backend_move_to_dlq_locked_returns_false_on_race(tmp_path) -> None:
    backend = SQLiteBackend(tmp_path / "dlq-race.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    backend.enqueue("q", {"x": 1}, max_attempts=1, now=now)
    claim = backend.claim_next("q", DeliveryMode.AT_LEAST_ONCE, timedelta(seconds=30), "w", now)
    assert claim.delivery is not None
    with closing(backend._connect()) as conn:  # noqa: SLF001
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT * FROM messages WHERE id = ?", (claim.delivery.message_id,)).fetchone()
        conn.execute(
            "UPDATE messages SET status = ? WHERE id = ?",
            (MessageStatus.ACKED.value, claim.delivery.message_id),
        )
        assert backend._move_to_dlq_locked(conn, row, "race", now) is False  # noqa: SLF001
        conn.rollback()


def test_backend_enqueue_without_idempotency_reraises_integrity_error(tmp_path, monkeypatch) -> None:
    backend = SQLiteBackend(tmp_path / "integrity.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    real_connect = backend._connect  # noqa: SLF001

    class _ConnectionProxy:
        def __init__(self, conn: sqlite3.Connection) -> None:
            self._conn = conn

        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Cursor:
            if "INSERT INTO messages" in sql:
                raise sqlite3.IntegrityError("forced")
            return self._conn.execute(sql, params)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._conn, name)

    monkeypatch.setattr(backend, "_connect", lambda: _ConnectionProxy(real_connect()))
    with pytest.raises(Exception, match="forced"):
        backend.enqueue("q", {"x": 1}, now=now)


def test_backend_enqueue_missing_lastrowid_raises_runtime_error(tmp_path, monkeypatch) -> None:
    backend = SQLiteBackend(tmp_path / "lastrowid.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    real_connect = backend._connect  # noqa: SLF001

    class _CursorProxy:
        def __init__(self, cursor: sqlite3.Cursor) -> None:
            self._cursor = cursor
            self.lastrowid = None
            self.rowcount = 1

        def __getattr__(self, name: str) -> Any:
            return getattr(self._cursor, name)

    class _ConnectionProxy:
        def __init__(self, conn: sqlite3.Connection) -> None:
            self._conn = conn

        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Cursor:
            if "INSERT INTO messages" in sql:
                return _CursorProxy(self._conn.execute(sql, params))
            return self._conn.execute(sql, params)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._conn, name)

    monkeypatch.setattr(backend, "_connect", lambda: _ConnectionProxy(real_connect()))
    with pytest.raises(RuntimeError, match="did not return a message id"):
        backend.enqueue("q", {"x": 1}, now=now)


def test_nack_expired_lease_stale_when_update_loses_race(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("nack-expired-stale-race", clock=clock)
    queue.enqueue({"x": 1})
    delivery = queue.dequeue(visibility_timeout=5, worker_id="w")
    assert delivery is not None
    clock.advance(6)
    backend = queue.backend
    assert isinstance(backend, SQLiteBackend)
    with closing(backend._connect()) as conn:  # noqa: SLF001
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM messages WHERE id = ?",
            (delivery.message_id,),
        ).fetchone()
        conn.execute(
            "UPDATE messages SET status = ? WHERE id = ?",
            (MessageStatus.AVAILABLE.value, delivery.message_id),
        )
        result = backend._nack_expired_lease_locked(conn, row, clock.now(), "late")  # noqa: SLF001
        conn.rollback()
    assert not result.success
    assert result.reason == "stale_receipt"


def test_system_clock_monotonic_increases() -> None:
    clock = SystemClock()
    first = clock.monotonic()
    time.sleep(0.01)
    assert clock.monotonic() >= first


def test_main_merged_config_applies_max_attempts_override(tmp_path) -> None:
    from simplequeue.cli.main import _merged_config, build_parser

    parser = build_parser()
    args = parser.parse_args(
        [
            "produce",
            "--db",
            str(tmp_path / "cfg.db"),
            "--queue",
            "jobs",
            "--payload",
            '{"x":1}',
            "--max-attempts",
            "5",
        ]
    )
    config = _merged_config(args)
    assert config.max_attempts == 5


def test_consume_fail_every_nacks_on_alternating_messages(tmp_path, capsys) -> None:
    db = str(tmp_path / "fail-every.db")
    config = QueueConfig(database_path=db, poll_interval=0.05, idle_timeout=0.2)
    from simplequeue.cli.commands import init_db

    init_db.run(argparse.Namespace(db=db, config=None, queue=None), config)
    produce.run(
        argparse.Namespace(
            db=db,
            config=None,
            queue="jobs",
            payload='{"x":1}',
            payload_template=None,
            count=2,
            idempotency_key=None,
            idempotent=False,
            max_attempts=None,
        ),
        config,
    )
    capsys.readouterr()
    args = _consume_args(
        tmp_path,
        db=db,
        limit=2,
        fail_every=2,
        mode=DeliveryMode.AT_LEAST_ONCE,
        sweeper=True,
    )
    assert consume.run(args, config) == 0
    output = capsys.readouterr().out
    assert output.count('"event": "processed"') == 2


def test_consume_processor_exits_early_when_stop_processing_set(tmp_path, monkeypatch) -> None:
    db = str(tmp_path / "stop-proc.db")
    config = QueueConfig(database_path=db, poll_interval=0.05, idle_timeout=0.2)
    from simplequeue.cli.commands import init_db

    init_db.run(argparse.Namespace(db=db, config=None, queue=None), config)

    stop_events: list[threading.Event] = []
    original_event = threading.Event

    class _CapturingEvent(original_event):
        def __init__(self) -> None:
            super().__init__()
            stop_events.append(self)

    class _BreakLoop(Exception):
        pass

    class _Pool:
        def __init__(self, _queue, processor, **kwargs) -> None:
            self._processor = processor

        def start(self) -> None:
            stop_events[0].set()
            delivery = MagicMock(message_id=1, queue_name="jobs", attempt=1)
            delivery.delivery_mode = DeliveryMode.AT_LEAST_ONCE
            delivery.receipt_handle = "rh" * 8
            delivery.payload = {"x": 1}
            assert self._processor(delivery) is False

        def stop(self) -> None:
            return None

        def join(self, _timeout: float) -> bool:
            return True

    monkeypatch.setattr(consume.threading, "Event", _CapturingEvent)
    monkeypatch.setattr(consume, "WorkerPool", _Pool)
    monkeypatch.setattr(consume.time, "sleep", lambda _secs: (_ for _ in ()).throw(_BreakLoop()))
    args = _consume_args(tmp_path, db=db, limit=1, process_time=1.0)
    with pytest.raises(_BreakLoop):
        consume.run(args, config)


def test_worker_stop_after_claim_before_processor_abandons_delivery(queue_factory) -> None:
    queue = queue_factory("stop-after-claim")
    queue.enqueue({"x": 1})
    claimed: list = []

    def processor(delivery) -> bool:
        claimed.append(delivery)
        return True

    worker = Worker(
        queue,
        processor,
        shutdown_mode=ShutdownMode.ABANDON_CURRENT,
        poll_interval=0.01,
        visibility_timeout=30,
    )
    original_dequeue = queue.dequeue

    def dequeue_and_stop(**kwargs):
        delivery = original_dequeue(**kwargs)
        if delivery is not None:
            worker.stop()
        return delivery

    queue.dequeue = dequeue_and_stop  # type: ignore[method-assign]
    worker.start()
    worker.join(2)
    assert claimed == []
    assert queue.stats(cached=False).in_flight_count == 1


def test_backend_claim_at_least_once_retries_after_lost_version_race(tmp_path, monkeypatch) -> None:
    backend = SQLiteBackend(tmp_path / "claim-retry.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    backend.enqueue("q", {"x": 1}, now=now)
    real_connect = backend._connect  # noqa: SLF001
    bumped = False

    class _ConnectionProxy:
        def __init__(self, conn: sqlite3.Connection) -> None:
            self._conn = conn

        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Cursor:
            nonlocal bumped
            if (
                not bumped
                and "SET status = ?" in sql
                and "lease_expires_at = ?" in sql
                and MessageStatus.LEASED.value in params
            ):
                bumped = True
                self._conn.execute(
                    "UPDATE messages SET version = version + 1 WHERE id = ?",
                    (params[-3],),
                )
            return self._conn.execute(sql, params)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._conn, name)

    monkeypatch.setattr(backend, "_connect", lambda: _ConnectionProxy(real_connect()))
    claim = backend.claim_next("q", DeliveryMode.AT_LEAST_ONCE, timedelta(seconds=30), "w", now)
    assert claim.delivery is not None


def test_backend_claim_at_most_once_retries_after_lost_version_race(tmp_path, monkeypatch) -> None:
    backend = SQLiteBackend(tmp_path / "amo-retry.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    backend.enqueue("q", {"x": 1}, now=now)
    real_connect = backend._connect  # noqa: SLF001
    bumped = False

    class _ConnectionProxy:
        def __init__(self, conn: sqlite3.Connection) -> None:
            self._conn = conn

        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Cursor:
            nonlocal bumped
            if (
                not bumped
                and "SET status = ?" in sql
                and "deleted_at = ?" in sql
            ):
                bumped = True
                self._conn.execute(
                    "UPDATE messages SET version = version + 1 WHERE id = ?",
                    (params[-3],),
                )
            return self._conn.execute(sql, params)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._conn, name)

    monkeypatch.setattr(backend, "_connect", lambda: _ConnectionProxy(real_connect()))
    claim = backend.claim_next("q", DeliveryMode.AT_MOST_ONCE, timedelta(seconds=30), "w", now)
    assert claim.delivery is not None


def test_backend_idempotency_integrity_error_retries_insert(tmp_path, monkeypatch) -> None:
    backend = SQLiteBackend(tmp_path / "idemp-retry.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    backend.enqueue("q", {"x": 1}, idempotency_key="k", now=now)
    real_connect = backend._connect  # noqa: SLF001
    attempts = 0

    class _ConnectionProxy:
        def __init__(self, conn: sqlite3.Connection) -> None:
            self._conn = conn

        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Cursor:
            nonlocal attempts
            if "INSERT INTO messages" in sql:
                attempts += 1
                if attempts == 1:
                    raise sqlite3.IntegrityError("duplicate")
            return self._conn.execute(sql, params)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._conn, name)

    monkeypatch.setattr(backend, "_connect", lambda: _ConnectionProxy(real_connect()))
    duplicate = backend.enqueue("q", {"x": 1}, idempotency_key="k", now=now)
    assert duplicate == 1


def test_backend_ack_stale_receipt_when_update_loses_race(queue_factory) -> None:
    queue = queue_factory("ack-update-race")
    queue.enqueue({"x": 1})
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    backend = queue.backend
    assert isinstance(backend, SQLiteBackend)
    with closing(backend._connect()) as conn:  # noqa: SLF001
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE messages SET status = ? WHERE id = ?",
            (MessageStatus.ACKED.value, delivery.message_id),
        )
        now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        cur = conn.execute(
            """
            UPDATE messages
            SET status = ?, acked_at = ?, updated_at = ?,
                receipt_handle = NULL, worker_id = NULL,
                lease_expires_at = NULL, leased_at = NULL,
                version = version + 1
            WHERE receipt_handle = ?
              AND status = ?
              AND lease_expires_at > ?
            """,
            (
                MessageStatus.ACKED.value,
                sb._dt(now),
                sb._dt(now),
                delivery.receipt_handle,
                MessageStatus.LEASED.value,
                sb._dt(now),
            ),
        )
        assert cur.rowcount == 0
        conn.rollback()


def test_cli_main_module_entrypoint() -> None:
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "-m", "simplequeue.cli.main", "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "0.2.1" in result.stdout
