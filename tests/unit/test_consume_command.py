from __future__ import annotations

import argparse
from unittest.mock import MagicMock

import pytest

from simplequeue.cli.commands import consume
from simplequeue.config import QueueConfig
from simplequeue.core.modes import DeliveryMode


def test_consume_rejects_invalid_limit(tmp_path) -> None:
    args = argparse.Namespace(
        limit=0,
        duration=None,
        fail_every=0,
        process_time=0.0,
        db=str(tmp_path / "c.db"),
        config=None,
        queue="jobs",
        mode=None,
        workers=1,
        visibility_timeout=30.0,
        idle_timeout=1.0,
        poll_interval=0.1,
        sweeper=False,
        sweeper_interval=1.0,
        shutdown_mode="finish-current",
    )
    with pytest.raises(ValueError, match="limit"):
        consume.run(args, QueueConfig(database_path=str(tmp_path / "c.db")))


def test_consume_rejects_invalid_duration(tmp_path) -> None:
    args = argparse.Namespace(
        limit=None,
        duration=0,
        fail_every=0,
        process_time=0.0,
        db=str(tmp_path / "c.db"),
        config=None,
        queue="jobs",
        mode=None,
        workers=1,
        visibility_timeout=30.0,
        idle_timeout=1.0,
        poll_interval=0.1,
        sweeper=False,
        sweeper_interval=1.0,
        shutdown_mode="finish-current",
    )
    with pytest.raises(ValueError, match="duration"):
        consume.run(args, QueueConfig(database_path=str(tmp_path / "c.db")))


def test_consume_rejects_negative_fail_every(tmp_path) -> None:
    args = argparse.Namespace(
        limit=1,
        duration=None,
        fail_every=-1,
        process_time=0.0,
        db=str(tmp_path / "c.db"),
        config=None,
        queue="jobs",
        mode=None,
        workers=1,
        visibility_timeout=30.0,
        idle_timeout=1.0,
        poll_interval=0.1,
        sweeper=False,
        sweeper_interval=1.0,
        shutdown_mode="finish-current",
    )
    with pytest.raises(ValueError, match="fail-every"):
        consume.run(args, QueueConfig(database_path=str(tmp_path / "c.db")))


def test_consume_rejects_negative_process_time(tmp_path) -> None:
    args = argparse.Namespace(
        limit=1,
        duration=None,
        fail_every=0,
        process_time=-1.0,
        db=str(tmp_path / "c.db"),
        config=None,
        queue="jobs",
        mode=None,
        workers=1,
        visibility_timeout=30.0,
        idle_timeout=1.0,
        poll_interval=0.1,
        sweeper=False,
        sweeper_interval=1.0,
        shutdown_mode="finish-current",
    )
    with pytest.raises(ValueError, match="process-time"):
        consume.run(args, QueueConfig(database_path=str(tmp_path / "c.db")))


def test_consume_rejects_invalid_poll_interval(tmp_path) -> None:
    args = argparse.Namespace(
        limit=1,
        duration=None,
        fail_every=0,
        process_time=0.0,
        db=str(tmp_path / "c.db"),
        config=None,
        queue="jobs",
        mode=None,
        workers=1,
        visibility_timeout=30.0,
        idle_timeout=1.0,
        poll_interval=0.0,
        sweeper=False,
        sweeper_interval=1.0,
        shutdown_mode="finish-current",
    )
    with pytest.raises(ValueError, match="poll_interval"):
        consume.run(args, QueueConfig(database_path=str(tmp_path / "c.db")))


def test_consume_rejects_invalid_idle_timeout(tmp_path) -> None:
    args = argparse.Namespace(
        limit=1,
        duration=None,
        fail_every=0,
        process_time=0.0,
        db=str(tmp_path / "c.db"),
        config=None,
        queue="jobs",
        mode=None,
        workers=1,
        visibility_timeout=30.0,
        idle_timeout=0.0,
        poll_interval=0.1,
        sweeper=False,
        sweeper_interval=1.0,
        shutdown_mode="finish-current",
    )
    with pytest.raises(ValueError, match="idle_timeout"):
        consume.run(args, QueueConfig(database_path=str(tmp_path / "c.db")))


def test_consume_rejects_invalid_sweeper_interval(tmp_path) -> None:
    args = argparse.Namespace(
        limit=1,
        duration=None,
        fail_every=0,
        process_time=0.0,
        db=str(tmp_path / "c.db"),
        config=None,
        queue="jobs",
        mode=None,
        workers=1,
        visibility_timeout=30.0,
        idle_timeout=1.0,
        poll_interval=0.1,
        sweeper=True,
        sweeper_interval=0.0,
        shutdown_mode="finish-current",
    )
    with pytest.raises(ValueError, match="sweeper_interval"):
        consume.run(args, QueueConfig(database_path=str(tmp_path / "c.db")))


def test_consume_with_process_time(tmp_path, capsys) -> None:
    db = str(tmp_path / "process.db")
    config = QueueConfig(database_path=db, poll_interval=0.05, idle_timeout=0.2)
    from simplequeue.cli.commands import init_db, produce

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
    args = argparse.Namespace(
        limit=1,
        duration=None,
        fail_every=0,
        process_time=0.05,
        db=db,
        config=None,
        queue="jobs",
        mode=DeliveryMode.AT_LEAST_ONCE,
        workers=1,
        visibility_timeout=30.0,
        idle_timeout=0.2,
        poll_interval=0.05,
        sweeper=False,
        sweeper_interval=1.0,
        shutdown_mode="finish-current",
    )
    assert consume.run(args, config) == 0
    output = capsys.readouterr().out
    assert "processed" in output
    assert "consume_finished" in output


def test_consume_join_timeout_warning(tmp_path, capsys, monkeypatch) -> None:
    db = str(tmp_path / "join-warn.db")
    config = QueueConfig(database_path=db, poll_interval=0.05, idle_timeout=0.2)
    from simplequeue.cli.commands import init_db, produce

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

    slow_pool = MagicMock()
    slow_pool.join.return_value = False
    slow_pool.stop = MagicMock()

    monkeypatch.setattr("simplequeue.cli.commands.consume.WorkerPool", lambda *a, **k: slow_pool)
    monkeypatch.setattr("simplequeue.cli.commands.consume.DEFAULT_CONSUME_JOIN_TIMEOUT", 0.001)

    args = argparse.Namespace(
        limit=None,
        duration=0.01,
        fail_every=0,
        process_time=0.0,
        db=db,
        config=None,
        queue="jobs",
        mode=DeliveryMode.AT_LEAST_ONCE,
        workers=1,
        visibility_timeout=30.0,
        idle_timeout=0.2,
        poll_interval=0.05,
        sweeper=False,
        sweeper_interval=1.0,
        shutdown_mode="finish-current",
    )
    assert consume.run(args, config) == 0
    assert "workers did not stop within join timeout" in capsys.readouterr().err


def test_consume_sweeper_join_timeout_warning(tmp_path, capsys, monkeypatch) -> None:
    db = str(tmp_path / "sweeper-warn.db")
    config = QueueConfig(database_path=db, poll_interval=0.05, idle_timeout=0.2)
    from simplequeue.cli.commands import init_db, produce

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

    fast_pool = MagicMock()
    fast_pool.join.return_value = True
    fast_pool.stop = MagicMock()
    fast_pool.start = MagicMock()

    slow_sweeper = MagicMock()
    slow_sweeper.join.return_value = False
    slow_sweeper.stop = MagicMock()
    slow_sweeper.start = MagicMock()

    monkeypatch.setattr("simplequeue.cli.commands.consume.WorkerPool", lambda *a, **k: fast_pool)
    monkeypatch.setattr("simplequeue.cli.commands.consume.BackgroundSweeper", lambda *a, **k: slow_sweeper)
    monkeypatch.setattr("simplequeue.cli.commands.consume.DEFAULT_CONSUME_JOIN_TIMEOUT", 0.001)

    args = argparse.Namespace(
        limit=None,
        duration=0.01,
        fail_every=0,
        process_time=0.0,
        db=db,
        config=None,
        queue="jobs",
        mode=DeliveryMode.AT_LEAST_ONCE,
        workers=1,
        visibility_timeout=30.0,
        idle_timeout=0.2,
        poll_interval=0.05,
        sweeper=True,
        sweeper_interval=0.1,
        shutdown_mode="finish-current",
    )
    assert consume.run(args, config) == 0
    assert "sweeper did not stop within join timeout" in capsys.readouterr().err
