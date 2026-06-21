from __future__ import annotations

import argparse
import json

import pytest

from simplequeue.cli import _shared
from simplequeue.cli.commands import inspect, produce
from simplequeue.cli.demos import run_demo
from simplequeue.config import QueueConfig


def test_jsonable_handles_tuple() -> None:
    assert _shared._jsonable((1, 2)) == [1, 2]


def test_produce_rejects_dual_payload_flags(tmp_path) -> None:
    args = argparse.Namespace(
        db=str(tmp_path / "p.db"),
        config=None,
        queue="jobs",
        payload='{"a":1}',
        payload_template='{"n":"{n}"}',
        count=1,
        idempotency_key=None,
        idempotent=False,
        max_attempts=None,
    )
    with pytest.raises(ValueError, match="only one"):
        produce.run(args, QueueConfig(database_path=str(tmp_path / "p.db")))


def test_produce_rejects_count_zero(tmp_path) -> None:
    args = argparse.Namespace(
        db=str(tmp_path / "p.db"),
        config=None,
        queue="jobs",
        payload='{"a":1}',
        payload_template=None,
        count=0,
        idempotency_key=None,
        idempotent=False,
        max_attempts=None,
    )
    with pytest.raises(ValueError, match="count"):
        produce.run(args, QueueConfig(database_path=str(tmp_path / "p.db")))


def test_produce_idempotent_count_warning(tmp_path, capsys) -> None:
    db = str(tmp_path / "warn.db")
    config = QueueConfig(database_path=db)
    args = argparse.Namespace(
        db=db,
        config=None,
        queue="jobs",
        payload='{"x":1}',
        payload_template=None,
        count=3,
        idempotency_key=None,
        idempotent=True,
        max_attempts=None,
    )
    assert produce.run(args, config) == 0
    assert "dedupes" in capsys.readouterr().err


def test_produce_invalid_json_payload(tmp_path) -> None:
    args = argparse.Namespace(
        db=str(tmp_path / "bad.db"),
        config=None,
        queue="jobs",
        payload="{bad",
        payload_template=None,
        count=1,
        idempotency_key=None,
        idempotent=False,
        max_attempts=None,
    )
    with pytest.raises(ValueError, match="invalid JSON"):
        produce.run(args, QueueConfig(database_path=str(tmp_path / "bad.db")))


def test_inspect_wrong_queue_returns_exit_3(tmp_path, capsys) -> None:
    db = str(tmp_path / "inspect-wrong.db")
    config = QueueConfig(database_path=db)
    produce_args = argparse.Namespace(
        db=db,
        config=None,
        queue="alpha",
        payload='{"x":1}',
        payload_template=None,
        count=1,
        idempotency_key=None,
        idempotent=False,
        max_attempts=None,
    )
    produce.run(produce_args, config)
    message_id = json.loads(capsys.readouterr().out)["message_ids"][0]
    inspect_args = argparse.Namespace(db=db, config=None, queue="beta", message_id=message_id)
    assert inspect.run(inspect_args, config) == 3
    payload = json.loads(capsys.readouterr().out)
    assert payload["found"] is False
    assert "reason" in payload


def test_unsafe_demo_stale_ack_alias() -> None:
    result = run_demo("unsafe-stale-ack")
    assert result["demo"] == "unsafe_ack_by_message_id"
    assert result["corrupted"] is True


def test_run_safe_demo_not_implemented_raises() -> None:
    from simplequeue.cli.demos import _run_safe_demo

    with pytest.raises(ValueError, match="not implemented"):
        _run_safe_demo("not-real", "/tmp/x.db")


def test_run_unsafe_not_implemented_raises() -> None:
    from simplequeue.cli.demos import _run_unsafe

    with pytest.raises(ValueError, match="not implemented"):
        _run_unsafe("unsafe-not-real")


def test_main_value_error_exit_code(tmp_path, monkeypatch) -> None:
    from simplequeue.cli.main import main

    def boom(*args: object, **kwargs: object) -> int:
        raise ValueError("bad value")

    monkeypatch.setattr("simplequeue.cli.commands.init_db.run", boom)
    assert main(["init-db", "--db", str(tmp_path / "q.db")]) == 2


def test_main_file_not_found_exit_code(tmp_path, monkeypatch) -> None:
    from simplequeue.cli.main import main

    def boom(*args: object, **kwargs: object) -> int:
        raise FileNotFoundError("missing")

    monkeypatch.setattr("simplequeue.cli.commands.init_db.run", boom)
    assert main(["init-db", "--db", str(tmp_path / "q.db")]) == 2


def test_peek_rejects_limit_zero(tmp_path) -> None:
    from simplequeue.cli.commands import peek

    args = argparse.Namespace(db=str(tmp_path / "peek.db"), config=None, queue="jobs", limit=0)
    with pytest.raises(ValueError, match="limit"):
        peek.run(args, QueueConfig(database_path=str(tmp_path / "peek.db")))
