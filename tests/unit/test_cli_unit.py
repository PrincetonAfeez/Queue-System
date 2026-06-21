from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path

import pytest

from simplequeue.cache.stats_cache import StatsCache
from simplequeue.cli import _shared
from simplequeue.cli.commands import (
    consume,
    demo,
    dlq,
    init_db,
    inspect,
    list_queues,
    peek,
    produce,
    stats,
    sweep,
)
from simplequeue.cli.demos import DEMOS, dumps_demo, run_demo
from simplequeue.cli.main import _merged_config, build_parser, main
from simplequeue.config import QueueConfig
from simplequeue.core.message import Message
from simplequeue.core.modes import DeliveryMode
from simplequeue.core.states import MessageStatus


class _SampleEnum(Enum):
    FOO = "foo-value"


def test_jsonable_handles_dataclass_enum_datetime_path() -> None:
    message = Message(
        id=1,
        queue_name="q",
        payload={"x": 1},
        status=MessageStatus.AVAILABLE,
        attempts=0,
        max_attempts=3,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
        available_at=datetime(2026, 1, 1, tzinfo=UTC),
        leased_at=None,
        lease_expires_at=None,
        acked_at=None,
        dead_lettered_at=None,
        idempotency_key=None,
        last_error=None,
        version=0,
    )
    data = _shared._jsonable(message)
    assert data["status"] == "available"
    assert data["created_at"].startswith("2026-01-01")
    assert _shared._jsonable(_SampleEnum.FOO) == "foo-value"
    assert _shared._jsonable(Path("a/b")) == str(Path("a/b"))
    assert _shared._jsonable([1, 2]) == [1, 2]
    assert _shared._jsonable({"k": 1}) == {"k": 1}


def test_make_queue_uses_config_and_args(tmp_path: Path) -> None:
    config = QueueConfig(database_path=str(tmp_path / "cli.db"), queue_name="default")
    args = argparse.Namespace(queue="override")
    queue = _shared.make_queue(args, config)
    assert queue.queue_name == "override"
    assert queue.backend.db_path == tmp_path / "cli.db"


def test_print_json_writes_to_stdout(capsys) -> None:
    _shared.print_json({"hello": "world"})
    out = capsys.readouterr().out
    assert json.loads(out) == {"hello": "world"}


def test_build_parser_registers_all_commands() -> None:
    parser = build_parser()
    command_args = {
        "init-db": ["init-db", "--db", "q.db"],
        "produce": ["produce", "--db", "q.db"],
        "consume": ["consume", "--db", "q.db"],
        "sweep": ["sweep", "--db", "q.db"],
        "stats": ["stats", "--db", "q.db"],
        "list-queues": ["list-queues", "--db", "q.db"],
        "peek": ["peek", "--db", "q.db"],
        "inspect": ["inspect", "--db", "q.db", "--message-id", "1"],
        "dlq": ["dlq", "--db", "q.db"],
        "dlq-requeue": ["dlq-requeue", "--db", "q.db", "--message-id", "1"],
        "demo": ["demo", "basic", "--db", "q.db"],
    }
    for command, argv in command_args.items():
        args = parser.parse_args(argv)
        assert hasattr(args, "handler"), command


def test_merged_config_applies_cli_overrides() -> None:
    parser = build_parser()
    args = parser.parse_args(
        ["consume", "--db", "custom.db", "--queue", "jobs", "--mode", "at-most-once", "--workers", "3"]
    )
    config = _merged_config(args)
    assert config.database_path == "custom.db"
    assert config.queue_name == "jobs"
    assert config.delivery_mode == "at-most-once"
    assert config.worker_count == 3


def test_init_db_handler(tmp_path: Path, capsys) -> None:
    db = str(tmp_path / "init.db")
    args = argparse.Namespace(db=db, config=None, queue=None)
    config = QueueConfig(database_path=db)
    assert init_db.run(args, config) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["initialized"] is True


def test_produce_handler_default_payload(tmp_path: Path, capsys) -> None:
    db = str(tmp_path / "produce.db")
    args = argparse.Namespace(
        db=db,
        config=None,
        queue="jobs",
        payload=None,
        payload_template=None,
        count=2,
        idempotency_key=None,
        idempotent=False,
        max_attempts=None,
    )
    config = QueueConfig(database_path=db)
    assert produce.run(args, config) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 2
    assert payload["requested"] == 2


def test_produce_handler_payload_template(tmp_path: Path, capsys) -> None:
    db = str(tmp_path / "template.db")
    args = argparse.Namespace(
        db=db,
        config=None,
        queue="jobs",
        payload=None,
        payload_template='{"n":"{n}"}',
        count=1,
        idempotency_key=None,
        idempotent=False,
        max_attempts=None,
    )
    config = QueueConfig(database_path=db)
    assert produce.run(args, config) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1


def test_peek_stats_sweep_list_handlers(tmp_path: Path, capsys) -> None:
    db = str(tmp_path / "handlers.db")
    config = QueueConfig(database_path=db)
    produce_args = argparse.Namespace(
        db=db,
        config=None,
        queue="jobs",
        payload='{"x":1}',
        payload_template=None,
        count=1,
        idempotency_key=None,
        idempotent=False,
        max_attempts=None,
    )
    produce.run(produce_args, config)
    capsys.readouterr()

    peek_args = argparse.Namespace(db=db, config=None, queue="jobs", limit=5)
    assert peek.run(peek_args, config) == 0
    peek_out = json.loads(capsys.readouterr().out)
    assert len(peek_out["messages"]) == 1

    stats_args = argparse.Namespace(db=db, config=None, queue="jobs", no_cache=True)
    assert stats.run(stats_args, config) == 0
    stats_out = json.loads(capsys.readouterr().out)
    assert stats_out["enqueued"] == 1

    sweep_args = argparse.Namespace(db=db, config=None, queue="jobs")
    assert sweep.run(sweep_args, config) == 0
    sweep_out = json.loads(capsys.readouterr().out)
    assert "expired" in sweep_out

    list_args = argparse.Namespace(db=db, config=None, queue="jobs")
    assert list_queues.run(list_args, config) == 0
    list_out = json.loads(capsys.readouterr().out)
    assert "jobs" in list_out["queues"]


def test_inspect_handler_found_and_not_found(tmp_path: Path, capsys) -> None:
    db = str(tmp_path / "inspect.db")
    config = QueueConfig(database_path=db)
    produce_args = argparse.Namespace(
        db=db,
        config=None,
        queue="jobs",
        payload='{"x":1}',
        payload_template=None,
        count=1,
        idempotency_key=None,
        idempotent=False,
        max_attempts=None,
    )
    produce.run(produce_args, config)
    produced = json.loads(capsys.readouterr().out)
    message_id = produced["message_ids"][0]

    missing_args = argparse.Namespace(db=db, config=None, queue="jobs", message_id=999)
    assert inspect.run(missing_args, config) == 3
    missing = json.loads(capsys.readouterr().out)
    assert missing["found"] is False

    found_args = argparse.Namespace(db=db, config=None, queue="jobs", message_id=message_id)
    assert inspect.run(found_args, config) == 0
    found = json.loads(capsys.readouterr().out)
    assert found["message"]["payload"] == {"x": 1}


def test_dlq_handlers(tmp_path: Path, capsys) -> None:
    db = str(tmp_path / "dlq-handlers.db")
    config = QueueConfig(database_path=db, max_attempts=1)
    produce_args = argparse.Namespace(
        db=db,
        config=None,
        queue="jobs",
        payload='{"x":1}',
        payload_template=None,
        count=1,
        idempotency_key=None,
        idempotent=False,
        max_attempts=1,
    )
    produce.run(produce_args, config)
    consume_args = argparse.Namespace(
        db=db,
        config=None,
        queue="jobs",
        mode=DeliveryMode.AT_LEAST_ONCE,
        workers=1,
        visibility_timeout=30.0,
        limit=1,
        duration=None,
        idle_timeout=0.2,
        poll_interval=0.05,
        process_time=0.0,
        fail_every=1,
        sweeper=False,
        sweeper_interval=1.0,
        shutdown_mode="finish-current",
    )
    consume.run(consume_args, config)
    capsys.readouterr()

    list_args = argparse.Namespace(db=db, config=None, queue="jobs")
    assert dlq.run_list(list_args, config) == 0
    dead = json.loads(capsys.readouterr().out)["dead_letters"]
    assert len(dead) == 1
    message_id = dead[0]["original_message_id"]

    requeue_args = argparse.Namespace(db=db, config=None, queue="jobs", message_id=message_id)
    assert dlq.run_requeue(requeue_args, config) == 0
    requeued = json.loads(capsys.readouterr().out)
    assert requeued["requeued_message_id"] == message_id


def test_demo_handler(tmp_path: Path, capsys) -> None:
    db = str(tmp_path / "demo-handler.db")
    args = argparse.Namespace(demo_name="basic", db=db, config=None)
    config = QueueConfig(database_path=db)
    assert demo.run(args, config) == 0
    payload = json.loads(capsys.readouterr().out)
    assert "enqueued" in payload


@pytest.mark.parametrize("name", sorted(DEMOS - {"all"}))
def test_run_demo_each_name(name: str) -> None:
    result = run_demo(name)
    assert isinstance(result, dict)
    assert result


def test_run_demo_all_excludes_unsafe() -> None:
    result = run_demo("all")
    assert "unsafe-double-claim" not in result
    assert "basic" in result


def test_run_demo_unknown_raises() -> None:
    with pytest.raises(ValueError, match="unknown demo"):
        run_demo("not-a-demo")


def test_dumps_demo_serializes_result() -> None:
    text = dumps_demo({"count": 1, "when": datetime(2026, 1, 1, tzinfo=UTC)})
    assert "count" in text
    assert "2026" in text


def test_main_queue_error_exit_code(tmp_path: Path, monkeypatch) -> None:
    from simplequeue.core.exceptions import QueueError

    def boom(*args: object, **kwargs: object) -> int:
        raise QueueError("domain failure")

    monkeypatch.setattr("simplequeue.cli.commands.init_db.run", boom)
    assert main(["init-db", "--db", str(tmp_path / "q.db")]) == 4


def test_main_unexpected_exception_exit_code(tmp_path: Path, monkeypatch) -> None:
    def boom(*args: object, **kwargs: object) -> int:
        raise RuntimeError("unexpected")

    monkeypatch.setattr("simplequeue.cli.commands.init_db.run", boom)
    assert main(["init-db", "--db", str(tmp_path / "q.db")]) == 1


def test_stats_cache_evictions_property() -> None:
    cache = StatsCache(ttl_seconds=1.0)
    assert cache.evictions == 0
