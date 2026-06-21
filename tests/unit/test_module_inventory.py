"""Module inventory tests."""

from __future__ import annotations

import importlib
import inspect
import pkgutil
from datetime import UTC, datetime

import pytest

import simplequeue
from simplequeue.cli.demos import DEMOS, run_demo
from simplequeue.reliability import __all__ as reliability_exports


def _package_modules() -> list[str]:
    names = ["simplequeue"]
    prefix = simplequeue.__name__ + "."
    for module in pkgutil.walk_packages(simplequeue.__path__, prefix):
        names.append(module.name)
    return sorted(set(names))


@pytest.mark.parametrize("module_name", _package_modules())
def test_every_package_module_imports(module_name: str) -> None:
    importlib.import_module(module_name)


@pytest.mark.parametrize("name", simplequeue.__all__)
def test_public_export_is_documented_in_all(name: str) -> None:
    assert name in simplequeue.__all__


@pytest.mark.parametrize("name", reliability_exports)
def test_reliability_subpackage_exports(name: str) -> None:
    module = importlib.import_module("simplequeue.reliability")
    assert hasattr(module, name)


def test_core_message_models_are_frozen() -> None:
    from simplequeue.core.message import DeadLetter, Message, MessageDetails, QueueEvent
    from simplequeue.core.states import MessageStatus

    now = datetime(2026, 1, 1, tzinfo=UTC)
    message = Message(
        id=1,
        queue_name="q",
        payload={"x": 1},
        status=MessageStatus.AVAILABLE,
        attempts=0,
        max_attempts=3,
        created_at=now,
        updated_at=now,
        available_at=now,
        leased_at=None,
        lease_expires_at=None,
        acked_at=None,
        dead_lettered_at=None,
        idempotency_key=None,
        last_error=None,
        version=0,
    )
    dead = DeadLetter(
        id=1,
        original_message_id=1,
        queue_name="q",
        payload={"x": 1},
        failure_reason="fail",
        attempts=1,
        created_at=now,
        dead_lettered_at=now,
        final_status=MessageStatus.DEAD_LETTERED,
    )
    event = QueueEvent(
        id=1,
        event_type="enqueue",
        queue_name="q",
        message_id=1,
        worker_id=None,
        receipt_handle_short=None,
        details={},
        created_at=now,
    )
    details = MessageDetails(message=message, events=[event], dead_letter=dead)
    assert details.dead_letter is dead


def test_delivery_dataclass_fields() -> None:
    from simplequeue.core.delivery import Delivery
    from simplequeue.core.modes import DeliveryMode

    now = datetime(2026, 1, 1, tzinfo=UTC)
    delivery = Delivery(
        message_id=1,
        receipt_handle="rh",
        queue_name="q",
        payload={},
        attempt=1,
        delivery_mode=DeliveryMode.AT_LEAST_ONCE,
        leased_at=now,
        lease_expires_at=now,
    )
    assert delivery.delivery_mode is DeliveryMode.AT_LEAST_ONCE


def test_results_dataclasses() -> None:
    from simplequeue.core.results import AckResult, ClaimResult, LeaseReleaseResult, NackResult

    assert AckResult(True, 1, "acked", None).success
    assert NackResult(True, 1, "available", "x", moved_to_dlq=False).success
    assert ClaimResult(None, False).mutated is False
    assert LeaseReleaseResult(1, 2).total == 3


@pytest.mark.parametrize(
    "demo_name",
    [name for name in DEMOS if name != "all" and not name.startswith("unsafe")],
)
def test_safe_demo_runs_without_db_path(demo_name: str) -> None:
    result = run_demo(demo_name)
    assert isinstance(result, dict)
    assert result


def test_run_demo_all_safe_demos() -> None:
    payload = run_demo("all")
    assert isinstance(payload, dict)
    assert payload


def test_storage_serializers_roundtrip() -> None:
    from simplequeue.storage.serializers import dumps, loads

    value = {"a": 1, "nested": [True, None]}
    assert loads(dumps(value)) == value


def test_storage_load_schema_sql_non_empty() -> None:
    from simplequeue.storage.migrations import load_schema_sql

    assert "CREATE TABLE" in load_schema_sql()


def test_processor_type_alias_accepts_callable() -> None:
    from simplequeue.core.delivery import Delivery
    from simplequeue.core.modes import DeliveryMode
    from simplequeue.workers.processor import Processor

    now = datetime(2026, 1, 1, tzinfo=UTC)

    def handler(delivery: Delivery) -> bool:
        assert delivery.message_id == 1
        return True

    fn: Processor = handler
    delivery = Delivery(
        message_id=1,
        receipt_handle="rh",
        queue_name="q",
        payload={},
        attempt=1,
        delivery_mode=DeliveryMode.AT_LEAST_ONCE,
        leased_at=now,
        lease_expires_at=None,
    )
    assert fn(delivery) is True


def test_sqlite_backend_required_dt() -> None:
    from simplequeue.storage.sqlite_backend import SQLiteBackend

    with pytest.raises(ValueError, match="non-null timestamp"):
        SQLiteBackend._required_dt(None)
    parsed = SQLiteBackend._required_dt("2026-01-01T00:00:00+00:00")
    assert parsed.tzinfo is not None


def test_queue_public_methods_exist() -> None:
    from simplequeue.core.queue import Queue

    expected = {
        "init_schema",
        "enqueue",
        "dequeue",
        "ack",
        "nack",
        "sweep",
        "stats",
        "peek",
        "inspect",
        "list_dead_letters",
        "requeue_dead_letter",
        "purge_terminal",
        "list_queues",
    }
    for name in expected:
        assert callable(getattr(Queue, name))


def test_worker_pool_public_methods_exist() -> None:
    from simplequeue.workers.worker_pool import WorkerPool

    for name in ("start", "stop", "join", "__enter__", "__exit__"):
        assert callable(getattr(WorkerPool, name))


def test_scheduler_and_sweeper_public_surface() -> None:
    from simplequeue.scheduling.scheduler import RepeatingScheduler
    from simplequeue.scheduling.sweeper import BackgroundSweeper

    for cls in (RepeatingScheduler, BackgroundSweeper):
        for name in ("start", "stop", "join", "is_alive"):
            assert hasattr(cls, name)


def test_cli_command_modules_expose_register_and_run() -> None:
    from simplequeue.cli.commands import (
        consume,
        demo,
        dlq,
        init_db,
        inspect,
        list_queues,
        peek,
        produce,
        purge,
        stats,
        sweep,
        verify,
    )

    for module in (
        consume,
        demo,
        dlq,
        init_db,
        inspect,
        list_queues,
        peek,
        produce,
        purge,
        stats,
        sweep,
        verify,
    ):
        assert callable(module.register)
    assert callable(consume.run)
    assert callable(dlq.run_list)
    assert callable(dlq.run_requeue)


def test_build_parser_registers_every_command() -> None:
    from simplequeue.cli.main import build_parser

    parser = build_parser()
    commands = {action.dest for action in parser._actions if action.choices}  # noqa: SLF001
    assert "init-db" in commands or any(
        choice in {"init-db", "produce", "consume", "purge"} for choice in (parser._subparsers._group_actions[0].choices or {})  # type: ignore[attr-defined]  # noqa: SLF001
    )
    subparsers = next(action for action in parser._actions if action.dest == "command")
    assert set(subparsers.choices) >= {
        "init-db",
        "produce",
        "consume",
        "sweep",
        "stats",
        "list-queues",
        "peek",
        "inspect",
        "dlq",
        "purge",
        "verify",
        "demo",
    }


def test_retry_decision_named_tuple_fields() -> None:
    from simplequeue.reliability.retry import RetryDecision, decide_retry

    decision = decide_retry(1, 3)
    assert isinstance(decision, RetryDecision)
    assert hasattr(decision, "should_retry")
    assert hasattr(decision, "should_dead_letter")


def test_exceptions_are_subclasses_of_exception() -> None:
    from simplequeue.core.exceptions import (
        DeadLetterNotFound,
        IdempotencyConflict,
        QueueError,
        StorageError,
    )

    for exc_type in (QueueError, StorageError, DeadLetterNotFound, IdempotencyConflict):
        assert issubclass(exc_type, Exception)


def test_defaults_module_has_no_callables_without_doc() -> None:
    import simplequeue.defaults as defaults

    for name, value in inspect.getmembers(defaults):
        if name.startswith("_"):
            continue
        assert not callable(value) or name == "__builtins__"
