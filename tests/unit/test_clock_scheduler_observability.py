from __future__ import annotations

import logging
import threading
import time
from datetime import UTC, datetime

import pytest

from simplequeue.observability.logging import get_logger, log_event
from simplequeue.scheduling.clock import Clock, FakeClock, SystemClock
from simplequeue.scheduling.scheduler import RepeatingScheduler


def test_system_clock_now_is_utc_aware() -> None:
    clock = SystemClock()
    now = clock.now()
    assert now.tzinfo is not None
    assert clock.monotonic() >= 0


def test_fake_clock_starting_at_default() -> None:
    clock = FakeClock.starting_at()
    assert clock.now() == datetime(2026, 1, 1, tzinfo=UTC)


def test_fake_clock_advance_and_sleep() -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    clock.advance(10)
    assert clock.now() == datetime(2026, 1, 1, 0, 0, 10, tzinfo=UTC)
    assert clock.monotonic_value == 10
    clock.sleep(5)
    assert clock.monotonic_value == 15


def test_fake_clock_wait_does_not_advance_logical_time() -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    stop = threading.Event()
    stop.set()
    assert clock.wait(stop, 1.0) is True
    assert clock.now() == datetime(2026, 1, 1, tzinfo=UTC)


def test_clock_base_class_methods() -> None:
    clock = Clock()
    with pytest.raises(NotImplementedError):
        clock.now()
    with pytest.raises(NotImplementedError):
        clock.monotonic()
    stop = threading.Event()
    stop.set()
    assert clock.wait(stop, 0.01) is True


def test_repeating_scheduler_rejects_non_positive_interval() -> None:
    with pytest.raises(ValueError, match="interval"):
        RepeatingScheduler(lambda: None, interval=0)


def test_repeating_scheduler_runs_callback() -> None:
    calls: list[int] = []

    def tick() -> None:
        calls.append(1)

    scheduler = RepeatingScheduler(tick, interval=0.02, name="test-sched")
    scheduler.start()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and len(calls) < 2:
        time.sleep(0.01)
    scheduler.stop()
    scheduler.join(2)
    assert len(calls) >= 2


def test_repeating_scheduler_start_is_idempotent() -> None:
    calls: list[int] = []
    scheduler = RepeatingScheduler(lambda: calls.append(1), interval=0.05)
    scheduler.start()
    scheduler.start()
    assert scheduler.is_alive
    scheduler.stop()
    scheduler.join(2)


def test_get_logger_returns_named_logger() -> None:
    logger = get_logger("simplequeue-test")
    assert logger.name == "simplequeue-test"


def test_log_event_emits_json(caplog) -> None:
    logger = logging.getLogger("simplequeue-log-test")
    with caplog.at_level(logging.INFO, logger="simplequeue-log-test"):
        log_event(logger, "test_event", queue_name="q", count=3)
    assert any("test_event" in record.message for record in caplog.records)
    assert any("queue_name" in record.message for record in caplog.records)


def test_log_event_respects_level(caplog) -> None:
    logger = logging.getLogger("simplequeue-log-level")
    with caplog.at_level(logging.WARNING, logger="simplequeue-log-level"):
        log_event(logger, "debug_only", level=logging.DEBUG)
    assert not caplog.records
