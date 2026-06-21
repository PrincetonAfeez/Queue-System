"""Worker pool tests."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from threading import Lock

from simplequeue.core.delivery import Delivery
from simplequeue.core.modes import DeliveryMode
from simplequeue.scheduling.clock import FakeClock
from simplequeue.workers.shutdown import ShutdownMode
from simplequeue.workers.worker import Worker
from simplequeue.workers.worker_pool import WorkerPool


def _drain(queue, predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and not predicate():
        time.sleep(0.01)


def test_worker_pool_processes_all_and_stops_with_no_orphan_threads(queue_factory) -> None:
    queue = queue_factory("pool")
    total = 20
    for index in range(total):
        queue.enqueue({"i": index})

    seen: list[int] = []
    lock = Lock()

    def processor(delivery: Delivery) -> bool:
        with lock:
            seen.append(delivery.message_id)
        return True

    pool = WorkerPool(queue, processor, workers=4, visibility_timeout=30, poll_interval=0.01)
    pool.start()
    _drain(queue, lambda: queue.stats(cached=False).acked >= total)
    pool.stop()
    pool.join(2)

    assert queue.stats(cached=False).acked == total
    assert len(set(seen)) == total  # no double-delivery
    assert pool.all_stopped  # no orphan worker threads


def test_worker_retries_then_dlqs_when_processor_returns_false(queue_factory) -> None:
    queue = queue_factory("falsey")
    queue.enqueue({"x": 1}, max_attempts=2)
    attempts: list[int] = []
    lock = Lock()

    def processor(delivery: Delivery) -> bool:
        with lock:
            attempts.append(delivery.attempt)
        return False

    pool = WorkerPool(queue, processor, workers=1, visibility_timeout=30, poll_interval=0.01)
    pool.start()
    _drain(queue, lambda: len(queue.list_dead_letters()) == 1)
    pool.stop()
    pool.join(2)

    assert attempts == [1, 2]
    assert len(queue.list_dead_letters()) == 1
    assert pool.all_stopped


def test_worker_nacks_on_processor_exception(queue_factory) -> None:
    queue = queue_factory("raises")
    queue.enqueue({"x": 1}, max_attempts=1)

    def processor(delivery: Delivery) -> bool:
        raise RuntimeError("boom")

    pool = WorkerPool(queue, processor, workers=1, visibility_timeout=30, poll_interval=0.01)
    pool.start()
    _drain(queue, lambda: len(queue.list_dead_letters()) == 1)
    pool.stop()
    pool.join(2)

    assert len(queue.list_dead_letters()) == 1  # max_attempts=1 -> first failure DLQs
    assert pool.all_stopped


def test_at_most_once_worker_crash_loses_message(queue_factory) -> None:
    queue = queue_factory("amo")
    queue.enqueue({"low_value": True})

    def processor(delivery: Delivery) -> bool:
        raise RuntimeError("crash mid-process")

    pool = WorkerPool(
        queue,
        processor,
        workers=1,
        delivery_mode=DeliveryMode.AT_MOST_ONCE,
        visibility_timeout=30,
        poll_interval=0.01,
    )
    pool.start()
    _drain(queue, lambda: queue.stats(cached=False).delivered >= 1)
    pool.stop()
    pool.join(2)

    assert queue.dequeue(worker_id="after") is None  # deleted at delivery, lost on crash
    assert len(queue.list_dead_letters()) == 0
    assert pool.all_stopped


def test_shutdown_nack_current_releases_inflight_work(queue_factory) -> None:
    queue = queue_factory("sd-nack")
    queue.enqueue({"x": 1})
    worker = Worker(
        queue,
        lambda delivery: True,
        delivery_mode=DeliveryMode.AT_LEAST_ONCE,
        shutdown_mode=ShutdownMode.NACK_CURRENT,
        visibility_timeout=30,
    )
    delivery = queue.dequeue(worker_id="manual")
    assert delivery is not None

    worker._handle_interrupted_delivery(delivery)  # simulate stop right after claim

    again = queue.dequeue(worker_id="next")
    assert again is not None
    assert again.message_id == delivery.message_id


def test_shutdown_abandon_current_leaves_lease_for_sweeper(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("sd-abandon", clock=clock)
    queue.enqueue({"x": 1})
    worker = Worker(
        queue,
        lambda delivery: True,
        delivery_mode=DeliveryMode.AT_LEAST_ONCE,
        shutdown_mode=ShutdownMode.ABANDON_CURRENT,
        visibility_timeout=5,
    )
    delivery = queue.dequeue(visibility_timeout=5, worker_id="manual")
    assert delivery is not None

    worker._handle_interrupted_delivery(delivery)  # abandon: do nothing, lease stands

    assert queue.dequeue(worker_id="next") is None  # still leased
    clock.advance(6)
    queue.sweep()
    redelivered = queue.dequeue(worker_id="next")
    assert redelivered is not None
    assert redelivered.message_id == delivery.message_id


def test_background_sweeper_starts_and_stops_cleanly(queue_factory) -> None:
    from simplequeue.scheduling.sweeper import BackgroundSweeper

    queue = queue_factory("sweep")
    with BackgroundSweeper(queue, interval=0.02) as sweeper:
        assert sweeper.is_alive
        time.sleep(0.05)
    assert not sweeper.is_alive  # no orphan sweeper thread


def test_background_sweeper_redelivers_expired_lease(queue_factory) -> None:
    from datetime import UTC, datetime

    from simplequeue.scheduling.clock import FakeClock
    from simplequeue.scheduling.sweeper import BackgroundSweeper

    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("sweeper-redeliver", clock=clock)
    queue.enqueue({"x": 1})
    delivery = queue.dequeue(visibility_timeout=5, worker_id="w")
    assert delivery is not None
    clock.advance(6)
    sweeper = BackgroundSweeper(queue, interval=0.02)
    sweeper.start()
    deadline = time.monotonic() + 2
    redelivered = None
    while time.monotonic() < deadline:
        redelivered = queue.dequeue(worker_id="w2")
        if redelivered is not None:
            break
        time.sleep(0.02)
    sweeper.stop()
    sweeper.join(2)
    assert redelivered is not None
    assert redelivered.message_id == delivery.message_id


def test_duplicate_background_sweeper_raises(queue_factory) -> None:
    import pytest

    from simplequeue.core.exceptions import QueueError
    from simplequeue.scheduling.sweeper import BackgroundSweeper

    queue = queue_factory("dup-sweeper")
    first = BackgroundSweeper(queue, interval=0.05)
    second = BackgroundSweeper(queue, interval=0.05)
    first.start()
    try:
        with pytest.raises(QueueError, match="only one BackgroundSweeper"):
            second.start()
    finally:
        first.stop()
        first.join(2)


def test_repeating_scheduler_survives_callback_exception() -> None:
    from simplequeue.scheduling.scheduler import RepeatingScheduler

    calls: list[int] = []

    def boom() -> None:
        calls.append(1)
        raise RuntimeError("tick failed")

    scheduler = RepeatingScheduler(boom, interval=0.01)
    scheduler.start()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and len(calls) < 3:
        time.sleep(0.01)
    alive = scheduler.is_alive
    scheduler.stop()
    scheduler.join(2)

    assert len(calls) >= 3  # kept ticking despite exceptions
    assert alive  # the thread did not die on the raised exception
