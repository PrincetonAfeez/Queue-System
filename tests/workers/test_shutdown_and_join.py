"""Shutdown and join tests."""

from __future__ import annotations

import threading
import time

from simplequeue.core.delivery import Delivery
from simplequeue.core.modes import DeliveryMode
from simplequeue.workers.shutdown import ShutdownMode
from simplequeue.workers.worker import Worker
from simplequeue.workers.worker_pool import WorkerPool


def test_shutdown_finish_current_before_processor_still_acks(queue_factory) -> None:
    queue = queue_factory("finish-stop-before")
    queue.enqueue({"x": 1})
    worker = Worker(
        queue,
        lambda delivery: True,
        shutdown_mode=ShutdownMode.FINISH_CURRENT,
        visibility_timeout=30,
        poll_interval=0.01,
    )
    delivery = queue.dequeue(worker_id="manual")
    assert delivery is not None
    worker.stop()
    worker._finish(delivery, True)  # noqa: SLF001 — finish-current path after stop
    assert queue.stats(cached=False).acked == 1

    queue = queue_factory("finish")
    queue.enqueue({"x": 1})
    started = threading.Event()
    release = threading.Event()

    def processor(delivery: Delivery) -> bool:
        started.set()
        release.wait(timeout=2)
        return True

    worker = Worker(
        queue,
        processor,
        delivery_mode=DeliveryMode.AT_LEAST_ONCE,
        shutdown_mode=ShutdownMode.FINISH_CURRENT,
        visibility_timeout=30,
        poll_interval=0.01,
    )
    worker.start()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and not started.is_set():
        time.sleep(0.01)
    assert started.is_set()
    worker.stop()
    release.set()
    worker.join(2)

    assert queue.stats(cached=False).acked == 1


def test_shutdown_nack_current_during_processing(queue_factory) -> None:
    queue = queue_factory("nack-mid")
    queue.enqueue({"x": 1})
    started = threading.Event()
    release = threading.Event()

    def processor(delivery: Delivery) -> bool:
        started.set()
        release.wait(timeout=2)
        return True

    worker = Worker(
        queue,
        processor,
        delivery_mode=DeliveryMode.AT_LEAST_ONCE,
        shutdown_mode=ShutdownMode.NACK_CURRENT,
        visibility_timeout=30,
        poll_interval=0.01,
    )
    worker.start()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and not started.is_set():
        time.sleep(0.01)
    assert started.is_set()
    worker.stop()
    release.set()
    worker.join(2)

    again = queue.dequeue(worker_id="next")
    assert again is not None


def test_worker_pool_join_timeout_is_total(queue_factory) -> None:
    queue = queue_factory("join-timeout")
    pool = WorkerPool(
        queue,
        lambda delivery: True,
        workers=3,
        visibility_timeout=30,
        poll_interval=0.05,
    )
    pool.start()
    start = time.monotonic()
    pool.stop()
    pool.join(0.2)
    elapsed = time.monotonic() - start
    assert elapsed < 0.6
    assert pool.all_stopped


def test_worker_pool_join_waits_for_slow_workers(queue_factory) -> None:
    queue = queue_factory("join-slow")
    started = threading.Event()
    release = threading.Event()

    def processor(delivery: Delivery) -> bool:
        started.set()
        release.wait(timeout=2)
        return True

    pool = WorkerPool(queue, processor, workers=2, visibility_timeout=30, poll_interval=0.01)
    for _ in range(2):
        queue.enqueue({"x": 1})
    pool.start()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and not started.is_set():
        time.sleep(0.01)
    pool.stop()
    release.set()
    pool.join(2)
    assert pool.all_stopped


def test_shutdown_abandon_current_on_processor_exception(queue_factory) -> None:
    queue = queue_factory("abandon-exc")
    queue.enqueue({"x": 1})
    started = threading.Event()
    release = threading.Event()

    def processor(delivery: Delivery) -> bool:
        started.set()
        release.wait(timeout=2)
        raise RuntimeError("boom")

    worker = Worker(
        queue,
        processor,
        delivery_mode=DeliveryMode.AT_LEAST_ONCE,
        shutdown_mode=ShutdownMode.ABANDON_CURRENT,
        visibility_timeout=30,
        poll_interval=0.01,
    )
    worker.start()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and not started.is_set():
        time.sleep(0.01)
    worker.stop()
    release.set()
    worker.join(2)

    assert queue.stats(cached=False).nacked == 0
    assert queue.stats(cached=False).in_flight_count == 1


def test_worker_pool_join_timeout_with_slow_processor(queue_factory) -> None:
    queue = queue_factory("join-slow-timeout")
    started = threading.Event()
    release = threading.Event()

    def processor(delivery: Delivery) -> bool:
        started.set()
        release.wait(timeout=2)
        return True

    pool = WorkerPool(queue, processor, workers=1, visibility_timeout=30, poll_interval=0.01)
    queue.enqueue({"x": 1})
    pool.start()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and not started.is_set():
        time.sleep(0.01)
    pool.stop()
    release.set()
    pool.join(0.5)
    assert pool.all_stopped


def test_worker_pool_join_returns_bool(queue_factory) -> None:
    queue = queue_factory("join-bool")
    pool = WorkerPool(queue, lambda _d: True, workers=1, poll_interval=0.01, visibility_timeout=30)
    queue.enqueue({"x": 1})
    pool.start()
    pool.stop()
    assert pool.join(2) is True


def test_background_sweeper_join_timeout_returns_false_while_alive(queue_factory) -> None:
    from simplequeue.scheduling.sweeper import BackgroundSweeper

    queue = queue_factory("sweeper-join")
    sweeper = BackgroundSweeper(queue, interval=60.0)
    sweeper.start()
    try:
        assert sweeper.join(0.001) is False
        assert sweeper.is_alive
    finally:
        sweeper.stop()
        assert sweeper.join(2) is True


def test_background_sweeper_can_restart_after_join(queue_factory) -> None:
    from simplequeue.scheduling.sweeper import BackgroundSweeper

    queue = queue_factory("sweeper-restart")
    first = BackgroundSweeper(queue, interval=0.05)
    first.start()
    first.stop()
    assert first.join(2) is True
    second = BackgroundSweeper(queue, interval=0.05)
    second.start()
    second.stop()
    assert second.join(2) is True
