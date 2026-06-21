"""Package and worker internals tests."""

from __future__ import annotations

import logging

from simplequeue import defaults
from simplequeue.core.exceptions import QueueError, StorageError
from simplequeue.observability import events
from simplequeue.reliability import __init__ as reliability_init


def test_defaults_module_exports_expected_values() -> None:
    assert defaults.DEFAULT_QUEUE_NAME == "default"
    assert defaults.DEFAULT_DELIVERY_MODE == "at-least-once"
    assert defaults.DEFAULT_VISIBILITY_TIMEOUT == 30.0
    assert defaults.DEFAULT_MAX_ATTEMPTS == 3
    assert defaults.DEFAULT_WORKER_COUNT == 1
    assert defaults.DEFAULT_SHUTDOWN_MODE == "finish-current"


def test_events_module_exports_canonical_names() -> None:
    assert events.ENQUEUE == "enqueue"
    assert events.LEASE == "lease"
    assert events.DELETE_DELIVERY == "delete_delivery"
    assert events.WORKER_FAILURE == "worker_failure"
    assert events.SCHEDULER_TICK == "scheduler_tick"


def test_exception_hierarchy() -> None:
    assert issubclass(StorageError, Exception)
    assert issubclass(QueueError, Exception)
    assert issubclass(StorageError, QueueError) is False


def test_reliability_package_imports() -> None:
    assert reliability_init is not None


def test_worker_safe_ack_survives_storage_exception(queue_factory, monkeypatch, caplog) -> None:
    from datetime import UTC, datetime

    from simplequeue.core.delivery import Delivery
    from simplequeue.core.modes import DeliveryMode
    from simplequeue.workers.worker import Worker

    queue = queue_factory("ack-exc")
    worker = Worker(queue, lambda _d: True, visibility_timeout=30)
    delivery = Delivery(
        message_id=1,
        receipt_handle="rh",
        queue_name="ack-exc",
        payload={},
        attempt=1,
        delivery_mode=DeliveryMode.AT_LEAST_ONCE,
        leased_at=datetime(2026, 1, 1, tzinfo=UTC),
        lease_expires_at=None,
    )

    def boom(_handle: str):
        raise RuntimeError("storage down")

    monkeypatch.setattr(queue, "ack", boom)
    with caplog.at_level(logging.INFO, logger="simplequeue"):
        worker._safe_ack(delivery)
    assert any("ack failed" in record.message for record in caplog.records)


def test_worker_claim_budget_blocks_dequeue(queue_factory) -> None:
    from simplequeue.workers.claim_budget import ClaimBudget
    from simplequeue.workers.worker import Worker

    queue = queue_factory("budget-worker")
    for index in range(5):
        queue.enqueue({"n": index})
    budget = ClaimBudget(2)
    worker = Worker(
        queue,
        lambda _d: True,
        claim_budget=budget,
        poll_interval=0.01,
        visibility_timeout=30,
    )
    worker.start()
    import time

    deadline = time.monotonic() + 3
    while time.monotonic() < deadline and queue.stats(cached=False).acked < 2:
        time.sleep(0.01)
    worker.stop()
    worker.join(2)
    assert queue.stats(cached=False).acked == 2


def test_worker_stop_during_processing_nack_current_redelivers(queue_factory) -> None:
    import threading
    import time

    from simplequeue.core.delivery import Delivery
    from simplequeue.workers.shutdown import ShutdownMode
    from simplequeue.workers.worker import Worker

    queue = queue_factory("stop-mid")
    queue.enqueue({"x": 1})
    started = threading.Event()
    release = threading.Event()

    def processor(_delivery: Delivery) -> bool:
        started.set()
        release.wait(timeout=2)
        return True

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
    again = queue.dequeue(worker_id="next")
    assert again is not None
