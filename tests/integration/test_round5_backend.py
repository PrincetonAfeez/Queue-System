from __future__ import annotations

import threading
import time
from datetime import UTC, datetime, timedelta

import pytest

from simplequeue.core.exceptions import IdempotencyConflict, StorageError
from simplequeue.core.modes import DeliveryMode
from simplequeue.scheduling.clock import FakeClock
from simplequeue.workers.claim_budget import ClaimBudget
from simplequeue.workers.worker_pool import WorkerPool


def test_idempotency_key_rejects_payload_mismatch(queue_factory) -> None:
    queue = queue_factory("idemp-mismatch")
    queue.enqueue({"job": 1}, idempotency_key="k")
    with pytest.raises(IdempotencyConflict, match="different payload"):
        queue.enqueue({"job": 999}, idempotency_key="k")


def test_enqueue_rejects_non_serializable_payload(queue_factory) -> None:
    queue = queue_factory("bad-payload")
    with pytest.raises(StorageError):
        queue.enqueue(object())


def test_queue_inspect_missing_returns_none(queue_factory) -> None:
    queue = queue_factory("inspect-none")
    assert queue.inspect(999_999) is None


def test_dequeue_accepts_timedelta_visibility_timeout(queue_factory) -> None:
    queue = queue_factory("td-vt")
    queue.enqueue({"x": 1})
    delivery = queue.dequeue(visibility_timeout=timedelta(seconds=30), worker_id="w")
    assert delivery is not None


def test_processor_returning_none_acks(queue_factory) -> None:
    queue = queue_factory("none-ack")
    queue.enqueue({"x": 1})

    def processor(_delivery):
        return None

    with WorkerPool(queue, processor, workers=1, poll_interval=0.01, visibility_timeout=30):
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and queue.stats(cached=False).acked < 1:
            time.sleep(0.01)
    assert queue.stats(cached=False).acked == 1


def test_at_most_once_processor_false_does_not_nack(queue_factory) -> None:
    queue = queue_factory("amo-false")
    queue.enqueue({"x": 1})
    with WorkerPool(
        queue,
        lambda _d: False,
        workers=1,
        delivery_mode=DeliveryMode.AT_MOST_ONCE,
        poll_interval=0.01,
        visibility_timeout=30,
    ):
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and queue.stats(cached=False).delivered < 1:
            time.sleep(0.01)
    stats = queue.stats(cached=False)
    assert stats.delivered == 1
    assert stats.nacked == 0
    assert stats.dead_lettered == 0


def test_worker_pool_claim_budget_exact_limit(queue_factory) -> None:
    queue = queue_factory("budget-pool")
    for index in range(10):
        queue.enqueue({"n": index})
    with WorkerPool(
        queue,
        lambda _d: True,
        workers=4,
        poll_interval=0.01,
        visibility_timeout=30,
        claim_budget=ClaimBudget(3),
    ):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and queue.stats(cached=False).acked < 3:
            time.sleep(0.01)
    assert queue.stats(cached=False).acked == 3
    assert queue.stats(cached=False).current_depth == 7


def test_exhausted_available_message_dlq_on_claim(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("exhausted-claim", clock=clock)
    message_id = queue.enqueue({"x": 1}, max_attempts=1)
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    queue.nack(delivery.receipt_handle, reason="fail")
    again = queue.dequeue(worker_id="w2")
    assert again is None
    assert len(queue.list_dead_letters()) == 1
    assert queue.list_dead_letters()[0].original_message_id == message_id


def test_stats_redelivery_increments_counters(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("stats-redeliver", clock=clock)
    queue.enqueue({"x": 1})
    first = queue.dequeue(visibility_timeout=5, worker_id="w1")
    assert first is not None
    clock.advance(6)
    queue.sweep()
    second = queue.dequeue(worker_id="w2")
    assert second is not None
    stats = queue.stats(cached=False)
    assert stats.delivered >= 2
    assert stats.redelivered >= 1
    assert stats.expired >= 1


def test_concurrent_idempotency_same_key(queue_factory) -> None:
    queue = queue_factory("concurrent-idemp", db_name="concurrent-idemp.db")
    barrier = threading.Barrier(2)
    results: list[int] = []
    lock = threading.Lock()

    def producer() -> None:
        barrier.wait(timeout=2)
        message_id = queue.enqueue({"job": 1}, idempotency_key="shared")
        with lock:
            results.append(message_id)

    threads = [threading.Thread(target=producer) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(set(results)) == 1
