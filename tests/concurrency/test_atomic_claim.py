"""Atomic claim concurrency tests."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Lock


def test_many_consumers_do_not_double_claim_messages(queue_factory) -> None:
    queue = queue_factory("contention")
    total = 150
    for index in range(total):
        queue.enqueue({"job": index})

    seen: list[int] = []
    lock = Lock()

    def consume(worker_index: int) -> None:
        while True:
            delivery = queue.dequeue(visibility_timeout=30, worker_id=f"worker-{worker_index}")
            if delivery is None:
                return
            with lock:
                seen.append(delivery.message_id)
            assert queue.ack(delivery.receipt_handle).success

    with ThreadPoolExecutor(max_workers=16) as executor:
        list(executor.map(consume, range(16)))

    assert len(seen) == total
    assert len(set(seen)) == total
    stats = queue.stats(cached=False)
    assert stats.acked == total
    assert stats.in_flight_count == 0
    assert stats.current_depth == 0


def test_many_producers_can_enqueue_without_losing_messages(queue_factory) -> None:
    queue = queue_factory("producers")

    def produce(worker_index: int) -> list[int]:
        return [queue.enqueue({"producer": worker_index, "n": n}) for n in range(20)]

    with ThreadPoolExecutor(max_workers=10) as executor:
        nested = list(executor.map(produce, range(10)))

    ids = [message_id for group in nested for message_id in group]
    assert len(ids) == 200
    assert len(set(ids)) == 200
    assert queue.stats(cached=False).enqueued == 200
