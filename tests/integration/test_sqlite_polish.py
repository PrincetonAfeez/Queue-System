from __future__ import annotations

import threading
from contextlib import closing
from datetime import UTC, datetime

from simplequeue.core.modes import DeliveryMode
from simplequeue.core.states import MessageStatus
from simplequeue.scheduling.clock import FakeClock
from simplequeue.storage.sqlite_backend import SQLiteBackend


def test_sweep_move_exhausted_to_dlq_for_available_rows(queue_factory) -> None:
    """Available rows with attempts >= max_attempts are moved by ``move_exhausted_to_dlq``."""
    queue = queue_factory("sweep-exhausted")
    message_id = queue.enqueue({"x": 1}, max_attempts=1)
    backend = queue.backend
    assert isinstance(backend, SQLiteBackend)
    with closing(backend._connect()) as conn:  # noqa: SLF001
        conn.execute(
            "UPDATE messages SET attempts = 1 WHERE id = ?",
            (message_id,),
        )
        conn.commit()
    assert queue.sweep()["dead_lettered"] == 1
    assert queue.dequeue() is None
    dead = queue.list_dead_letters()
    assert len(dead) == 1
    assert dead[0].original_message_id == message_id


def test_concurrent_ack_surfaces_stale_receipt(queue_factory) -> None:
    queue = queue_factory("ack-race")
    queue.enqueue({"x": 1})
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    results: list = []
    barrier = threading.Barrier(2)

    def contender() -> None:
        barrier.wait()
        results.append(queue.ack(delivery.receipt_handle))

    threads = [threading.Thread(target=contender) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sum(1 for result in results if result.success) == 1
    failures = [result for result in results if not result.success]
    assert len(failures) == 1
    assert failures[0].reason in ("stale_receipt", "not_leased", "receipt_handle_not_found")


def test_nack_on_final_attempt_moves_to_dlq(queue_factory) -> None:
    queue = queue_factory("nack-dlq")
    queue.enqueue({"poison": True}, max_attempts=1)
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    result = queue.nack(delivery.receipt_handle, reason="fail")
    assert result.success
    assert result.moved_to_dlq
    details = queue.inspect(delivery.message_id)
    assert details is not None
    assert details.message.status is MessageStatus.DEAD_LETTERED


def test_at_most_once_claim_deletes_message(queue_factory) -> None:
    queue = queue_factory("amo-claim")
    message_id = queue.enqueue({"x": 1})
    delivery = queue.dequeue(delivery_mode=DeliveryMode.AT_MOST_ONCE, worker_id="w")
    assert delivery is not None
    details = queue.inspect(message_id)
    assert details is not None
    assert details.message.status is MessageStatus.DELETED


def test_purge_include_dead_lettered_removes_dlq_rows(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("purge-dlq", clock=clock)
    queue.enqueue({"x": 1}, max_attempts=1)
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    queue.nack(delivery.receipt_handle, reason="fail")
    clock.advance(8 * 86400)
    removed = queue.purge_terminal(
        older_than=clock.now(),
        include_dead_lettered=True,
    )
    assert removed == 1
    assert queue.list_dead_letters() == []
