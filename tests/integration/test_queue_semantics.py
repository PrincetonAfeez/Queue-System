from __future__ import annotations

from datetime import UTC, datetime

from simplequeue.core.modes import DeliveryMode
from simplequeue.core.states import MessageStatus
from simplequeue.scheduling.clock import FakeClock


def test_enqueue_dequeue_ack_round_trip(queue_factory) -> None:
    queue = queue_factory("emails")
    message_id = queue.enqueue({"to": "user@example.com"})
    delivery = queue.dequeue(worker_id="worker-a")
    assert delivery is not None
    assert delivery.message_id == message_id
    assert delivery.payload == {"to": "user@example.com"}

    result = queue.ack(delivery.receipt_handle)
    assert result.success
    assert queue.dequeue(worker_id="worker-b") is None

    details = queue.inspect(message_id)
    assert details is not None
    assert details.message.status is MessageStatus.ACKED


def test_idempotency_key_returns_existing_message(queue_factory) -> None:
    queue = queue_factory("jobs")
    first = queue.enqueue({"job": 1}, idempotency_key="same")
    second = queue.enqueue({"job": 1}, idempotency_key="same")
    assert first == second
    assert queue.stats(cached=False).enqueued == 1


def test_at_most_once_deletes_at_delivery_time(queue_factory) -> None:
    queue = queue_factory("loss")
    message_id = queue.enqueue({"low_value": True})
    delivery = queue.dequeue(delivery_mode=DeliveryMode.AT_MOST_ONCE, worker_id="crashy")
    assert delivery is not None
    assert queue.dequeue(delivery_mode=DeliveryMode.AT_MOST_ONCE, worker_id="new-worker") is None
    details = queue.inspect(message_id)
    assert details is not None
    assert details.message.status is MessageStatus.DELETED


def test_at_least_once_redelivers_after_visibility_timeout(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("redelivery", clock=clock)
    message_id = queue.enqueue({"task": "retry"})
    first = queue.dequeue(visibility_timeout=5, worker_id="first")
    assert first is not None

    clock.advance(6)
    expired_ack = queue.ack(first.receipt_handle)
    assert not expired_ack.success
    assert expired_ack.reason == "lease_expired"
    sweep = queue.sweep()
    assert sweep["expired"] == 1

    second = queue.dequeue(visibility_timeout=5, worker_id="second")
    assert second is not None
    assert second.message_id == message_id
    assert second.receipt_handle != first.receipt_handle
    assert second.attempt == 2
    assert queue.ack(second.receipt_handle).success


def test_stale_receipt_cannot_ack_or_nack_new_lease(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("receipts", clock=clock)
    queue.enqueue({"task": "receipt"})
    old = queue.dequeue(visibility_timeout=5, worker_id="old")
    assert old is not None
    clock.advance(6)
    queue.sweep()
    new = queue.dequeue(visibility_timeout=5, worker_id="new")
    assert new is not None

    stale_ack = queue.ack(old.receipt_handle)
    stale_nack = queue.nack(old.receipt_handle, reason="too late")
    assert not stale_ack.success
    assert stale_ack.reason in ("stale_receipt", "receipt_handle_not_found")
    assert not stale_nack.success
    assert stale_nack.reason in ("stale_receipt", "receipt_handle_not_found", "lease_expired")
    assert queue.ack(new.receipt_handle).success


def test_nack_retries_until_dlq_and_requeue(queue_factory) -> None:
    queue = queue_factory("retry")
    message_id = queue.enqueue({"poison": True}, max_attempts=2)
    first = queue.dequeue(worker_id="w1")
    assert first is not None
    retry = queue.nack(first.receipt_handle, reason="fail once")
    assert retry.success
    assert not retry.moved_to_dlq

    second = queue.dequeue(worker_id="w2")
    assert second is not None
    dead = queue.nack(second.receipt_handle, reason="fail twice")
    assert dead.success
    assert dead.moved_to_dlq
    assert queue.dequeue(worker_id="w3") is None
    dead_letters = queue.list_dead_letters()
    assert len(dead_letters) == 1
    assert dead_letters[0].payload == {"poison": True}

    requeued_id = queue.requeue_dead_letter(message_id)
    assert requeued_id == message_id
    assert len(queue.list_dead_letters()) == 0
    redelivery = queue.dequeue(worker_id="w4")
    assert redelivery is not None
    assert redelivery.message_id == message_id


def test_exhausted_expired_lease_moves_to_dlq(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("expiry-dlq", clock=clock)
    queue.enqueue({"expires": True}, max_attempts=1)
    delivery = queue.dequeue(visibility_timeout=5, worker_id="w1")
    assert delivery is not None
    clock.advance(6)
    queue.sweep()
    assert queue.dequeue(worker_id="w2") is None
    assert len(queue.list_dead_letters()) == 1


def test_messages_survive_backend_restart(tmp_path) -> None:
    from simplequeue.core.queue import Queue
    from simplequeue.storage.sqlite_backend import SQLiteBackend

    db_path = tmp_path / "durable.db"
    first_queue = Queue(SQLiteBackend(db_path), "durable")
    first_queue.init_schema()
    message_id = first_queue.enqueue({"persist": True})

    second_queue = Queue(SQLiteBackend(db_path), "durable")
    delivery = second_queue.dequeue(worker_id="after-restart")
    assert delivery is not None
    assert delivery.message_id == message_id


def test_leased_message_recovered_after_restart(tmp_path) -> None:
    """A persisted lease deadline must remain meaningful after a process restart.

    The lease is taken by one backend instance; a fresh instance on the same
    file, with its clock past the stored deadline, reclaims and redelivers it.
    """
    from datetime import timedelta

    from simplequeue.core.queue import Queue
    from simplequeue.scheduling.clock import FakeClock
    from simplequeue.storage.sqlite_backend import SQLiteBackend

    db_path = tmp_path / "restart-lease.db"
    t0 = datetime(2026, 1, 1, tzinfo=UTC)

    first = Queue(SQLiteBackend(db_path), "durable", clock=FakeClock.starting_at(t0))
    first.init_schema()
    message_id = first.enqueue({"persist": True})
    lease = first.dequeue(visibility_timeout=5, worker_id="before-restart")
    assert lease is not None  # message is now leased with a durable deadline

    later = FakeClock.starting_at(t0 + timedelta(seconds=10))  # restart, clock past deadline
    second = Queue(SQLiteBackend(db_path), "durable", clock=later)
    recovered = second.dequeue(visibility_timeout=5, worker_id="after-restart")
    assert recovered is not None
    assert recovered.message_id == message_id
    assert recovered.attempt == 2  # redelivered after the persisted lease expired


def test_stats_cache_invalidates_after_mutation(queue_factory) -> None:
    queue = queue_factory("stats")
    assert queue.stats(cached=True).enqueued == 0
    assert queue.stats_cache.misses == 1
    assert queue.stats(cached=True).enqueued == 0
    assert queue.stats_cache.hits == 1
    queue.enqueue({"fresh": True})
    assert queue.stats(cached=True).enqueued == 1
