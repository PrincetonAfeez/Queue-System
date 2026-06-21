from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from simplequeue.core.exceptions import DeadLetterNotFound, IdempotencyConflict
from simplequeue.core.modes import DeliveryMode
from simplequeue.core.states import LEGAL_TRANSITIONS, MessageStatus
from simplequeue.scheduling.clock import FakeClock


def test_scheduled_message_not_available_until_future(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("scheduled", clock=clock)
    future = clock.now() + timedelta(hours=1)
    queue.enqueue({"x": 1}, available_at=future)
    assert queue.dequeue(worker_id="w") is None
    assert queue.stats(cached=False).scheduled_count == 1
    clock.advance(3601)
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    queue.ack(delivery.receipt_handle)


def test_peek_respects_limit_and_order(queue_factory) -> None:
    queue = queue_factory("peek-order")
    ids = [queue.enqueue({"n": index}) for index in range(5)]
    messages = queue.peek(limit=3)
    assert len(messages) == 3
    assert [message.id for message in messages] == ids[:3]


def test_list_queues_includes_event_only_queue(queue_factory, tmp_path) -> None:
    queue = queue_factory("alpha", db_name="list-q.db")
    queue.enqueue({"x": 1})
    other = queue_factory("beta", db_name="list-q.db")
    other.enqueue({"y": 2})
    names = queue.list_queues()
    assert "alpha" in names
    assert "beta" in names


def test_requeue_dead_letter_not_found(queue_factory) -> None:
    queue = queue_factory("requeue-miss")
    with pytest.raises(DeadLetterNotFound):
        queue.requeue_dead_letter(999)


def test_requeue_dead_letter_idempotency_conflict(queue_factory) -> None:
    queue = queue_factory("requeue-conflict")
    queue.enqueue({"a": 1}, idempotency_key="k", max_attempts=1)
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    queue.nack(delivery.receipt_handle, reason="fail")
    dead_id = queue.list_dead_letters()[0].original_message_id
    queue.enqueue({"b": 2}, idempotency_key="k")
    with pytest.raises(IdempotencyConflict, match="cannot requeue"):
        queue.requeue_dead_letter(dead_id)


def test_nack_on_at_most_once_deleted_message(queue_factory) -> None:
    queue = queue_factory("amo-nack")
    queue.enqueue({"x": 1})
    delivery = queue.dequeue(delivery_mode=DeliveryMode.AT_MOST_ONCE, worker_id="w")
    assert delivery is not None
    result = queue.nack(delivery.receipt_handle, reason="late")
    assert not result.success
    assert result.reason == "not_leased"


def test_ack_stale_receipt_after_successful_ack(queue_factory) -> None:
    queue = queue_factory("double-ack")
    queue.enqueue({"x": 1})
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    assert queue.ack(delivery.receipt_handle).success
    again = queue.ack(delivery.receipt_handle)
    assert not again.success
    assert again.reason in {"receipt_handle_not_found", "not_leased", "stale_receipt"}


def test_redelivery_increments_attempt_counter(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("attempts", clock=clock)
    queue.enqueue({"x": 1})
    first = queue.dequeue(visibility_timeout=5, worker_id="w1")
    assert first is not None
    assert first.attempt == 1
    clock.advance(6)
    queue.sweep()
    second = queue.dequeue(visibility_timeout=5, worker_id="w2")
    assert second is not None
    assert second.attempt == 2


def test_inspect_includes_event_history(queue_factory) -> None:
    queue = queue_factory("inspect-events")
    message_id = queue.enqueue({"x": 1})
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    queue.ack(delivery.receipt_handle)
    details = queue.inspect(message_id)
    assert details is not None
    event_types = {event.event_type for event in details.events}
    assert "enqueue" in event_types
    assert "lease" in event_types
    assert "ack" in event_types


def test_inspect_includes_dead_letter_record(queue_factory) -> None:
    queue = queue_factory("inspect-dlq")
    message_id = queue.enqueue({"x": 1}, max_attempts=1)
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    queue.nack(delivery.receipt_handle, reason="fail")
    details = queue.inspect(message_id)
    assert details is not None
    assert details.dead_letter is not None
    assert details.dead_letter.original_message_id == message_id


def test_legal_transitions_table_is_documented_set() -> None:
    assert (MessageStatus.AVAILABLE, MessageStatus.LEASED) in LEGAL_TRANSITIONS
    assert (MessageStatus.LEASED, MessageStatus.ACKED) in LEGAL_TRANSITIONS
    assert len(LEGAL_TRANSITIONS) == 7


def test_delivery_mode_parse_accepts_enum_instance() -> None:
    assert DeliveryMode.parse(DeliveryMode.AT_MOST_ONCE) is DeliveryMode.AT_MOST_ONCE


def test_stats_recent_throughput_after_ack(queue_factory) -> None:
    queue = queue_factory("throughput")
    queue.enqueue({"x": 1})
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    queue.ack(delivery.receipt_handle)
    stats = queue.stats(cached=False)
    assert stats.recent_throughput >= 0
