"""Fixes tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from simplequeue.core.exceptions import DeadLetterNotFound, IdempotencyConflict
from simplequeue.scheduling.clock import FakeClock
from simplequeue.workers.shutdown import ShutdownMode


def test_requeue_dead_letter_is_scoped_to_queue_name(queue_factory) -> None:
    queue_a = queue_factory("queue-a")
    queue_b = queue_factory("queue-b")
    message_id = queue_a.enqueue({"x": 1}, max_attempts=1)
    delivery = queue_a.dequeue(worker_id="w")
    assert delivery is not None
    queue_a.nack(delivery.receipt_handle, reason="fail")
    assert len(queue_a.list_dead_letters()) == 1

    with pytest.raises(DeadLetterNotFound):
        queue_b.requeue_dead_letter(message_id)


def test_stats_cache_invalidates_when_claim_maintenance_runs(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("maint-cache", clock=clock)
    queue.enqueue({"x": 1}, max_attempts=2)
    assert queue.dequeue(visibility_timeout=5, worker_id="w1") is not None
    clock.advance(6)
    assert queue.dequeue(visibility_timeout=5, worker_id="w2") is not None
    assert queue.stats(cached=True).in_flight_count == 1

    clock.advance(6)
    assert queue.dequeue(worker_id="w3") is None
    assert queue.stats(cached=True).dead_lettered == 1
    assert queue.stats(cached=True).in_flight_count == 0


def test_peek_returns_only_claimable_messages(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("peek", clock=clock)
    queue.enqueue({"ready": True})
    future_at = clock.now() + timedelta(seconds=60)
    queue.enqueue({"later": True}, available_at=future_at)
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    queue.ack(delivery.receipt_handle)

    peeked = queue.peek(limit=10)
    assert len(peeked) == 0


def test_current_depth_excludes_future_scheduled_messages(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("depth", clock=clock)
    queue.enqueue({"now": True})
    queue.enqueue({"later": True}, available_at=clock.now() + timedelta(seconds=30))
    assert queue.stats(cached=False).current_depth == 1


def test_peek_rejects_non_positive_limit(queue_factory) -> None:
    queue = queue_factory("peek-limit")
    with pytest.raises(ValueError):
        queue.peek(limit=0)


def test_dequeue_rejects_non_positive_visibility_timeout(queue_factory) -> None:
    queue = queue_factory("vt")
    queue.enqueue({"x": 1})
    with pytest.raises(ValueError):
        queue.dequeue(visibility_timeout=0)


def test_list_queues_discovers_multiple_queue_names(queue_factory) -> None:
    queue_a = queue_factory("alpha")
    queue_b = queue_factory("beta")
    queue_a.enqueue({"a": 1})
    queue_b.enqueue({"b": 1})
    names = queue_a.list_queues()
    assert "alpha" in names
    assert "beta" in names


def test_nack_retry_increments_redeliveries_counter(queue_factory) -> None:
    queue = queue_factory("redeliveries")
    queue.enqueue({"x": 1}, max_attempts=3)
    first = queue.dequeue(worker_id="w1")
    assert first is not None
    queue.nack(first.receipt_handle, reason="retry")
    details = queue.inspect(first.message_id)
    assert details is not None
    assert details.message.redeliveries == 1


def test_shutdown_mode_parse_accepts_underscore_aliases() -> None:
    assert ShutdownMode.parse("nack_current") is ShutdownMode.NACK_CURRENT


def test_load_config_rejects_out_of_range_values(tmp_path) -> None:
    from simplequeue.config import load_config

    path = tmp_path / "bad.json"
    path.write_text('{"worker_count": 0}', encoding="utf-8")
    with pytest.raises(ValueError, match="worker_count"):
        load_config(str(path), command="consume")


def test_requeue_dead_letter_rejects_live_idempotency_conflict(queue_factory) -> None:
    queue = queue_factory("idem-requeue", db_name="shared.db")
    other = queue_factory("idem-requeue", db_name="shared.db")
    message_id = queue.enqueue({"x": 1}, idempotency_key="k", max_attempts=1)
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    queue.nack(delivery.receipt_handle, reason="fail")
    other.enqueue({"y": 2}, idempotency_key="k")
    with pytest.raises(IdempotencyConflict):
        queue.requeue_dead_letter(message_id)


def test_nack_does_not_report_dlq_when_move_fails(queue_factory, monkeypatch) -> None:
    queue = queue_factory("nack-dlq-fail")
    queue.enqueue({"x": 1}, max_attempts=1)
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None

    monkeypatch.setattr(queue.backend, "_move_to_dlq_locked", lambda *args, **kwargs: False)

    result = queue.nack(delivery.receipt_handle, reason="fail")
    assert not result.success
    assert not result.moved_to_dlq
    assert result.reason == "stale_receipt"


def test_requeue_resets_redeliveries_counter(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("requeue-redel", clock=clock)
    message_id = queue.enqueue({"x": 1}, max_attempts=2)
    first = queue.dequeue(visibility_timeout=5, worker_id="w1")
    assert first is not None
    clock.advance(6)
    queue.dequeue(visibility_timeout=5, worker_id="w2")
    clock.advance(6)
    queue.dequeue(worker_id="w3")
    assert queue.stats(cached=False).dead_lettered == 1

    queue.requeue_dead_letter(message_id)
    details = queue.inspect(message_id)
    assert details is not None
    assert details.message.redeliveries == 0


def test_sqlite_backend_rejects_invalid_default_max_attempts(tmp_path) -> None:
    from simplequeue.storage.sqlite_backend import SQLiteBackend

    with pytest.raises(ValueError, match="default_max_attempts"):
        SQLiteBackend(tmp_path / "bad.db", default_max_attempts=0)


def test_worker_rejects_non_positive_poll_interval(queue_factory) -> None:
    from simplequeue.workers.worker import Worker

    queue = queue_factory("poll")
    with pytest.raises(ValueError, match="poll_interval"):
        Worker(queue, lambda d: True, poll_interval=0)


def test_sweep_expired_count_is_accurate(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("sweep-count", clock=clock)
    queue.enqueue({"x": 1})
    delivery = queue.dequeue(visibility_timeout=5, worker_id="w")
    assert delivery is not None
    clock.advance(6)
    first = queue.sweep()
    assert first["expired"] == 1
    assert first["dead_lettered"] == 0
    second = queue.sweep()
    assert second["expired"] == 0


def test_sweep_counts_lease_expiry_dlq_under_dead_lettered(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("sweep-dlq-metric", clock=clock)
    queue.enqueue({"x": 1}, max_attempts=1)
    assert queue.dequeue(visibility_timeout=5, worker_id="w") is not None
    clock.advance(6)
    result = queue.sweep()
    assert result["expired"] == 0
    assert result["dead_lettered"] == 1


def test_shared_stats_cache_invalidates_across_queue_instances(queue_factory) -> None:
    from simplequeue.cache.stats_cache import shared_stats_cache

    queue_a = queue_factory("jobs", db_name="shared-cache.db")
    cache = shared_stats_cache(queue_a.backend.db_path)
    queue_a.stats_cache = cache
    queue_b = queue_factory("jobs", db_name="shared-cache.db", stats_cache=cache)
    assert queue_a.stats(cached=True).enqueued == 0
    queue_b.enqueue({"a": 1})
    assert queue_a.stats(cached=True).enqueued == 1


def test_scheduled_message_becomes_claimable_after_clock_advance(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("scheduled", clock=clock)
    future_at = clock.now() + timedelta(seconds=30)
    queue.enqueue({"later": True}, available_at=future_at)
    assert queue.peek(limit=10) == []
    assert queue.stats(cached=False).scheduled_count == 1
    clock.advance(30)
    peeked = queue.peek(limit=10)
    assert len(peeked) == 1
    assert queue.dequeue(worker_id="w") is not None


def test_stats_lease_expiry_dlq_not_double_counted(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("stats-dlq", clock=clock)
    queue.enqueue({"x": 1}, max_attempts=1)
    assert queue.dequeue(visibility_timeout=5, worker_id="w") is not None
    clock.advance(6)
    queue.sweep()
    stats = queue.stats(cached=False)
    assert stats.dead_lettered == 1
    assert stats.expired == 0


def test_nack_on_expired_lease_redelivers(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("nack-expired", clock=clock)
    queue.enqueue({"x": 1}, max_attempts=3)
    delivery = queue.dequeue(visibility_timeout=5, worker_id="w")
    assert delivery is not None
    clock.advance(6)
    result = queue.nack(delivery.receipt_handle, reason="late nack")
    assert result.success
    assert result.moved_to_dlq is False
    again = queue.dequeue(worker_id="w2")
    assert again is not None
    assert again.message_id == delivery.message_id
    assert again.attempt == 2


def test_idempotency_key_reusable_after_terminal_message(queue_factory) -> None:
    queue = queue_factory("idemp-reuse")
    first = queue.enqueue({"job": 1}, idempotency_key="k")
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    queue.ack(delivery.receipt_handle)
    second = queue.enqueue({"job": 2}, idempotency_key="k")
    assert second != first
