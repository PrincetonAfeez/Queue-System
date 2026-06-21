from __future__ import annotations

import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from simplequeue.cache.stats_cache import StatsCache
from simplequeue.core.delivery import Delivery
from simplequeue.core.modes import DeliveryMode
from simplequeue.core.queue import Queue
from simplequeue.core.results import AckResult, ClaimResult, LeaseReleaseResult, NackResult
from simplequeue.observability.stats import QueueStatsSnapshot
from simplequeue.scheduling.clock import FakeClock
from simplequeue.scheduling.sweeper import BackgroundSweeper, _database_key
from simplequeue.storage.base import StorageBackend
from simplequeue.workers.shutdown import ShutdownMode
from simplequeue.workers.worker import Worker
from simplequeue.workers.worker_pool import WorkerPool


class InMemoryBackend(StorageBackend):
    """Minimal non-SQLite backend for Queue constructor branch coverage."""

    def __init__(self) -> None:
        self.messages: dict[int, dict[str, Any]] = {}
        self._next_id = 1

    def init_schema(self) -> None:
        return None

    def enqueue(
        self,
        queue_name: str,
        payload: Any,
        idempotency_key: str | None = None,
        available_at: datetime | None = None,
        max_attempts: int | None = None,
        now: datetime | None = None,
    ) -> int:
        mid = self._next_id
        self._next_id += 1
        self.messages[mid] = {"queue_name": queue_name, "payload": payload}
        return mid

    def claim_next(
        self,
        queue_name: str,
        delivery_mode: DeliveryMode,
        visibility_timeout: timedelta,
        worker_id: str,
        now: datetime,
    ) -> ClaimResult:
        return ClaimResult(None, False)

    def ack(self, receipt_handle: str, now: datetime) -> AckResult:
        return AckResult(False, None, "invalid", "not_implemented")

    def nack(self, receipt_handle: str, now: datetime, reason: str | None = None) -> NackResult:
        return NackResult(False, None, "invalid", "not_implemented")

    def release_expired_leases(self, now: datetime) -> LeaseReleaseResult:
        return LeaseReleaseResult(0, 0)

    def move_exhausted_to_dlq(self, now: datetime) -> int:
        return 0

    def requeue_dead_letter(self, message_id: int, queue_name: str, now: datetime) -> int:
        return message_id

    def peek(self, queue_name: str, limit: int = 10, now: datetime | None = None) -> list:
        return []

    def inspect(self, message_id: int):
        return None

    def stats(self, queue_name: str, now: datetime | None = None) -> QueueStatsSnapshot:
        return QueueStatsSnapshot(
            queue_name=queue_name,
            enqueued=0,
            delivered=0,
            acked=0,
            nacked=0,
            redelivered=0,
            dead_lettered=0,
            expired=0,
            current_depth=0,
            scheduled_count=0,
            in_flight_count=0,
            recent_worker_ids=0,
            recent_throughput=0.0,
        )

    def list_queues(self) -> list[str]:
        return []

    def list_dead_letters(self, queue_name: str | None = None) -> list:
        return []

    def purge_terminal_messages(
        self,
        queue_name: str,
        older_than: datetime,
        *,
        include_dead_lettered: bool = False,
    ) -> int:
        return 0


def test_queue_uses_standalone_stats_cache_for_non_sqlite_backend() -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = Queue(InMemoryBackend(), "mem", clock=clock)
    assert isinstance(queue.stats_cache, StatsCache)
    snap = queue.stats()
    assert snap.queue_name == "mem"


def test_enqueue_rejects_max_attempts_below_one(queue_factory) -> None:
    queue = queue_factory("validation")
    with pytest.raises(ValueError, match="max_attempts"):
        queue.enqueue({"x": 1}, max_attempts=0)


def test_dequeue_rejects_non_positive_visibility_timeout(queue_factory) -> None:
    queue = queue_factory("validation")
    queue.enqueue({"x": 1})
    with pytest.raises(ValueError, match="visibility_timeout"):
        queue.dequeue(visibility_timeout=0)
    with pytest.raises(ValueError, match="visibility_timeout"):
        queue.dequeue(visibility_timeout=timedelta(seconds=0))


def test_peek_rejects_limit_below_one(queue_factory) -> None:
    queue = queue_factory("validation")
    with pytest.raises(ValueError, match="limit"):
        queue.peek(limit=0)


def test_stats_cache_hit_path(queue_factory) -> None:
    queue = queue_factory("cached-stats")
    queue.enqueue({"x": 1})
    first = queue.stats(cached=True)
    second = queue.stats(cached=True)
    assert first.enqueued == second.enqueued == 1
    assert queue.stats_cache.hits >= 1


def test_sweep_invalidates_entire_stats_cache(queue_factory) -> None:
    queue = queue_factory("sweep-cache")
    queue.enqueue({"x": 1})
    queue.stats(cached=True)
    queue.sweep()
    queue.stats(cached=True)
    assert queue.stats_cache.misses >= 1


def test_worker_rejects_invalid_visibility_timeout(queue_factory) -> None:
    queue = queue_factory("worker-val")
    with pytest.raises(ValueError, match="visibility_timeout"):
        Worker(queue, lambda _d: True, visibility_timeout=0)


def test_worker_rejects_invalid_poll_interval(queue_factory) -> None:
    queue = queue_factory("worker-val")
    with pytest.raises(ValueError, match="poll_interval"):
        Worker(queue, lambda _d: True, poll_interval=0)


def test_worker_start_is_idempotent(queue_factory) -> None:
    queue = queue_factory("worker-idem")
    worker = Worker(queue, lambda _d: True, poll_interval=0.05, visibility_timeout=30)
    worker.start()
    assert worker.is_alive
    worker.start()
    worker.stop()
    worker.join(2)


def test_worker_context_manager(queue_factory) -> None:
    queue = queue_factory("worker-ctx")
    queue.enqueue({"x": 1})
    with Worker(queue, lambda _d: True, poll_interval=0.01, visibility_timeout=30) as worker:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and queue.stats(cached=False).acked < 1:
            time.sleep(0.01)
    assert not worker.is_alive
    assert queue.stats(cached=False).acked == 1


def test_worker_at_most_once_finish_does_not_ack(queue_factory) -> None:
    queue = queue_factory("amo-finish")
    queue.enqueue({"x": 1})
    worker = Worker(
        queue,
        lambda _d: True,
        delivery_mode=DeliveryMode.AT_MOST_ONCE,
        poll_interval=0.01,
        visibility_timeout=30,
    )
    worker.start()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and queue.stats(cached=False).delivered < 1:
        time.sleep(0.01)
    worker.stop()
    worker.join(2)
    stats = queue.stats(cached=False)
    assert stats.delivered == 1
    assert stats.acked == 0


def test_worker_safe_ack_logs_rejected_ack(queue_factory, caplog) -> None:
    queue = queue_factory("safe-ack")
    worker = Worker(queue, lambda _d: True, visibility_timeout=30)
    delivery = Delivery(
        message_id=1,
        receipt_handle="bad-handle",
        queue_name="safe-ack",
        payload={},
        attempt=1,
        delivery_mode=DeliveryMode.AT_LEAST_ONCE,
        leased_at=datetime(2026, 1, 1, tzinfo=UTC),
        lease_expires_at=None,
    )
    with caplog.at_level(logging.WARNING):
        worker._safe_ack(delivery)
    assert any("ack rejected" in record.message for record in caplog.records)


def test_worker_safe_nack_logs_rejected_nack(queue_factory, caplog) -> None:
    queue = queue_factory("safe-nack")
    worker = Worker(queue, lambda _d: True, visibility_timeout=30)
    delivery = Delivery(
        message_id=1,
        receipt_handle="bad-handle",
        queue_name="safe-nack",
        payload={},
        attempt=1,
        delivery_mode=DeliveryMode.AT_LEAST_ONCE,
        leased_at=datetime(2026, 1, 1, tzinfo=UTC),
        lease_expires_at=None,
    )
    with caplog.at_level(logging.WARNING):
        worker._safe_nack(delivery, "reason")
    assert any("nack rejected" in record.message for record in caplog.records)


def test_worker_pool_rejects_invalid_workers(queue_factory) -> None:
    queue = queue_factory("pool-val")
    with pytest.raises(ValueError, match="workers"):
        WorkerPool(queue, lambda _d: True, workers=0)


def test_worker_pool_rejects_invalid_poll_interval(queue_factory) -> None:
    queue = queue_factory("pool-val")
    with pytest.raises(ValueError, match="poll_interval"):
        WorkerPool(queue, lambda _d: True, poll_interval=0)


def test_worker_pool_context_manager(queue_factory) -> None:
    queue = queue_factory("pool-ctx")
    queue.enqueue({"x": 1})
    with WorkerPool(queue, lambda _d: True, workers=1, poll_interval=0.01, visibility_timeout=30) as pool:
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and queue.stats(cached=False).acked < 1:
            time.sleep(0.01)
    assert pool.all_stopped


def test_worker_pool_join_without_timeout(queue_factory) -> None:
    queue = queue_factory("pool-join-none")
    pool = WorkerPool(queue, lambda _d: True, workers=1, poll_interval=0.01, visibility_timeout=30)
    pool.start()
    pool.stop()
    assert pool.join(None) is True


def test_background_sweeper_rejects_non_positive_interval(queue_factory) -> None:
    queue = queue_factory("sweeper-val")
    with pytest.raises(ValueError, match="interval"):
        BackgroundSweeper(queue, interval=0)


def test_database_key_non_sqlite_backend(queue_factory) -> None:
    queue = Queue(InMemoryBackend(), "mem")
    key = _database_key(queue)
    assert key == str(id(queue.backend))


def test_shutdown_mode_parse_aliases() -> None:
    assert ShutdownMode.parse("finish_current") is ShutdownMode.FINISH_CURRENT
    assert ShutdownMode.parse(ShutdownMode.NACK_CURRENT) is ShutdownMode.NACK_CURRENT


def test_delivery_mode_parse_strips_and_normalizes() -> None:
    assert DeliveryMode.parse("  AT_LEAST_ONCE  ") is DeliveryMode.AT_LEAST_ONCE


def test_receipt_handles_are_unique() -> None:
    from simplequeue.reliability.receipt_handles import new_receipt_handle

    handles = {new_receipt_handle() for _ in range(20)}
    assert len(handles) == 20


def test_dlq_reason_constants() -> None:
    from simplequeue.reliability.dlq import DLQ_REASON_LEASE_EXPIRED, DLQ_REASON_MAX_ATTEMPTS

    assert "max attempts" in DLQ_REASON_MAX_ATTEMPTS
    assert "lease expired" in DLQ_REASON_LEASE_EXPIRED
