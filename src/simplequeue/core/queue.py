""" Queue API. """

from __future__ import annotations

import logging
import math
import uuid
from datetime import datetime, timedelta
from typing import Any

from simplequeue.cache.stats_cache import StatsCache, shared_stats_cache
from simplequeue.core.delivery import Delivery
from simplequeue.core.message import DeadLetter, Message, MessageDetails
from simplequeue.core.modes import DeliveryMode
from simplequeue.core.results import AckResult, NackResult
from simplequeue.core.validation import require_finite_positive, validate_queue_name
from simplequeue.defaults import DEFAULT_CACHE_TTL, DEFAULT_VISIBILITY_TIMEOUT
from simplequeue.observability import events
from simplequeue.observability.logging import get_logger, log_event
from simplequeue.observability.stats import QueueStatsSnapshot
from simplequeue.scheduling.clock import Clock, SystemClock
from simplequeue.storage.base import StorageBackend
from simplequeue.storage.sqlite_backend import SQLiteBackend


class Queue:
    """Stable public queue API.

    Queue owns semantics and delegates persistence to the backend. CLI and
    workers call this API; backend-specific SQL remains behind StorageBackend.
    """

    def __init__(
        self,
        backend: StorageBackend,
        queue_name: str,
        *,
        clock: Clock | None = None,
        stats_cache: StatsCache | None = None,
        cache_ttl_seconds: float = DEFAULT_CACHE_TTL,
        logger: logging.Logger | None = None,
    ) -> None:
        self.backend = backend
        self.queue_name = validate_queue_name(queue_name)
        self.clock = clock or SystemClock()
        if stats_cache is None:
            require_finite_positive(cache_ttl_seconds, field="cache_ttl_seconds")
            if isinstance(backend, SQLiteBackend):
                stats_cache = shared_stats_cache(
                    backend.db_path,
                    ttl_seconds=cache_ttl_seconds,
                    clock=self.clock,
                )
            else:
                stats_cache = StatsCache(ttl_seconds=cache_ttl_seconds, clock=self.clock)
        self.stats_cache = stats_cache
        self.logger = logger or get_logger()
        self._default_worker_id = f"queue-{uuid.uuid4().hex[:8]}"

    def init_schema(self) -> None:
        self.backend.init_schema()

    def enqueue(
        self,
        payload: Any,
        *,
        idempotency_key: str | None = None,
        available_at: datetime | None = None,
        max_attempts: int | None = None,
    ) -> int:
        if max_attempts is not None and max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        now = self.clock.now()
        message_id = self.backend.enqueue(
            self.queue_name,
            payload,
            idempotency_key=idempotency_key,
            available_at=available_at or now,
            max_attempts=max_attempts,
            now=now,
        )
        self.stats_cache.invalidate(self.queue_name)
        log_event(
            self.logger,
            events.ENQUEUE,
            queue_name=self.queue_name,
            message_id=message_id,
            idempotency_key=idempotency_key,
        )
        return message_id

    def dequeue(
        self,
        *,
        delivery_mode: DeliveryMode | str = DeliveryMode.AT_LEAST_ONCE,
        visibility_timeout: float | timedelta = DEFAULT_VISIBILITY_TIMEOUT,
        worker_id: str | None = None,
    ) -> Delivery | None:
        if isinstance(visibility_timeout, timedelta):
            try:
                timeout_seconds = visibility_timeout.total_seconds()
            except OverflowError as exc:
                raise ValueError("visibility_timeout must be a finite number > 0") from exc
            timeout = visibility_timeout
        else:
            timeout_seconds = visibility_timeout
            if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
                raise ValueError("visibility_timeout must be a finite number > 0")
            timeout = timedelta(seconds=visibility_timeout)
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("visibility_timeout must be a finite number > 0")
        mode = DeliveryMode.parse(delivery_mode)
        claim = self.backend.claim_next(
            self.queue_name,
            mode,
            timeout,
            worker_id or self._default_worker_id,
            self.clock.now(),
        )
        if claim.mutated:
            # claim_next may release expired leases across all queues on this database.
            self.stats_cache.invalidate()
        if claim.delivery is not None:
            log_event(
                self.logger,
                events.LEASE if mode is DeliveryMode.AT_LEAST_ONCE else events.DELETE_DELIVERY,
                queue_name=self.queue_name,
                message_id=claim.delivery.message_id,
                receipt_handle=claim.delivery.receipt_handle[:12],
                attempt=claim.delivery.attempt,
                delivery_mode=mode.value,
            )
        return claim.delivery

    def ack(self, receipt_handle: str) -> AckResult:
        result = self.backend.ack(receipt_handle, self.clock.now())
        if result.success:
            self.stats_cache.invalidate(self.queue_name)
        log_event(
            self.logger,
            events.ACK,
            queue_name=self.queue_name,
            receipt_handle=receipt_handle[:12],
            success=result.success,
            reason=result.reason,
            message_id=result.message_id,
        )
        return result

    def nack(self, receipt_handle: str, reason: str | None = None) -> NackResult:
        result = self.backend.nack(receipt_handle, self.clock.now(), reason=reason)
        if result.success:
            self.stats_cache.invalidate(self.queue_name)
        log_event(
            self.logger,
            events.NACK,
            queue_name=self.queue_name,
            receipt_handle=receipt_handle[:12],
            success=result.success,
            reason=result.reason,
            moved_to_dlq=result.moved_to_dlq,
            message_id=result.message_id,
        )
        return result

    def sweep(self) -> dict[str, int]:
        """Reclaim expired leases and DLQ exhausted messages.

        Per the StorageBackend contract this maintenance is database-wide, not
        limited to this queue's name, so the whole stats cache is invalidated.
        """
        now = self.clock.now()
        lease_release = self.backend.release_expired_leases(now)
        exhausted_dlq = self.backend.move_exhausted_to_dlq(now)
        expired = lease_release.redelivered
        dead_lettered = lease_release.dead_lettered + exhausted_dlq
        if expired or dead_lettered:
            # release_expired_leases / move_exhausted_to_dlq act across all
            # queues, so clear the whole cache rather than just this queue's entry.
            self.stats_cache.invalidate()
        log_event(
            self.logger,
            events.SWEEP,
            queue_name=self.queue_name,
            expired=expired,
            dead_lettered=dead_lettered,
        )
        return {"expired": expired, "dead_lettered": dead_lettered}

    def requeue_dead_letter(self, message_id: int) -> int:
        requeued_id = self.backend.requeue_dead_letter(
            message_id, self.queue_name, self.clock.now()
        )
        self.stats_cache.invalidate(self.queue_name)
        log_event(
            self.logger,
            events.REQUEUE,
            queue_name=self.queue_name,
            message_id=message_id,
            requeued_id=requeued_id,
        )
        return requeued_id

    def stats(self, *, cached: bool = True) -> QueueStatsSnapshot:
        if cached:
            cached_snapshot = self.stats_cache.get(self.queue_name)
            if cached_snapshot is not None:
                return cached_snapshot
        snapshot = self.backend.stats(self.queue_name, self.clock.now())
        if cached:
            self.stats_cache.set(self.queue_name, snapshot)
        return snapshot

    def peek(self, limit: int = 10) -> list[Message]:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        return self.backend.peek(self.queue_name, limit=limit, now=self.clock.now())

    def inspect(self, message_id: int) -> MessageDetails | None:
        return self.backend.inspect(message_id)

    def list_queues(self) -> list[str]:
        return self.backend.list_queues()

    def list_dead_letters(
        self,
        queue_name: str | None = None,
        *,
        all_queues: bool = False,
    ) -> list[DeadLetter]:
        if all_queues and queue_name is not None:
            raise ValueError("pass only one of queue_name or all_queues=True")
        if all_queues:
            return self.backend.list_dead_letters(None)
        target = self.queue_name if queue_name is None else validate_queue_name(queue_name)
        return self.backend.list_dead_letters(target)

    def purge_terminal(
        self,
        *,
        older_than: datetime | None = None,
        queue_name: str | None = None,
        include_dead_lettered: bool = False,
        all_queues: bool = False,
    ) -> int:
        """Delete terminal rows with ``updated_at`` (or ``dead_lettered_at``) on or before the cutoff.

        When ``older_than`` is omitted, rows older than
        ``DEFAULT_PURGE_RETENTION_DAYS`` (7 days) relative to the queue clock are
        removed. Does not purge anything when the retention window is empty.

        Pass ``all_queues=True`` to purge every queue in the database file.
        """
        from simplequeue.defaults import DEFAULT_PURGE_RETENTION_DAYS

        if all_queues and queue_name is not None:
            raise ValueError("pass only one of queue_name or all_queues=True")
        if older_than is None:
            older_than = self.clock.now() - timedelta(days=DEFAULT_PURGE_RETENTION_DAYS)
        if all_queues:
            removed_total = 0
            for name in self.list_queues():
                removed_total += self._purge_terminal_queue(
                    name,
                    older_than,
                    include_dead_lettered=include_dead_lettered,
                )
            return removed_total
        target = self.queue_name if queue_name is None else validate_queue_name(queue_name)
        return self._purge_terminal_queue(
            target,
            older_than,
            include_dead_lettered=include_dead_lettered,
        )

    def _purge_terminal_queue(
        self,
        queue_name: str,
        older_than: datetime,
        *,
        include_dead_lettered: bool,
    ) -> int:
        removed = self.backend.purge_terminal_messages(
            queue_name,
            older_than,
            include_dead_lettered=include_dead_lettered,
        )
        if removed:
            self.stats_cache.invalidate(queue_name)
        log_event(
            self.logger,
            events.PURGE,
            queue_name=queue_name,
            removed=removed,
            older_than=older_than.isoformat(),
            include_dead_lettered=include_dead_lettered,
        )
        return removed
