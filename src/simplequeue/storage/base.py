""" Storage backend interface. """

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any

from simplequeue.core.message import DeadLetter, Message, MessageDetails
from simplequeue.core.modes import DeliveryMode
from simplequeue.core.results import AckResult, ClaimResult, LeaseReleaseResult, NackResult
from simplequeue.observability.stats import QueueStatsSnapshot


class StorageBackend(ABC):
    @abstractmethod
    def init_schema(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def enqueue(
        self,
        queue_name: str,
        payload: Any,
        idempotency_key: str | None = None,
        available_at: datetime | None = None,
        max_attempts: int | None = None,
        now: datetime | None = None,
    ) -> int:
        raise NotImplementedError

    @abstractmethod
    def claim_next(
        self,
        queue_name: str,
        delivery_mode: DeliveryMode,
        visibility_timeout: timedelta,
        worker_id: str,
        now: datetime,
    ) -> ClaimResult:
        raise NotImplementedError

    @abstractmethod
    def ack(self, receipt_handle: str, now: datetime) -> AckResult:
        raise NotImplementedError

    @abstractmethod
    def nack(
        self,
        receipt_handle: str,
        now: datetime,
        reason: str | None = None,
    ) -> NackResult:
        raise NotImplementedError

    @abstractmethod
    def release_expired_leases(self, now: datetime) -> LeaseReleaseResult:
        raise NotImplementedError

    @abstractmethod
    def move_exhausted_to_dlq(self, now: datetime) -> int:
        raise NotImplementedError

    @abstractmethod
    def requeue_dead_letter(self, message_id: int, queue_name: str, now: datetime) -> int:
        raise NotImplementedError

    @abstractmethod
    def peek(self, queue_name: str, limit: int = 10, now: datetime | None = None) -> list[Message]:
        raise NotImplementedError

    @abstractmethod
    def inspect(self, message_id: int) -> MessageDetails | None:
        raise NotImplementedError

    @abstractmethod
    def stats(self, queue_name: str, now: datetime | None = None) -> QueueStatsSnapshot:
        raise NotImplementedError

    @abstractmethod
    def list_queues(self) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def list_dead_letters(self, queue_name: str | None = None) -> list[DeadLetter]:
        raise NotImplementedError

    @abstractmethod
    def purge_terminal_messages(
        self,
        queue_name: str,
        older_than: datetime,
        *,
        include_dead_lettered: bool = False,
    ) -> int:
        raise NotImplementedError
