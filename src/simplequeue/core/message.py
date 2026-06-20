""" Message model. """

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from simplequeue.core.states import MessageStatus


@dataclass(frozen=True, slots=True)
class Message:
    id: int
    queue_name: str
    payload: Any
    status: MessageStatus
    attempts: int
    max_attempts: int
    created_at: datetime
    updated_at: datetime
    available_at: datetime
    leased_at: datetime | None
    lease_expires_at: datetime | None
    acked_at: datetime | None
    dead_lettered_at: datetime | None
    idempotency_key: str | None
    last_error: str | None
    version: int
    receipt_handle: str | None = None
    worker_id: str | None = None
    redeliveries: int = 0


@dataclass(frozen=True, slots=True)
class DeadLetter:
    id: int
    original_message_id: int
    queue_name: str
    payload: Any
    failure_reason: str | None
    attempts: int
    created_at: datetime
    dead_lettered_at: datetime
    final_status: MessageStatus
    requeued_at: datetime | None = None
    requeued_message_id: int | None = None


@dataclass(frozen=True, slots=True)
class QueueEvent:
    id: int
    event_type: str
    queue_name: str
    message_id: int | None
    worker_id: str | None
    receipt_handle_short: str | None
    details: dict[str, Any]
    created_at: datetime


@dataclass(frozen=True, slots=True)
class MessageDetails:
    message: Message
    events: list[QueueEvent]
    dead_letter: DeadLetter | None = None
