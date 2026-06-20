""" Delivery model. """

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from simplequeue.core.modes import DeliveryMode


@dataclass(frozen=True, slots=True)
class Delivery:
    message_id: int
    receipt_handle: str
    queue_name: str
    payload: Any
    attempt: int
    delivery_mode: DeliveryMode
    leased_at: datetime
    lease_expires_at: datetime | None
