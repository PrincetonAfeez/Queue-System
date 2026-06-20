""" Results for the core queue domain. """

from __future__ import annotations

from dataclasses import dataclass

from simplequeue.core.delivery import Delivery


@dataclass(frozen=True, slots=True)
class ClaimResult:
    delivery: Delivery | None
    mutated: bool


@dataclass(frozen=True, slots=True)
class AckResult:
    success: bool
    message_id: int | None
    status: str
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class NackResult:
    success: bool
    message_id: int | None
    status: str
    reason: str | None = None
    moved_to_dlq: bool = False


@dataclass(frozen=True, slots=True)
class LeaseReleaseResult:
    """Counts from reclaiming expired leases in one maintenance pass."""

    redelivered: int
    dead_lettered: int

    @property
    def total(self) -> int:
        return self.redelivered + self.dead_lettered
