""" Results for the core queue domain. """

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

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


@dataclass(frozen=True, slots=True)
class VerifyResult:
    """Outcome of a read-only SQLite database health check."""

    healthy: bool
    db_path: str
    integrity_check: str
    foreign_key_check_ok: bool
    schema_version: int | None
    expected_schema_version: int
    schema_consistent: bool
    tables: dict[str, bool]
    row_counts: dict[str, int]
    errors: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "healthy": self.healthy,
            "db": self.db_path,
            "integrity_check": self.integrity_check,
            "foreign_key_check_ok": self.foreign_key_check_ok,
            "schema_version": self.schema_version,
            "expected_schema_version": self.expected_schema_version,
            "schema_consistent": self.schema_consistent,
            "tables": dict(self.tables),
            "row_counts": dict(self.row_counts),
            "errors": list(self.errors),
        }
