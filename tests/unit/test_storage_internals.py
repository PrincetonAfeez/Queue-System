"""Storage internals tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from simplequeue.core.exceptions import StorageError
from simplequeue.core.modes import DeliveryMode
from simplequeue.core.results import AckResult, ClaimResult, LeaseReleaseResult, NackResult
from simplequeue.core.states import MessageStatus
from simplequeue.storage import sqlite_backend as sb
from simplequeue.storage.base import StorageBackend
from simplequeue.storage.migrations import load_schema_sql
from simplequeue.storage.sqlite_backend import SQLiteBackend


def test_load_schema_sql_contains_messages_table() -> None:
    sql = load_schema_sql()
    assert "CREATE TABLE IF NOT EXISTS messages" in sql
    assert "CREATE TABLE IF NOT EXISTS dead_letters" in sql
    assert "CREATE TABLE IF NOT EXISTS schema_meta" in sql


def test_sqlite_backend_rejects_invalid_default_max_attempts(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="default_max_attempts"):
        SQLiteBackend(tmp_path / "q.db", default_max_attempts=0)


def test_dt_normalizes_naive_datetime_to_utc() -> None:
    naive = datetime(2026, 6, 1, 12, 0, 0)
    formatted = sb._dt(naive)
    assert formatted.endswith("+00:00")
    assert "2026-06-01T12:00:00" in formatted


def test_dt_preserves_aware_datetime() -> None:
    eastern = datetime(2026, 6, 1, 8, 0, 0, tzinfo=timezone(timedelta(hours=-4)))
    formatted = sb._dt(eastern)
    assert "+00:00" in formatted


def test_parse_dt_none_returns_none() -> None:
    assert sb._parse_dt(None) is None


def test_parse_dt_naive_gets_utc() -> None:
    parsed = sb._parse_dt("2026-01-01T12:00:00")
    assert parsed is not None
    assert parsed.tzinfo == UTC


def test_parse_dt_aware_converts_to_utc() -> None:
    parsed = sb._parse_dt("2026-01-01T08:00:00-05:00")
    assert parsed is not None
    assert parsed.tzinfo == UTC


def test_short_receipt_handle() -> None:
    assert sb._short("abcdef1234567890") == "abcdef123456"
    assert sb._short(None) is None
    assert sb._short("") is None


def test_storage_backend_cannot_be_instantiated() -> None:
    with pytest.raises(TypeError):
        StorageBackend()  # type: ignore[abstract]


def test_init_schema_before_use_raises_storage_error(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path / "no-schema.db")
    with pytest.raises(StorageError, match="no such table"):
        backend.peek("q")


def test_list_dead_letters_all_queues(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path / "dlq-all.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    mid = backend.enqueue("alpha", {"x": 1}, max_attempts=1, now=now)
    claim = backend.claim_next("alpha", DeliveryMode.AT_LEAST_ONCE, timedelta(seconds=30), "w", now)
    assert claim.delivery is not None
    backend.nack(claim.delivery.receipt_handle, now, reason="fail")
    all_dlq = backend.list_dead_letters(None)
    assert len(all_dlq) == 1
    assert all_dlq[0].original_message_id == mid
    filtered = backend.list_dead_letters("alpha")
    assert len(filtered) == 1


def test_nack_expired_lease_redelivers_when_retries_remain(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path / "nack-exp.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    backend.enqueue("q", {"x": 1}, max_attempts=3, now=now)
    claim = backend.claim_next("q", DeliveryMode.AT_LEAST_ONCE, timedelta(seconds=5), "w", now)
    assert claim.delivery is not None
    later = now + timedelta(seconds=10)
    result = backend.nack(claim.delivery.receipt_handle, later, reason="late nack")
    assert result.success
    assert result.status == MessageStatus.AVAILABLE.value


def test_nack_expired_lease_dlqs_when_exhausted(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path / "nack-exp-dlq.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    backend.enqueue("q", {"x": 1}, max_attempts=1, now=now)
    claim = backend.claim_next("q", DeliveryMode.AT_LEAST_ONCE, timedelta(seconds=5), "w", now)
    assert claim.delivery is not None
    later = now + timedelta(seconds=10)
    result = backend.nack(claim.delivery.receipt_handle, later, reason="late nack")
    assert result.success
    assert result.moved_to_dlq


def test_move_exhausted_to_dlq_on_available_messages(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path / "move-exhausted.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    mid = backend.enqueue("q", {"x": 1}, max_attempts=1, now=now)
    claim = backend.claim_next("q", DeliveryMode.AT_LEAST_ONCE, timedelta(seconds=30), "w", now)
    assert claim.delivery is not None
    backend.nack(claim.delivery.receipt_handle, now, reason="fail")
    moved = backend.move_exhausted_to_dlq(now)
    assert moved >= 0
    dlq = backend.list_dead_letters("q")
    assert any(entry.original_message_id == mid for entry in dlq)


def test_ack_not_leased_message(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path / "ack-not-leased.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    backend.enqueue("q", {"x": 1}, now=now)
    claim = backend.claim_next("q", DeliveryMode.AT_MOST_ONCE, timedelta(seconds=30), "w", now)
    assert claim.delivery is not None
    result = backend.ack(claim.delivery.receipt_handle, now)
    assert not result.success
    assert result.reason == "not_leased"


def test_ack_invalid_receipt_handle(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path / "ack-invalid.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    result = backend.ack("does-not-exist", now)
    assert result == AckResult(False, None, "invalid", "receipt_handle_not_found")


def test_nack_invalid_receipt_handle(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path / "nack-invalid.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    result = backend.nack("does-not-exist", now)
    assert result == NackResult(False, None, "invalid", "receipt_handle_not_found")


def test_release_expired_leases_empty(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path / "release-empty.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    result = backend.release_expired_leases(now)
    assert result == LeaseReleaseResult(redelivered=0, dead_lettered=0)


def test_claim_next_empty_queue(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path / "empty.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    result = backend.claim_next("q", DeliveryMode.AT_LEAST_ONCE, timedelta(seconds=30), "w", now)
    assert result == ClaimResult(None, False)


def test_stats_on_empty_queue(tmp_path: Path) -> None:
    backend = SQLiteBackend(tmp_path / "stats-empty.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    stats = backend.stats("empty", now)
    assert stats.queue_name == "empty"
    assert stats.enqueued == 0
    assert stats.current_depth == 0


def test_required_dt_raises_on_none() -> None:
    with pytest.raises(ValueError, match="non-null timestamp"):
        SQLiteBackend._required_dt(None)
