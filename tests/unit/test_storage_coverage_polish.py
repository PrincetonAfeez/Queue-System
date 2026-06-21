"""Storage coverage polish tests."""

from __future__ import annotations

from contextlib import closing
from datetime import UTC, datetime, timedelta

import pytest

from simplequeue.core.states import MessageStatus
from simplequeue.core.validation import validate_join_timeout, validate_queue_name
from simplequeue.observability import events
from simplequeue.scheduling.clock import FakeClock
from simplequeue.storage import sqlite_backend as sb
from simplequeue.storage.sqlite_backend import SQLiteBackend


def test_release_expired_leases_scoped_to_one_queue(tmp_path) -> None:
    backend = SQLiteBackend(tmp_path / "scoped-release.db")
    backend.init_schema()
    now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    expired = now - timedelta(seconds=30)
    message_ids: dict[str, int] = {}
    claims: dict[str, object] = {}
    for queue_name in ("alpha", "beta"):
        message_ids[queue_name] = backend.enqueue(queue_name, {"q": queue_name}, now=now)
        claim = backend.claim_next(
            queue_name,
            "at-least-once",
            timedelta(seconds=5),
            f"w-{queue_name}",
            now,
        )
        assert claim.delivery is not None
        claims[queue_name] = claim
    # Expire leases after both claims so beta's claim_next does not release alpha early.
    for queue_name in ("alpha", "beta"):
        claim = claims[queue_name]
        assert claim.delivery is not None
        with closing(backend._connect()) as conn:  # noqa: SLF001
            conn.execute(
                "UPDATE messages SET lease_expires_at = ? WHERE id = ?",
                (sb._dt(expired), claim.delivery.message_id),
            )
            conn.commit()
    with closing(backend._connect()) as conn:  # noqa: SLF001
        conn.execute("BEGIN IMMEDIATE")
        scoped = backend._release_expired_leases_locked(conn, now, queue_name="alpha")  # noqa: SLF001
        conn.commit()
    assert scoped.redelivered == 1
    alpha = backend.inspect(message_ids["alpha"])
    assert alpha is not None
    assert alpha.message.status is MessageStatus.AVAILABLE
    beta_claim = backend.claim_next(
        "beta",
        "at-least-once",
        timedelta(seconds=5),
        "w2",
        now,
    )
    assert beta_claim.delivery is not None


def test_second_claim_records_redeliver_event(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("redeliver-event", clock=clock)
    message_id = queue.enqueue({"x": 1})
    first = queue.dequeue(visibility_timeout=5, worker_id="w1")
    assert first is not None
    clock.advance(6)
    queue.sweep()
    second = queue.dequeue(visibility_timeout=5, worker_id="w2")
    assert second is not None
    assert second.attempt == 2
    details = queue.inspect(message_id)
    assert details is not None
    event_types = [event.event_type for event in details.events]
    assert events.REDELIVER in event_types


def test_nack_expired_lease_stale_when_dlq_move_fails(queue_factory, monkeypatch) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("nack-expired-stale", clock=clock)
    queue.enqueue({"x": 1}, max_attempts=1)
    delivery = queue.dequeue(visibility_timeout=5, worker_id="w")
    assert delivery is not None
    clock.advance(6)
    monkeypatch.setattr(queue.backend, "_move_to_dlq_locked", lambda *args, **kwargs: False)
    result = queue.nack(delivery.receipt_handle, reason="late")
    assert not result.success
    assert result.reason == "stale_receipt"


def test_purge_terminal_only_touches_target_queue(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    alpha = queue_factory("alpha", clock=clock, db_name="purge-scope.db")
    beta = queue_factory("beta", db_name="purge-scope.db", clock=clock)
    acked_ids: dict[str, int] = {}
    for queue in (alpha, beta):
        message_id = queue.enqueue({"x": 1})
        delivery = queue.dequeue(worker_id="w")
        assert delivery is not None
        queue.ack(delivery.receipt_handle)
        acked_ids[queue.queue_name] = message_id
    removed = alpha.purge_terminal(older_than=clock.now(), queue_name="alpha")
    assert removed == 1
    assert alpha.inspect(acked_ids["alpha"]) is None
    assert beta.inspect(acked_ids["beta"]) is not None


def test_validate_queue_name_rejects_whitespace_edges() -> None:
    with pytest.raises(ValueError, match="leading or trailing whitespace"):
        validate_queue_name(" jobs ")
    with pytest.raises(ValueError, match="at most"):
        validate_queue_name("x" * 257)


def test_validate_join_timeout_rejects_negative() -> None:
    with pytest.raises(ValueError, match="join_timeout"):
        validate_join_timeout(-1.0)
