"""Reliability helpers tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from simplequeue.core.states import MessageStatus, assert_legal_transition, is_legal_transition
from simplequeue.reliability.idempotency import payload_idempotency_key
from simplequeue.reliability.leases import lease_is_active
from simplequeue.reliability.retry import decide_retry


def test_decide_retry_boundaries() -> None:
    assert decide_retry(2, 3).should_retry
    assert not decide_retry(2, 3).should_dead_letter
    assert decide_retry(3, 3).should_dead_letter
    assert not decide_retry(3, 3).should_retry


def test_lease_is_active() -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    assert not lease_is_active(None, now)
    assert lease_is_active(now + timedelta(seconds=5), now)
    assert not lease_is_active(now, now)


def test_payload_idempotency_key_stable() -> None:
    assert payload_idempotency_key({"b": 2, "a": 1}) == payload_idempotency_key({"a": 1, "b": 2})
    assert payload_idempotency_key({"a": 1}) != payload_idempotency_key({"a": 2})


def test_assert_legal_transition() -> None:
    assert_legal_transition(MessageStatus.AVAILABLE, MessageStatus.LEASED)
    with pytest.raises(ValueError):
        assert_legal_transition(MessageStatus.ACKED, MessageStatus.AVAILABLE)


def test_is_legal_transition_table_edges() -> None:
    assert is_legal_transition(MessageStatus.LEASED, MessageStatus.AVAILABLE)
    assert is_legal_transition(MessageStatus.AVAILABLE, MessageStatus.DEAD_LETTERED)
    assert is_legal_transition(MessageStatus.DEAD_LETTERED, MessageStatus.AVAILABLE)
    assert is_legal_transition(MessageStatus.AVAILABLE, MessageStatus.DELETED)
