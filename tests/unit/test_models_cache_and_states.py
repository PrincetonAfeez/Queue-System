"""Models, cache, and states tests."""

from __future__ import annotations

from datetime import UTC, datetime

from simplequeue.cache.ttl_cache import TTLCache
from simplequeue.core.states import MessageStatus, is_legal_transition
from simplequeue.scheduling.clock import FakeClock


def test_state_transition_table_documents_legal_edges() -> None:
    assert is_legal_transition(MessageStatus.AVAILABLE, MessageStatus.LEASED)
    assert is_legal_transition(MessageStatus.LEASED, MessageStatus.ACKED)
    assert is_legal_transition(MessageStatus.LEASED, MessageStatus.DEAD_LETTERED)
    assert not is_legal_transition(MessageStatus.ACKED, MessageStatus.LEASED)
    assert not is_legal_transition(MessageStatus.DELETED, MessageStatus.LEASED)


def test_ttl_cache_uses_injected_clock() -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    cache: TTLCache[str, int] = TTLCache(ttl_seconds=10, clock=clock)
    cache.set("stats", 1)
    assert cache.get("stats") == 1
    assert cache.hits == 1
    clock.advance(11)
    assert cache.get("stats") is None
    assert cache.evictions == 1
