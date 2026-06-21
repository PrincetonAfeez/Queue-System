"""Modes, config, and results tests."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from simplequeue.cache.stats_cache import shared_stats_cache
from simplequeue.cache.ttl_cache import TTLCache
from simplequeue.core.modes import DeliveryMode
from simplequeue.core.results import LeaseReleaseResult
from simplequeue.observability.stats import QueueStatsSnapshot
from simplequeue.scheduling.clock import FakeClock
from simplequeue.workers.shutdown import ShutdownMode


def test_delivery_mode_parse_aliases() -> None:
    assert DeliveryMode.parse("at_least_once") is DeliveryMode.AT_LEAST_ONCE
    with pytest.raises(ValueError):
        DeliveryMode.parse("bogus")


def test_shutdown_mode_parse_rejects_unknown() -> None:
    with pytest.raises(ValueError):
        ShutdownMode.parse("bogus-mode")


def test_ttl_cache_rejects_non_positive_ttl() -> None:
    with pytest.raises(ValueError, match="ttl_seconds"):
        TTLCache(0)


def test_shared_stats_cache_keyed_by_path_ttl_and_clock() -> None:
    clock_a = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    clock_b = FakeClock.starting_at(datetime(2027, 1, 1, tzinfo=UTC))
    first = shared_stats_cache("same.db", ttl_seconds=1.0, clock=clock_a)
    same_key = shared_stats_cache("same.db", ttl_seconds=1.0, clock=clock_a)
    other_ttl = shared_stats_cache("same.db", ttl_seconds=99.0, clock=clock_a)
    other_clock = shared_stats_cache("same.db", ttl_seconds=1.0, clock=clock_b)
    assert first is same_key
    assert first is not other_ttl
    assert first is not other_clock


def test_lease_release_result_total() -> None:
    assert LeaseReleaseResult(2, 1).total == 3


def test_stats_snapshot_delivery_attempts_alias() -> None:
    snap = QueueStatsSnapshot(
        queue_name="q",
        enqueued=1,
        delivered=5,
        acked=4,
        nacked=0,
        redelivered=1,
        dead_lettered=0,
        expired=0,
        current_depth=0,
        scheduled_count=0,
        in_flight_count=0,
        recent_worker_ids=0,
        recent_throughput=0.0,
    )
    assert snap.to_dict()["delivery_attempts"] == 5
