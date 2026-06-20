""" Stats cache for queue statistics. """

from __future__ import annotations

import threading
from pathlib import Path

from simplequeue.cache.ttl_cache import TTLCache
from simplequeue.defaults import DEFAULT_CACHE_TTL
from simplequeue.observability.stats import QueueStatsSnapshot
from simplequeue.scheduling.clock import Clock

_CACHE_BY_KEY: dict[tuple[str, float, int], StatsCache] = {}
_CACHE_LOCK = threading.Lock()


class StatsCache:
    def __init__(self, ttl_seconds: float = 1.0, clock: Clock | None = None) -> None:
        self._cache: TTLCache[str, QueueStatsSnapshot] = TTLCache(ttl_seconds, clock)

    @property
    def hits(self) -> int:
        return self._cache.hits

    @property
    def misses(self) -> int:
        return self._cache.misses

    @property
    def evictions(self) -> int:
        return self._cache.evictions

    def get(self, queue_name: str) -> QueueStatsSnapshot | None:
        return self._cache.get(queue_name)

    def set(self, queue_name: str, snapshot: QueueStatsSnapshot) -> None:
        self._cache.set(queue_name, snapshot)

    def invalidate(self, queue_name: str | None = None) -> None:
        self._cache.invalidate(queue_name)


def shared_stats_cache(
    db_path: str | Path,
    *,
    ttl_seconds: float = DEFAULT_CACHE_TTL,
    clock: Clock | None = None,
) -> StatsCache:
    """Return one ``StatsCache`` per (database path, TTL, clock) tuple.

    Instances are keyed by resolved path, ``ttl_seconds``, and ``id(clock)`` when
    a clock is supplied (``0`` when omitted). Pass the same clock object in tests
    that share a cache.
    """
    clock_key = id(clock) if clock is not None else 0
    key = (str(Path(db_path).resolve()), ttl_seconds, clock_key)
    with _CACHE_LOCK:
        existing = _CACHE_BY_KEY.get(key)
        if existing is None:
            existing = StatsCache(ttl_seconds=ttl_seconds, clock=clock)
            _CACHE_BY_KEY[key] = existing
        return existing
