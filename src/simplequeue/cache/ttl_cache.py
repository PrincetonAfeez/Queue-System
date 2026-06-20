""" Tiny TTL cache. """

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from typing import Generic, TypeVar

from simplequeue.scheduling.clock import Clock, SystemClock

K = TypeVar("K")
V = TypeVar("V")


@dataclass(slots=True)
class _CacheEntry(Generic[V]):
    value: V
    stored_at: datetime


class TTLCache(Generic[K, V]):
    """Tiny TTL cache. Thread-safe so a shared stats cache can be invalidated
    concurrently by worker-pool threads while another thread reads it.

    Expiry follows the injected clock's ``now()`` domain so ``FakeClock`` tests
    stay aligned with queue semantics without a separate monotonic timeline.
    """

    def __init__(self, ttl_seconds: float, clock: Clock | None = None) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        self.ttl_seconds = ttl_seconds
        self.clock = clock or SystemClock()
        self._items: dict[K, _CacheEntry[V]] = {}
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def get(self, key: K) -> V | None:
        with self._lock:
            entry = self._items.get(key)
            if entry is None:
                self.misses += 1
                return None
            age = (self.clock.now() - entry.stored_at).total_seconds()
            if age >= self.ttl_seconds:
                self.evictions += 1
                self.misses += 1
                self._items.pop(key, None)
                return None
            self.hits += 1
            return entry.value

    def set(self, key: K, value: V) -> None:
        with self._lock:
            self._items[key] = _CacheEntry(value=value, stored_at=self.clock.now())

    def invalidate(self, key: K | None = None) -> None:
        with self._lock:
            if key is None:
                self.evictions += len(self._items)
                self._items.clear()
                return
            if key in self._items:
                self.evictions += 1
                self._items.pop(key, None)
