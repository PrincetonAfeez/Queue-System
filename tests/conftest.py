"""Shared fixtures for the tests."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pytest

from simplequeue.cache.stats_cache import StatsCache
from simplequeue.core.queue import Queue
from simplequeue.scheduling.clock import Clock
from simplequeue.storage.sqlite_backend import SQLiteBackend


@pytest.fixture
def queue_factory(tmp_path: Path) -> Callable[..., Queue]:
    def factory(
        name: str = "test",
        *,
        clock: Clock | None = None,
        db_name: str = "queue.db",
        stats_cache: StatsCache | None = None,
    ) -> Queue:
        backend = SQLiteBackend(tmp_path / db_name)
        queue = Queue(backend, name, clock=clock, stats_cache=stats_cache)
        queue.init_schema()
        return queue

    return factory
