""" Storage backend factory. """

from __future__ import annotations

from typing import TYPE_CHECKING

from simplequeue.cache.stats_cache import StatsCache, shared_stats_cache
from simplequeue.config import QueueConfig, validate_library_config
from simplequeue.defaults import DEFAULT_MAX_ATTEMPTS
from simplequeue.scheduling.clock import Clock, SystemClock
from simplequeue.storage.base import StorageBackend
from simplequeue.storage.sqlite_backend import SQLiteBackend

if TYPE_CHECKING:
    from simplequeue.core.queue import Queue


def create_backend(config: QueueConfig) -> StorageBackend:
    if config.backend == "sqlite":
        # Commands such as init-db may carry max_attempts=0 in config; the backend
        # still needs a valid default for later produce/consume on the same file.
        default_max_attempts = (
            config.max_attempts if config.max_attempts >= 1 else DEFAULT_MAX_ATTEMPTS
        )
        return SQLiteBackend(config.database_path, default_max_attempts=default_max_attempts)
    raise ValueError(
        f"unsupported backend {config.backend!r}; supported backends: ['sqlite']"
    )


def create_queue(
    config: QueueConfig,
    queue_name: str | None = None,
    *,
    clock: Clock | None = None,
    stats_cache: StatsCache | None = None,
) -> Queue:
    """Build a ``Queue`` from ``QueueConfig``, wiring backend and stats cache TTL."""
    from simplequeue.core.queue import Queue

    validate_library_config(config)
    backend = create_backend(config)
    resolved_clock = clock or SystemClock()
    resolved_cache = stats_cache
    if resolved_cache is None and isinstance(backend, SQLiteBackend):
        resolved_cache = shared_stats_cache(
            config.database_path,
            ttl_seconds=config.cache_ttl,
            clock=resolved_clock,
        )
    return Queue(
        backend,
        queue_name or config.queue_name,
        clock=resolved_clock,
        stats_cache=resolved_cache,
        cache_ttl_seconds=config.cache_ttl,
    )
