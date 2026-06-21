"""Round 5 fixes tests."""

from __future__ import annotations

import sqlite3
import time
from typing import Any

import pytest

from simplequeue.cache.ttl_cache import TTLCache
from simplequeue.config import QueueConfig, validate_library_config
from simplequeue.core.exceptions import StorageError
from simplequeue.core.queue import Queue
from simplequeue.scheduling.sweeper import BackgroundSweeper
from simplequeue.storage.factory import create_queue
from simplequeue.storage.sqlite_backend import SQLiteBackend


def test_ttl_cache_rejects_nan() -> None:
    with pytest.raises(ValueError, match="ttl_seconds"):
        TTLCache(float("nan"))


def test_queue_rejects_nan_cache_ttl_when_auto_creating_cache(tmp_path) -> None:
    with pytest.raises(ValueError, match="cache_ttl_seconds"):
        Queue(SQLiteBackend(tmp_path / "nan.db"), "jobs", cache_ttl_seconds=float("nan"))


def test_create_queue_rejects_nan_cache_ttl(tmp_path) -> None:
    with pytest.raises(ValueError, match="cache_ttl"):
        create_queue(QueueConfig(database_path=str(tmp_path / "nan.db"), cache_ttl=float("nan")))


def test_create_queue_rejects_zero_cache_ttl(tmp_path) -> None:
    with pytest.raises(ValueError, match="cache_ttl"):
        create_queue(QueueConfig(database_path=str(tmp_path / "zero.db"), cache_ttl=0.0))


def test_validate_library_config_allows_max_attempts_zero() -> None:
    validate_library_config(QueueConfig(max_attempts=0))


def test_enqueue_idempotency_storage_error_after_retries(queue_factory, monkeypatch) -> None:
    monkeypatch.setattr(
        "simplequeue.storage.sqlite_backend.IDEMPOTENCY_ENQUEUE_MAX_RETRIES",
        1,
    )
    queue = queue_factory("idemp-exhaust")
    queue.enqueue({"x": 1}, idempotency_key="race")
    backend = queue.backend
    assert isinstance(backend, SQLiteBackend)
    real_connect = backend._connect  # noqa: SLF001

    class _ConnectionProxy:
        def __init__(self, conn: sqlite3.Connection) -> None:
            self._conn = conn

        def execute(self, sql: str, params: tuple[Any, ...] = ()) -> sqlite3.Cursor:
            if "SELECT id, payload FROM messages" in sql:
                return self._conn.execute("SELECT 1 WHERE 0", ())
            return self._conn.execute(sql, params)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._conn, name)

    def connect_wrapper() -> sqlite3.Connection:
        return _ConnectionProxy(real_connect())  # type: ignore[return-value]

    monkeypatch.setattr(backend, "_connect", connect_wrapper)
    with pytest.raises(StorageError, match="after retries"):
        backend.enqueue("idemp-exhaust", {"x": 1}, idempotency_key="race")


def test_background_sweeper_clears_dead_registry_entry(queue_factory) -> None:
    queue = queue_factory("sweeper-registry")
    first = BackgroundSweeper(queue, interval=0.05)
    first.start()
    first.stop()
    deadline = time.monotonic() + 2.0
    while first.is_alive and time.monotonic() < deadline:
        time.sleep(0.01)
    assert not first.is_alive
    second = BackgroundSweeper(queue, interval=0.05)
    second.start()
    second.stop()
    second.join(2)
