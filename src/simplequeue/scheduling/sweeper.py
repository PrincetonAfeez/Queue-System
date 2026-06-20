""" Sweeper for the scheduling layer. """

from __future__ import annotations

import threading

from simplequeue.core.exceptions import QueueError
from simplequeue.core.queue import Queue
from simplequeue.core.validation import require_finite_positive, validate_join_timeout
from simplequeue.defaults import DEFAULT_JOIN_TIMEOUT, DEFAULT_SWEEPER_INTERVAL
from simplequeue.scheduling.scheduler import RepeatingScheduler
from simplequeue.storage.sqlite_backend import SQLiteBackend

_ACTIVE_BY_DB: dict[str, BackgroundSweeper] = {}
_SWEEPER_LOCK = threading.Lock()


class BackgroundSweeper:
    """Runs ``Queue.sweep`` on a fixed interval in a background thread.

    Built on ``RepeatingScheduler`` so the loop, clock handling, and clean
    start/stop live in one place. The sweep itself reclaims expired leases and
    moves exhausted-retry messages to the DLQ.

    ``Queue.sweep`` is database-wide. Use at most one ``BackgroundSweeper`` per
    SQLite database, even when multiple ``Queue`` instances share that file.
    Always call ``join()`` after ``stop()`` so the registry slot is released.
    """

    def __init__(
        self,
        queue: Queue,
        *,
        interval: float = DEFAULT_SWEEPER_INTERVAL,
        join_timeout: float | None = DEFAULT_JOIN_TIMEOUT,
    ) -> None:
        require_finite_positive(interval, field="interval")
        if join_timeout is not None:
            validate_join_timeout(join_timeout)
        self.queue = queue
        self.interval = interval
        self.join_timeout = join_timeout
        self._db_key = _database_key(queue)
        self._started = False
        self._scheduler = RepeatingScheduler(
            self._tick,
            interval=interval,
            clock=queue.clock,
            name=f"sweeper-{queue.queue_name}",
            logger=queue.logger,
        )

    def _tick(self) -> None:
        self.queue.sweep()

    @property
    def is_alive(self) -> bool:
        return self._scheduler.is_alive

    def start(self) -> None:
        with _SWEEPER_LOCK:
            existing = _ACTIVE_BY_DB.get(self._db_key)
            if existing is not None and existing is not self and existing.is_alive:
                raise QueueError(
                    f"only one BackgroundSweeper may run per database: {self._db_key}"
                )
            if existing is not None and not existing.is_alive:
                _ACTIVE_BY_DB.pop(self._db_key, None)
            self._scheduler.start()
            self._started = True
            _ACTIVE_BY_DB[self._db_key] = self

    def stop(self) -> None:
        self._scheduler.stop()

    def join(self, timeout: float | None = None) -> bool:
        validate_join_timeout(timeout)
        stopped = self._scheduler.join(timeout)
        with _SWEEPER_LOCK:
            if self._started and not self.is_alive:
                if _ACTIVE_BY_DB.get(self._db_key) is self:
                    _ACTIVE_BY_DB.pop(self._db_key, None)
                self._started = False
        return stopped

    def __enter__(self) -> BackgroundSweeper:
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.stop()
        self.join(self.join_timeout)


def _database_key(queue: Queue) -> str:
    backend = queue.backend
    if isinstance(backend, SQLiteBackend):
        return str(backend.db_path.resolve())
    return f"{id(backend)}"
