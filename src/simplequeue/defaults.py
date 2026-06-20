from __future__ import annotations

"""Single source of truth for library-wide default values.

The Python API, the worker classes, and ``QueueConfig`` all read these so the
same setting cannot drift to two different defaults depending on entry path.
"""

DEFAULT_QUEUE_NAME: str = "default"
DEFAULT_DELIVERY_MODE: str = "at-least-once"
DEFAULT_VISIBILITY_TIMEOUT: float = 30.0
DEFAULT_MAX_ATTEMPTS: int = 3
DEFAULT_WORKER_COUNT: int = 1
DEFAULT_CACHE_TTL: float = 1.0
DEFAULT_DATABASE_PATH: str = "queue.db"
DEFAULT_LOGGING_LEVEL: str = "INFO"
DEFAULT_SWEEPER_INTERVAL: float = 1.0
DEFAULT_POLL_INTERVAL: float = 0.25
DEFAULT_IDLE_TIMEOUT: float = 1.0
DEFAULT_SHUTDOWN_MODE: str = "finish-current"
DEFAULT_JOIN_TIMEOUT: float = 30.0
DEFAULT_CONSUME_JOIN_TIMEOUT: float = 30.0
DEFAULT_PURGE_RETENTION_DAYS: float = 7.0
