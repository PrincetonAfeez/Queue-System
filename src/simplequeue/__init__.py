"""Durable SQLite-backed queue library."""

__version__ = "0.2.1"  # keep in sync with pyproject.toml [project].version

from simplequeue.cache.stats_cache import StatsCache, shared_stats_cache
from simplequeue.config import QueueConfig, validate_library_config
from simplequeue.core.delivery import Delivery
from simplequeue.core.exceptions import (
    DeadLetterNotFound,
    IdempotencyConflict,
    QueueError,
    StorageError,
)
from simplequeue.core.message import DeadLetter, Message, MessageDetails
from simplequeue.core.modes import DeliveryMode
from simplequeue.core.queue import Queue
from simplequeue.core.results import AckResult, LeaseReleaseResult, NackResult
from simplequeue.core.states import MessageStatus
from simplequeue.observability.stats import QueueStatsSnapshot
from simplequeue.reliability.idempotency import payload_idempotency_key
from simplequeue.scheduling.clock import Clock, FakeClock
from simplequeue.scheduling.sweeper import BackgroundSweeper
from simplequeue.storage.factory import create_backend, create_queue
from simplequeue.storage.sqlite_backend import SQLiteBackend
from simplequeue.workers.claim_budget import ClaimBudget
from simplequeue.workers.processor import Processor
from simplequeue.workers.shutdown import ShutdownMode
from simplequeue.workers.worker import Worker
from simplequeue.workers.worker_pool import WorkerPool

__all__ = [
    "AckResult",
    "BackgroundSweeper",
    "ClaimBudget",
    "Clock",
    "create_backend",
    "create_queue",
    "DeadLetter",
    "DeadLetterNotFound",
    "Delivery",
    "DeliveryMode",
    "FakeClock",
    "IdempotencyConflict",
    "LeaseReleaseResult",
    "Message",
    "MessageDetails",
    "MessageStatus",
    "NackResult",
    "Processor",
    "Queue",
    "QueueConfig",
    "QueueError",
    "QueueStatsSnapshot",
    "SQLiteBackend",
    "ShutdownMode",
    "StatsCache",
    "StorageError",
    "Worker",
    "WorkerPool",
    "__version__",
    "payload_idempotency_key",
    "shared_stats_cache",
    "validate_library_config",
]
