""" Canonical event-type names. """

from __future__ import annotations

from typing import Final

"""Canonical event-type names.

Every queue mutation records a ``queue_events`` row and emits a structured log
line. The backend, the queue API, and the workers all reference these constants
so event names stay consistent and stats keys cannot silently diverge from the
strings that are written.
"""

ENQUEUE: Final = "enqueue"
LEASE: Final = "lease"
DELETE_DELIVERY: Final = "delete_delivery"
REDELIVER: Final = "redeliver"
ACK: Final = "ack"
NACK: Final = "nack"
DEAD_LETTER: Final = "dead_letter"
LEASE_EXPIRED: Final = "lease_expired"
REQUEUE: Final = "requeue"
SWEEP: Final = "sweep"
WORKER_START: Final = "worker_start"
WORKER_STOP: Final = "worker_stop"
WORKER_FAILURE: Final = "worker_failure"
SCHEDULER_TICK: Final = "scheduler_tick"
SCHEDULER_ERROR: Final = "scheduler_error"
PURGE: Final = "purge"
