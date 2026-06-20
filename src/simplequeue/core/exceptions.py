""" Exceptions for the core queue domain. """

from __future__ import annotations


class QueueError(Exception):
    """Base exception for queue domain errors."""


class StorageError(Exception):
    """Raised when the storage backend cannot complete an operation."""


class DeadLetterNotFound(QueueError):
    """Raised when a DLQ requeue targets a message that is not dead-lettered."""


class IdempotencyConflict(QueueError):
    """Raised when an idempotency key collides with a live message.

    Covers enqueue with a different payload for an active key and DLQ requeue
    when another live message already holds the same key.
    """


# Design note: ack() and nack() deliberately do NOT raise on a stale or invalid
# receipt handle. They return an AckResult / NackResult with ``success=False`` and
# a machine-readable ``reason`` so callers can branch without exception handling
# in the hot path. Receipt-handle validity is therefore reported, not thrown.
