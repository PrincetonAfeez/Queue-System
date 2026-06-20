"""Reliability helpers for retry, DLQ, leases, idempotency, and receipts.

These small, pure helpers encode the reliability rules in one place so the
storage backend (and tests) share a single definition of "should this retry?",
"is this lease still active?", and "how is a receipt handle minted?".
"""

from simplequeue.reliability.dlq import DLQ_REASON_LEASE_EXPIRED, DLQ_REASON_MAX_ATTEMPTS
from simplequeue.reliability.idempotency import payload_idempotency_key
from simplequeue.reliability.leases import lease_is_active
from simplequeue.reliability.receipt_handles import new_receipt_handle
from simplequeue.reliability.retry import RetryDecision, decide_retry

__all__ = [
    "DLQ_REASON_LEASE_EXPIRED",
    "DLQ_REASON_MAX_ATTEMPTS",
    "RetryDecision",
    "decide_retry",
    "lease_is_active",
    "new_receipt_handle",
    "payload_idempotency_key",
]
