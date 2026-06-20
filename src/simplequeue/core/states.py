""" Message states. """

from __future__ import annotations

from enum import Enum


class MessageStatus(str, Enum):
    AVAILABLE = "available"
    LEASED = "leased"
    ACKED = "acked"
    DELETED = "deleted"
    DEAD_LETTERED = "dead_lettered"


# Note: the ``expired`` STAT reported by QueueStatsSnapshot counts expired
# *leases* (a lease deadline passing), not a distinct message status. Message
# TTL eviction is intentionally out of scope, so there is no ``EXPIRED`` status.
LEGAL_TRANSITIONS: set[tuple[MessageStatus, MessageStatus]] = {
    (MessageStatus.AVAILABLE, MessageStatus.LEASED),
    (MessageStatus.LEASED, MessageStatus.ACKED),
    (MessageStatus.LEASED, MessageStatus.AVAILABLE),
    (MessageStatus.LEASED, MessageStatus.DEAD_LETTERED),
    # Defensive path: an available message whose attempts are already exhausted
    # (e.g. max_attempts lowered) is dead-lettered instead of being delivered.
    (MessageStatus.AVAILABLE, MessageStatus.DEAD_LETTERED),
    (MessageStatus.DEAD_LETTERED, MessageStatus.AVAILABLE),
    (MessageStatus.AVAILABLE, MessageStatus.DELETED),
}


def is_legal_transition(source: MessageStatus, target: MessageStatus) -> bool:
    return (source, target) in LEGAL_TRANSITIONS


def assert_legal_transition(source: MessageStatus, target: MessageStatus) -> None:
    """Guard a state change against the documented transition table.

    The SQLite backend already enforces legality with guarded ``WHERE`` clauses;
    this Python-level check makes the documented state machine executable so a
    programming error (an undocumented transition) fails loudly instead of
    silently corrupting state.
    """

    if not is_legal_transition(source, target):
        raise ValueError(f"illegal state transition: {source.value} -> {target.value}")
