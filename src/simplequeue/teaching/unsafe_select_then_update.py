""" Unsafe select-then-update demo. """

from __future__ import annotations

from dataclasses import dataclass

@dataclass(slots=True)
class UnsafeMessage:
    id: int
    status: str = "available"


def demonstrate_double_claim() -> dict[str, object]:
    """Show SELECT-then-UPDATE without a guard can double-deliver.

    Two consumers both read the same available row before either update commits.
    Because the update is not guarded by status/version, both believe they own it.
    """

    message = UnsafeMessage(id=1)
    consumer_a_selected = message.id if message.status == "available" else None
    consumer_b_selected = message.id if message.status == "available" else None
    if consumer_a_selected is not None:
        message.status = "leased-by-a"
    if consumer_b_selected is not None:
        message.status = "leased-by-b"
    return {
        "demo": "unsafe_select_then_update",
        "consumer_a_claimed": consumer_a_selected,
        "consumer_b_claimed": consumer_b_selected,
        "double_claimed": consumer_a_selected == consumer_b_selected == 1,
        "final_status": message.status,
        "fix": "use a transaction plus guarded UPDATE WHERE status='available' AND version=?",
    }
