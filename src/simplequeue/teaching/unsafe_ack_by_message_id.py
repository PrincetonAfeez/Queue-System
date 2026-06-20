""" Unsafe ack by message ID demo. """

from __future__ import annotations

def demonstrate_stale_ack_corruption() -> dict[str, object]:
    """Show why ack(message_id) is unsafe after a lease is reissued."""

    message = {
        "id": 1,
        "status": "leased",
        "receipt_handle": "old",
        "owner": "old-worker",
    }
    message.update({"status": "available", "receipt_handle": None, "owner": None})
    message.update({"status": "leased", "receipt_handle": "new", "owner": "new-worker"})

    stale_worker_message_id = 1
    if stale_worker_message_id == message["id"]:
        message.update({"status": "acked", "acked_by": "old-worker"})

    return {
        "demo": "unsafe_ack_by_message_id",
        "corrupted": message["status"] == "acked" and message["acked_by"] == "old-worker",
        "final_message": message,
        "fix": "ack by current receipt_handle while status is leased and lease is active",
    }
