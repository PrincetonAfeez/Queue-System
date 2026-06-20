""" Unsafe no visibility timeout demo. """

from __future__ import annotations

def demonstrate_stuck_message() -> dict[str, object]:
    message = {"id": 1, "status": "available"}
    message["status"] = "leased"
    crashed_worker = True
    redeliverable = message["status"] == "available"
    return {
        "demo": "unsafe_no_visibility_timeout",
        "worker_crashed": crashed_worker,
        "message_status": message["status"],
        "redeliverable": redeliverable,
        "stuck_forever": crashed_worker and not redeliverable,
        "fix": "store lease_expires_at and sweep expired leases back to available or DLQ",
    }
