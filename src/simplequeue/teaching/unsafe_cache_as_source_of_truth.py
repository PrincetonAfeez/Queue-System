""" Unsafe cache as source of truth demo. """

from __future__ import annotations

def demonstrate_cache_correctness_bug() -> dict[str, object]:
    db_state = {"message_id": 1, "status": "leased"}
    stale_cache = {"message_id": 1, "status": "available"}
    delivered_from_cache = stale_cache["status"] == "available"
    return {
        "demo": "unsafe_cache_as_source_of_truth",
        "db_status": db_state["status"],
        "cache_status": stale_cache["status"],
        "delivered_from_cache": delivered_from_cache,
        "corrupted": delivered_from_cache and db_state["status"] == "leased",
        "fix": "use cache only for read-side stats; delivery correctness must read the DB",
    }
