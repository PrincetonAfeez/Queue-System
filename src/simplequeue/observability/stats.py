""" Stats for the observability layer. """

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class QueueStatsSnapshot:
    """Point-in-time queue counters.

    ``delivered`` counts delivery attempts (LEASE + DELETE_DELIVERY events), not
    unique messages; redeliveries increment it again.
    """

    queue_name: str
    enqueued: int
    delivered: int
    acked: int
    nacked: int
    redelivered: int
    dead_lettered: int
    expired: int
    current_depth: int
    scheduled_count: int
    in_flight_count: int
    recent_worker_ids: int
    recent_throughput: float

    def to_dict(self) -> dict[str, int | float | str]:
        data = asdict(self)
        data["delivery_attempts"] = self.delivered
        return data
