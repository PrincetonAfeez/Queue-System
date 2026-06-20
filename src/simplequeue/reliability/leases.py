""" Lease helpers. """

from __future__ import annotations

from datetime import datetime


def lease_is_active(lease_expires_at: datetime | None, now: datetime) -> bool:
    return lease_expires_at is not None and lease_expires_at > now
