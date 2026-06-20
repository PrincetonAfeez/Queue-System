""" Claim budget for the workers layer. """

from __future__ import annotations

import threading

class ClaimBudget:
    """Limits how many messages workers may dequeue (used by CLI ``--limit``)."""

    def __init__(self, limit: int) -> None:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        self._limit = limit
        self._claimed = 0
        self._lock = threading.Lock()

    def try_acquire(self) -> bool:
        with self._lock:
            if self._claimed >= self._limit:
                return False
            self._claimed += 1
            return True

    def release_unused(self) -> None:
        with self._lock:
            if self._claimed > 0:
                self._claimed -= 1
