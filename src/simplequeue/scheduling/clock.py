""" Clock for the scheduling layer. """

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from threading import Event


class Clock:
    """Clock with durable wall timestamps and monotonic elapsed time."""

    def now(self) -> datetime:
        raise NotImplementedError

    def monotonic(self) -> float:
        raise NotImplementedError

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def wait(self, stop_event: Event, seconds: float) -> bool:
        return stop_event.wait(seconds)


class SystemClock(Clock):
    def now(self) -> datetime:
        return datetime.now(UTC)

    def monotonic(self) -> float:
        return time.monotonic()


@dataclass(slots=True)
class FakeClock(Clock):
    """Deterministic clock for time-dependent correctness tests.

    Logical time (``now``/``monotonic``) only moves when a test calls
    ``advance`` or ``sleep``. Loop cadence (``wait``) is decoupled from logical
    time: it performs a real, interruptible block on the stop event so a worker,
    sweeper, or scheduler thread driven by this clock never busy-loops and never
    races logical time forward. Tests that drive these clocks typically call the
    queue API directly and control time with ``advance``.
    """

    current: datetime
    monotonic_value: float = 0.0

    @classmethod
    def starting_at(cls, current: datetime | None = None) -> FakeClock:
        return cls(current or datetime(2026, 1, 1, tzinfo=UTC))

    def now(self) -> datetime:
        return self.current

    def monotonic(self) -> float:
        return self.monotonic_value

    def advance(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)
        self.monotonic_value += seconds

    def sleep(self, seconds: float) -> None:
        # Explicit sleeps advance logical time so a test can simulate elapsed time.
        self.advance(seconds)

    def wait(self, stop_event: Event, seconds: float) -> bool:
        # Real, interruptible block — does NOT advance logical time. This avoids
        # the busy-loop a background thread would otherwise hit with a fake clock.
        return stop_event.wait(seconds)
