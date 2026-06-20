""" Scheduler for the scheduling layer. """

from __future__ import annotations

import logging
from collections.abc import Callable
from threading import Event, Thread

from simplequeue.core.validation import require_finite_positive, validate_join_timeout
from simplequeue.observability import events
from simplequeue.observability.logging import get_logger, log_event
from simplequeue.scheduling.clock import Clock, SystemClock


class RepeatingScheduler:
    """Runs a callback on a fixed interval until stopped.

    The callback is guarded: a raised exception is logged as a
    ``scheduler_error`` and the loop continues, so a transient failure in one
    tick (for example a sweep hitting a momentarily locked database) cannot
    silently kill the background thread.
    """

    def __init__(
        self,
        callback: Callable[[], None],
        *,
        interval: float,
        clock: Clock | None = None,
        name: str = "simplequeue-scheduler",
        logger: logging.Logger | None = None,
    ) -> None:
        require_finite_positive(interval, field="interval")
        self.callback = callback
        self.interval = interval
        self.clock = clock or SystemClock()
        self.name = name
        self.logger = logger or get_logger()
        self._stop_event = Event()
        self._thread = Thread(target=self._run, name=name, daemon=False)

    @property
    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def start(self) -> None:
        if self._thread.is_alive():
            return
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def join(self, timeout: float | None = None) -> bool:
        validate_join_timeout(timeout)
        self._thread.join(timeout)
        return not self._thread.is_alive()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.callback()
                # Heartbeat at DEBUG so a 1s sweeper does not flood INFO logs.
                log_event(self.logger, events.SCHEDULER_TICK, level=logging.DEBUG, scheduler=self.name)
            except Exception as error:  # a bad tick must not kill the loop
                log_event(
                    self.logger,
                    events.SCHEDULER_ERROR,
                    level=logging.WARNING,
                    scheduler=self.name,
                    error=str(error),
                )
            self.clock.wait(self._stop_event, self.interval)
