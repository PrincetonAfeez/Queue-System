"""Round 5 concurrency tests."""

from __future__ import annotations

import threading
import time
from datetime import UTC, datetime

from simplequeue.scheduling.clock import FakeClock


def test_concurrent_sweep_and_dequeue(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("sweep-race", clock=clock)
    queue.enqueue({"x": 1})
    delivery = queue.dequeue(visibility_timeout=2, worker_id="w")
    assert delivery is not None
    clock.advance(3)
    stop = threading.Event()
    errors: list[str] = []

    def sweeper() -> None:
        while not stop.is_set():
            try:
                queue.sweep()
            except Exception as error:
                errors.append(str(error))
            clock.wait(stop, 0.01)

    thread = threading.Thread(target=sweeper)
    thread.start()
    redelivered = None
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        redelivered = queue.dequeue(worker_id="w2")
        if redelivered is not None:
            break
        time.sleep(0.01)
    stop.set()
    thread.join(2)
    assert not errors
    assert redelivered is not None
    assert redelivered.message_id == delivery.message_id
