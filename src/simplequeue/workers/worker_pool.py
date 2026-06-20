""" Worker pool for the workers layer. """

from __future__ import annotations

import time

from simplequeue.core.modes import DeliveryMode
from simplequeue.core.queue import Queue
from simplequeue.core.validation import require_finite_positive, validate_join_timeout
from simplequeue.defaults import (
    DEFAULT_JOIN_TIMEOUT,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_VISIBILITY_TIMEOUT,
    DEFAULT_WORKER_COUNT,
)
from simplequeue.workers.claim_budget import ClaimBudget
from simplequeue.workers.processor import Processor
from simplequeue.workers.shutdown import ShutdownMode
from simplequeue.workers.worker import Worker


class WorkerPool:
    def __init__(
        self,
        queue: Queue,
        processor: Processor,
        *,
        workers: int = DEFAULT_WORKER_COUNT,
        delivery_mode: DeliveryMode | str = DeliveryMode.AT_LEAST_ONCE,
        visibility_timeout: float = DEFAULT_VISIBILITY_TIMEOUT,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        shutdown_mode: ShutdownMode | str = ShutdownMode.FINISH_CURRENT,
        claim_budget: ClaimBudget | None = None,
        join_timeout: float | None = DEFAULT_JOIN_TIMEOUT,
    ) -> None:
        if workers < 1:
            raise ValueError("workers must be >= 1")
        require_finite_positive(poll_interval, field="poll_interval")
        if join_timeout is not None:
            validate_join_timeout(join_timeout)
        self.join_timeout = join_timeout
        self.workers = [
            Worker(
                queue,
                processor,
                worker_id=f"worker-{index + 1}",
                delivery_mode=delivery_mode,
                visibility_timeout=visibility_timeout,
                poll_interval=poll_interval,
                shutdown_mode=shutdown_mode,
                claim_budget=claim_budget,
                join_timeout=join_timeout,
            )
            for index in range(workers)
        ]

    @property
    def all_stopped(self) -> bool:
        return all(not worker.is_alive for worker in self.workers)

    def start(self) -> None:
        for worker in self.workers:
            worker.start()

    def stop(self) -> None:
        for worker in self.workers:
            worker.stop()

    def join(self, timeout: float | None = None) -> bool:
        validate_join_timeout(timeout)
        if timeout is None:
            for worker in self.workers:
                worker.join(None)
            return self.all_stopped
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.all_stopped:
                break
            time.sleep(0.005)
        for worker in self.workers:
            remaining = max(0.0, deadline - time.monotonic())
            worker.join(remaining)
        return self.all_stopped

    def __enter__(self) -> WorkerPool:
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.stop()
        self.join(self.join_timeout)
