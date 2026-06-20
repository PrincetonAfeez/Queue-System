""" Worker for the workers layer. """

from __future__ import annotations

import logging
import threading
import uuid

from simplequeue.core.delivery import Delivery
from simplequeue.core.modes import DeliveryMode
from simplequeue.core.queue import Queue
from simplequeue.core.validation import require_finite_positive, validate_join_timeout
from simplequeue.defaults import (
    DEFAULT_JOIN_TIMEOUT,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_VISIBILITY_TIMEOUT,
)
from simplequeue.observability import events
from simplequeue.observability.logging import get_logger, log_event
from simplequeue.workers.claim_budget import ClaimBudget
from simplequeue.workers.processor import Processor
from simplequeue.workers.shutdown import ShutdownMode


class Worker:
    def __init__(
        self,
        queue: Queue,
        processor: Processor,
        *,
        worker_id: str | None = None,
        delivery_mode: DeliveryMode | str = DeliveryMode.AT_LEAST_ONCE,
        visibility_timeout: float = DEFAULT_VISIBILITY_TIMEOUT,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        shutdown_mode: ShutdownMode | str = ShutdownMode.FINISH_CURRENT,
        claim_budget: ClaimBudget | None = None,
        join_timeout: float | None = DEFAULT_JOIN_TIMEOUT,
        logger: logging.Logger | None = None,
    ) -> None:
        require_finite_positive(visibility_timeout, field="visibility_timeout")
        require_finite_positive(poll_interval, field="poll_interval")
        if join_timeout is not None:
            validate_join_timeout(join_timeout)
        self.queue = queue
        self.processor = processor
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.delivery_mode = DeliveryMode.parse(delivery_mode)
        self.visibility_timeout = visibility_timeout
        self.poll_interval = poll_interval
        self.shutdown_mode = ShutdownMode.parse(shutdown_mode)
        self.claim_budget = claim_budget
        self.join_timeout = join_timeout
        self.logger = logger or get_logger()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, name=self.worker_id, daemon=False)

    @property
    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def start(self) -> None:
        if self._thread.is_alive():
            return
        log_event(self.logger, events.WORKER_START, worker_id=self.worker_id, queue_name=self.queue.queue_name)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def join(self, timeout: float | None = None) -> bool:
        validate_join_timeout(timeout)
        self._thread.join(timeout)
        return not self._thread.is_alive()

    def __enter__(self) -> Worker:
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.stop()
        self.join(self.join_timeout)

    def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                if self.claim_budget is not None:
                    if not self.claim_budget.try_acquire():
                        self.queue.clock.wait(self._stop_event, self.poll_interval)
                        continue
                delivery = self.queue.dequeue(
                    delivery_mode=self.delivery_mode,
                    visibility_timeout=self.visibility_timeout,
                    worker_id=self.worker_id,
                )
                if delivery is None:
                    if self.claim_budget is not None:
                        self.claim_budget.release_unused()
                    self.queue.clock.wait(self._stop_event, self.poll_interval)
                    continue

                # A stop arriving between claim and processing is handled per the
                # configured shutdown mode so the delivery guarantee stays true.
                if self._stop_event.is_set() and self.shutdown_mode is not ShutdownMode.FINISH_CURRENT:
                    self._handle_interrupted_delivery(delivery)
                    break

                try:
                    result = self.processor(delivery)
                except Exception as error:
                    log_event(
                        self.logger,
                        events.WORKER_FAILURE,
                        worker_id=self.worker_id,
                        queue_name=self.queue.queue_name,
                        message_id=delivery.message_id,
                        receipt_handle=delivery.receipt_handle[:12],
                        error=str(error),
                    )
                    if self.delivery_mode is DeliveryMode.AT_LEAST_ONCE:
                        if self._stop_event.is_set() and self.shutdown_mode is ShutdownMode.ABANDON_CURRENT:
                            break
                        if self._stop_event.is_set() and self.shutdown_mode is ShutdownMode.NACK_CURRENT:
                            self._safe_nack(delivery, "worker shutting down")
                            break
                        self._safe_nack(delivery, str(error))
                    continue

                if self._stop_event.is_set() and self.shutdown_mode is not ShutdownMode.FINISH_CURRENT:
                    self._handle_interrupted_delivery(delivery)
                    break

                self._finish(delivery, result)
        finally:
            log_event(self.logger, events.WORKER_STOP, worker_id=self.worker_id, queue_name=self.queue.queue_name)

    def _finish(self, delivery: Delivery, result: bool | None) -> None:
        if self.delivery_mode is not DeliveryMode.AT_LEAST_ONCE:
            if result is False:
                log_event(
                    self.logger,
                    events.WORKER_FAILURE,
                    worker_id=self.worker_id,
                    queue_name=self.queue.queue_name,
                    message_id=delivery.message_id,
                    receipt_handle=delivery.receipt_handle[:12],
                    error="processor returned false in at-most-once mode (message already deleted)",
                    level=logging.WARNING,
                )
            return  # at-most-once is already terminal at delivery time
        if result is False:
            self._safe_nack(delivery, "processor returned false")
        else:
            self._safe_ack(delivery)

    def _handle_interrupted_delivery(self, delivery: Delivery) -> None:
        if self.delivery_mode is not DeliveryMode.AT_LEAST_ONCE:
            return  # at-most-once message is gone; nothing to release
        if self.shutdown_mode is ShutdownMode.NACK_CURRENT:
            self._safe_nack(delivery, "worker shutting down")
        # ABANDON_CURRENT: leave the lease to expire; the sweeper redelivers it.

    def _safe_ack(self, delivery: Delivery) -> None:
        try:
            result = self.queue.ack(delivery.receipt_handle)
            if not result.success:
                log_event(
                    self.logger,
                    events.WORKER_FAILURE,
                    worker_id=self.worker_id,
                    queue_name=self.queue.queue_name,
                    message_id=delivery.message_id,
                    receipt_handle=delivery.receipt_handle[:12],
                    error=f"ack rejected: {result.reason}",
                    level=logging.WARNING,
                )
        except Exception as error:  # never let an ack failure kill the worker loop
            log_event(
                self.logger,
                events.WORKER_FAILURE,
                worker_id=self.worker_id,
                queue_name=self.queue.queue_name,
                message_id=delivery.message_id,
                receipt_handle=delivery.receipt_handle[:12],
                error=f"ack failed: {error}",
            )

    def _safe_nack(self, delivery: Delivery, reason: str) -> None:
        try:
            result = self.queue.nack(delivery.receipt_handle, reason=reason)
            if not result.success:
                log_event(
                    self.logger,
                    events.WORKER_FAILURE,
                    worker_id=self.worker_id,
                    queue_name=self.queue.queue_name,
                    message_id=delivery.message_id,
                    receipt_handle=delivery.receipt_handle[:12],
                    error=f"nack rejected: {result.reason}",
                    level=logging.WARNING,
                )
        except Exception as error:  # never let a nack failure kill the worker loop
            log_event(
                self.logger,
                events.WORKER_FAILURE,
                worker_id=self.worker_id,
                queue_name=self.queue.queue_name,
                message_id=delivery.message_id,
                receipt_handle=delivery.receipt_handle[:12],
                error=f"nack failed: {error}",
            )
