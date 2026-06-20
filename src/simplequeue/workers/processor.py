""" Processor for the workers layer. """

from __future__ import annotations

from typing import Protocol

from simplequeue.core.delivery import Delivery


class Processor(Protocol):
    def __call__(self, delivery: Delivery) -> bool | None:
        """Process a delivery.

        Return ``False`` to nack explicitly (at-least-once only); return ``True``
        or ``None`` to ack. Raising an exception nacks in at-least-once mode.

        In at-most-once mode the message is deleted at claim time, so ``False``
        and ack/nack have no effect on queue state.
        """
