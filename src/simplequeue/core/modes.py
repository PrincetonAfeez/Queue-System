""" Delivery modes. """

from __future__ import annotations

from enum import Enum


class DeliveryMode(str, Enum):
    AT_MOST_ONCE = "at-most-once"
    AT_LEAST_ONCE = "at-least-once"

    @classmethod
    def parse(cls, value: str | DeliveryMode) -> DeliveryMode:
        if isinstance(value, DeliveryMode):
            return value
        normalized = value.strip().lower().replace("_", "-")
        for mode in cls:
            if mode.value == normalized:
                return mode
        allowed = ", ".join(mode.value for mode in cls)
        raise ValueError(f"unknown delivery mode {value!r}; expected one of: {allowed}")
