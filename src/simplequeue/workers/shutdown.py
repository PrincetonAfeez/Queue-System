""" Shutdown modes for the workers layer. """

from __future__ import annotations

from enum import Enum


class ShutdownMode(str, Enum):
    FINISH_CURRENT = "finish-current"
    NACK_CURRENT = "nack-current"
    ABANDON_CURRENT = "abandon-current"

    @classmethod
    def parse(cls, value: str | ShutdownMode) -> ShutdownMode:
        if isinstance(value, ShutdownMode):
            return value
        normalized = value.strip().lower().replace("_", "-")
        for mode in cls:
            if mode.value == normalized:
                return mode
        allowed = ", ".join(mode.value for mode in cls)
        raise ValueError(f"unknown shutdown mode {value!r}; expected one of: {allowed}")
