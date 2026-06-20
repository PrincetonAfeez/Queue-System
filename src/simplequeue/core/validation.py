""" Validation for the core queue domain. """

from __future__ import annotations

import math

_MAX_QUEUE_NAME_LENGTH = 256


def validate_queue_name(name: str) -> str:
    if not name or not name.strip():
        raise ValueError("queue_name must be a non-empty string")
    if name != name.strip():
        raise ValueError("queue_name must not have leading or trailing whitespace")
    if len(name) > _MAX_QUEUE_NAME_LENGTH:
        raise ValueError(f"queue_name must be at most {_MAX_QUEUE_NAME_LENGTH} characters")
    return name


def require_finite(value: float, *, field: str) -> None:
    if not math.isfinite(value):
        raise ValueError(f"{field!r} must be a finite number")


def require_finite_positive(value: float, *, field: str) -> None:
    require_finite(value, field=field)
    if value <= 0:
        raise ValueError(f"{field!r} must be > 0")


def require_finite_non_negative(value: float, *, field: str) -> None:
    require_finite(value, field=field)
    if value < 0:
        raise ValueError(f"{field!r} must be >= 0")


def validate_join_timeout(timeout: float | None, *, field: str = "join_timeout") -> None:
    if timeout is not None:
        require_finite_non_negative(timeout, field=field)
