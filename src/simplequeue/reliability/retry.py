""" Retry decision. """

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetryDecision:
    should_retry: bool
    should_dead_letter: bool


def decide_retry(attempts: int, max_attempts: int) -> RetryDecision:
    exhausted = attempts >= max_attempts
    return RetryDecision(should_retry=not exhausted, should_dead_letter=exhausted)
