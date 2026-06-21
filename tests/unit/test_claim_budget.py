from __future__ import annotations

import threading

import pytest

from simplequeue.workers.claim_budget import ClaimBudget


def test_claim_budget_rejects_non_positive_limit() -> None:
    with pytest.raises(ValueError, match="limit must be >= 1"):
        ClaimBudget(0)


def test_claim_budget_caps_acquires() -> None:
    budget = ClaimBudget(3)
    assert budget.try_acquire()
    assert budget.try_acquire()
    assert budget.try_acquire()
    assert not budget.try_acquire()


def test_claim_budget_release_unused_allows_reacquire() -> None:
    budget = ClaimBudget(1)
    assert budget.try_acquire()
    assert not budget.try_acquire()
    budget.release_unused()
    assert budget.try_acquire()


def test_claim_budget_concurrent_cap() -> None:
    budget = ClaimBudget(5)
    acquired = 0
    lock = threading.Lock()

    def worker() -> None:
        nonlocal acquired
        for _ in range(20):
            if budget.try_acquire():
                with lock:
                    acquired += 1
                budget.release_unused()

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert acquired >= 5
