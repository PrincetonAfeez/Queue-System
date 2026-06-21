from __future__ import annotations

from simplequeue.teaching.unsafe_ack_by_message_id import demonstrate_stale_ack_corruption
from simplequeue.teaching.unsafe_cache_as_source_of_truth import demonstrate_cache_correctness_bug
from simplequeue.teaching.unsafe_no_visibility_timeout import demonstrate_stuck_message
from simplequeue.teaching.unsafe_select_then_update import demonstrate_double_claim


def test_unsafe_double_claim_demo_fails_by_design() -> None:
    result = demonstrate_double_claim()
    assert result["double_claimed"] is True


def test_unsafe_ack_by_message_id_demo_fails_by_design() -> None:
    result = demonstrate_stale_ack_corruption()
    assert result["corrupted"] is True


def test_unsafe_no_visibility_timeout_demo_fails_by_design() -> None:
    result = demonstrate_stuck_message()
    assert result["stuck_forever"] is True


def test_unsafe_cache_source_of_truth_demo_fails_by_design() -> None:
    result = demonstrate_cache_correctness_bug()
    assert result["corrupted"] is True
