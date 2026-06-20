""" Demos for the CLI. """

from __future__ import annotations

import json
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any

from simplequeue.core.delivery import Delivery
from simplequeue.core.modes import DeliveryMode
from simplequeue.core.queue import Queue
from simplequeue.scheduling.clock import FakeClock
from simplequeue.storage.sqlite_backend import SQLiteBackend
from simplequeue.teaching.unsafe_ack_by_message_id import demonstrate_stale_ack_corruption
from simplequeue.teaching.unsafe_cache_as_source_of_truth import demonstrate_cache_correctness_bug
from simplequeue.teaching.unsafe_no_visibility_timeout import demonstrate_stuck_message
from simplequeue.teaching.unsafe_select_then_update import demonstrate_double_claim
from simplequeue.workers.worker_pool import WorkerPool


def run_demo(name: str, db_path: str | None = None) -> dict[str, Any]:
    if name == "all":
        return {
            demo: run_demo(demo, db_path=db_path)
            for demo in DEMOS
            if demo != "all" and not demo.startswith("unsafe")
        }
    if name not in DEMOS:
        raise ValueError(f"unknown demo {name!r}")

    if name.startswith("unsafe"):
        return _run_unsafe(name)

    if db_path is None:
        # ignore_cleanup_errors guards against a slow OS handle release on Windows
        # even though the backend now closes every connection promptly.
        with tempfile.TemporaryDirectory(
            prefix="simplequeue-demo-", ignore_cleanup_errors=True
        ) as tmp:
            return _run_safe_demo(name, str(Path(tmp) / "queue.db"))
    return _run_safe_demo(name, db_path)


def _run_safe_demo(name: str, db_path: str) -> dict[str, Any]:
    if name == "basic":
        return _demo_basic(db_path)
    if name == "concurrent-workers":
        return _demo_concurrent_workers(db_path)
    if name == "at-most-once-loss":
        return _demo_at_most_once_loss(db_path)
    if name == "at-least-once-redelivery":
        return _demo_at_least_once_redelivery(db_path)
    if name == "retry-dlq":
        return _demo_retry_dlq(db_path)
    if name == "receipt-handle-stale-ack":
        return _demo_receipt_handle_stale_ack(db_path)
    raise ValueError(f"demo {name!r} is not implemented")


def _queue(db_path: str, queue_name: str = "demo", *, clock: FakeClock | None = None) -> Queue:
    backend = SQLiteBackend(db_path)
    queue = Queue(backend, queue_name, clock=clock)
    queue.init_schema()
    return queue


def _demo_basic(db_path: str) -> dict[str, Any]:
    queue = _queue(db_path)
    ids = [queue.enqueue({"job": index}) for index in range(3)]
    processed: list[int] = []
    while delivery := queue.dequeue(worker_id="demo-worker"):
        processed.append(delivery.message_id)
        queue.ack(delivery.receipt_handle)
    return {"db": db_path, "enqueued": ids, "processed": processed, "stats": queue.stats(cached=False).to_dict()}


def _demo_concurrent_workers(db_path: str) -> dict[str, Any]:
    queue = _queue(db_path, "concurrent")
    total = 50
    for index in range(total):
        queue.enqueue({"job": index})

    seen: list[int] = []
    lock = Lock()

    def processor(delivery: Delivery) -> bool:
        with lock:
            seen.append(delivery.message_id)
        return True

    pool = WorkerPool(queue, processor, workers=4, visibility_timeout=5, poll_interval=0.02)
    pool.start()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and queue.stats(cached=False).acked < total:
        time.sleep(0.02)
    pool.stop()
    pool.join()
    return {
        "db": db_path,
        "processed": len(seen),
        "unique_processed": len(set(seen)),
        "double_delivered": len(seen) != len(set(seen)),
        "stats": queue.stats(cached=False).to_dict(),
    }


def _demo_at_most_once_loss(db_path: str) -> dict[str, Any]:
    queue = _queue(db_path, "loss")
    message_id = queue.enqueue({"important": False})
    delivery = queue.dequeue(delivery_mode=DeliveryMode.AT_MOST_ONCE, worker_id="crashy")
    second = queue.dequeue(delivery_mode=DeliveryMode.AT_MOST_ONCE, worker_id="next-worker")
    details = queue.inspect(message_id)
    return {
        "db": db_path,
        "delivered_once": delivery.message_id if delivery else None,
        "redelivered_after_crash": second is not None,
        "final_status": details.message.status.value if details else None,
        "explanation": "at-most-once deletes at delivery time, so a crash loses the work",
    }


def _demo_at_least_once_redelivery(db_path: str) -> dict[str, Any]:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = _queue(db_path, "redelivery", clock=clock)
    message_id = queue.enqueue({"task": "retry-me"})
    first = queue.dequeue(visibility_timeout=5, worker_id="crashy")
    clock.advance(6)
    queue.sweep()
    second = queue.dequeue(visibility_timeout=5, worker_id="rescuer")
    if second:
        queue.ack(second.receipt_handle)
    return {
        "db": db_path,
        "message_id": message_id,
        "first_receipt": first.receipt_handle[:12] if first else None,
        "second_receipt": second.receipt_handle[:12] if second else None,
        "same_message_redelivered": bool(first and second and first.message_id == second.message_id),
        "stats": queue.stats(cached=False).to_dict(),
    }


def _demo_retry_dlq(db_path: str) -> dict[str, Any]:
    queue = _queue(db_path, "retry")
    message_id = queue.enqueue({"poison": True}, max_attempts=2)
    first = queue.dequeue(worker_id="worker-a")
    if first:
        queue.nack(first.receipt_handle, reason="forced failure 1")
    second = queue.dequeue(worker_id="worker-b")
    if second:
        queue.nack(second.receipt_handle, reason="forced failure 2")
    dlq = queue.list_dead_letters()
    requeued_id = queue.requeue_dead_letter(message_id)
    return {
        "db": db_path,
        "message_id": message_id,
        "dlq_size_before_requeue": len(dlq),
        "requeued_id": requeued_id,
        "stats": queue.stats(cached=False).to_dict(),
    }


def _demo_receipt_handle_stale_ack(db_path: str) -> dict[str, Any]:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = _queue(db_path, "receipts", clock=clock)
    queue.enqueue({"task": "receipt"})
    first = queue.dequeue(visibility_timeout=5, worker_id="old-worker")
    clock.advance(6)
    queue.sweep()
    second = queue.dequeue(visibility_timeout=5, worker_id="new-worker")
    stale_ack = queue.ack(first.receipt_handle) if first else None
    fresh_ack = queue.ack(second.receipt_handle) if second else None
    return {
        "db": db_path,
        "old_receipt": first.receipt_handle[:12] if first else None,
        "new_receipt": second.receipt_handle[:12] if second else None,
        "stale_ack_success": stale_ack.success if stale_ack else None,
        "fresh_ack_success": fresh_ack.success if fresh_ack else None,
    }


def _run_unsafe(name: str) -> dict[str, Any]:
    if name == "unsafe-double-claim":
        return demonstrate_double_claim()
    if name in {"unsafe-stale-ack", "unsafe_ack_by_message_id"}:
        return demonstrate_stale_ack_corruption()
    if name == "unsafe-no-visibility-timeout":
        return demonstrate_stuck_message()
    if name == "unsafe-cache-correctness":
        return demonstrate_cache_correctness_bug()
    raise ValueError(f"unsafe demo {name!r} is not implemented")


DEMOS = {
    "basic",
    "concurrent-workers",
    "at-most-once-loss",
    "at-least-once-redelivery",
    "retry-dlq",
    "receipt-handle-stale-ack",
    "unsafe-double-claim",
    "unsafe-stale-ack",
    "unsafe-no-visibility-timeout",
    "unsafe-cache-correctness",
    "all",
}


def dumps_demo(result: dict[str, Any]) -> str:
    return json.dumps(result, default=str, indent=2, sort_keys=True)
