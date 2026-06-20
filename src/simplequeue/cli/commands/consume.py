""" Consume command. """

from __future__ import annotations

import argparse
import sys
import threading
import time
from typing import Any

from simplequeue.cli._shared import make_queue, print_json
from simplequeue.config import QueueConfig
from simplequeue.core.delivery import Delivery
from simplequeue.core.modes import DeliveryMode
from simplequeue.core.validation import require_finite_positive
from simplequeue.defaults import DEFAULT_CONSUME_JOIN_TIMEOUT
from simplequeue.scheduling.sweeper import BackgroundSweeper
from simplequeue.workers.claim_budget import ClaimBudget
from simplequeue.workers.shutdown import ShutdownMode
from simplequeue.workers.worker_pool import WorkerPool


def _parse_shutdown_mode(value: str) -> ShutdownMode:
    return ShutdownMode.parse(value)


def _parse_delivery_mode(value: str) -> DeliveryMode:
    return DeliveryMode.parse(value)


def register(subparsers: Any, parents: list[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("consume", parents=parents, help="consume messages with worker threads")
    parser.add_argument("--queue", help="queue name")
    parser.add_argument("--mode", type=_parse_delivery_mode)
    parser.add_argument("--workers", type=int)
    parser.add_argument("--visibility-timeout", type=float)
    parser.add_argument("--limit", type=int, help="stop after processing this many messages")
    parser.add_argument("--duration", type=float, help="maximum run time in seconds")
    parser.add_argument("--idle-timeout", type=float, help="stop after this many idle seconds (default from config)")
    parser.add_argument("--poll-interval", type=float, help="worker poll interval in seconds (default from config)")
    parser.add_argument("--process-time", type=float, default=0.0, help="simulated handler latency in seconds")
    parser.add_argument(
        "--fail-every",
        type=int,
        default=0,
        help="return false from the processor every N messages (at-least-once only; 0 disables)",
    )
    parser.add_argument("--sweeper", action="store_true", help="run a background sweeper")
    parser.add_argument(
        "--sweeper-interval",
        type=float,
        help="background sweeper interval in seconds (default from config)",
    )
    parser.add_argument(
        "--shutdown-mode",
        type=_parse_shutdown_mode,
        help="how in-flight work is handled when the consumer stops (default from config)",
    )
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace, config: QueueConfig) -> int:
    if args.limit is not None and args.limit < 1:
        raise ValueError("--limit must be >= 1")
    if args.duration is not None and args.duration <= 0:
        raise ValueError("--duration must be > 0")
    if args.fail_every < 0:
        raise ValueError("--fail-every must be >= 0")
    if args.process_time < 0:
        raise ValueError("--process-time must be >= 0")
    queue = make_queue(args, config)
    queue.init_schema()
    processed = 0
    lock = threading.Lock()
    mode = DeliveryMode.parse(args.mode or config.delivery_mode)
    if mode is DeliveryMode.AT_MOST_ONCE and args.fail_every:
        print(
            "simplequeue: warning: --fail-every has no effect in at-most-once mode",
            file=sys.stderr,
        )
    bounded = args.limit is not None or args.duration is not None
    if (
        mode is DeliveryMode.AT_LEAST_ONCE
        and not args.sweeper
        and not bounded
    ):
        print(
            "simplequeue: warning: at-least-once consume without --sweeper reclaims expired "
            "leases only while workers are actively dequeuing; with no consumers, use "
            "--sweeper or run `simplequeue sweep` periodically",
            file=sys.stderr,
        )
    limit = args.limit
    poll_interval = args.poll_interval if args.poll_interval is not None else config.poll_interval
    idle_timeout = args.idle_timeout if args.idle_timeout is not None else config.idle_timeout
    shutdown_mode = ShutdownMode.parse(args.shutdown_mode or config.shutdown_mode)
    sweeper_interval = (
        args.sweeper_interval if args.sweeper_interval is not None else config.sweeper_interval
    )
    visibility_timeout = (
        args.visibility_timeout
        if args.visibility_timeout is not None
        else config.visibility_timeout
    )
    if args.poll_interval is not None:
        require_finite_positive(args.poll_interval, field="poll_interval")
    if args.idle_timeout is not None:
        require_finite_positive(args.idle_timeout, field="idle_timeout")
    if args.sweeper_interval is not None:
        require_finite_positive(args.sweeper_interval, field="sweeper_interval")
    if args.visibility_timeout is not None:
        require_finite_positive(args.visibility_timeout, field="visibility_timeout")
    if args.duration is not None:
        require_finite_positive(args.duration, field="duration")
    if args.process_time:
        require_finite_positive(args.process_time, field="process_time")
    require_finite_positive(poll_interval, field="poll_interval")
    require_finite_positive(idle_timeout, field="idle_timeout")
    require_finite_positive(sweeper_interval, field="sweeper_interval")
    require_finite_positive(visibility_timeout, field="visibility_timeout")

    stop_processing = threading.Event()

    def processor(delivery: Delivery) -> bool:
        nonlocal processed
        if args.process_time:
            deadline = time.monotonic() + args.process_time
            while time.monotonic() < deadline:
                if stop_processing.is_set():
                    return False
                time.sleep(min(0.05, deadline - time.monotonic()))
        with lock:
            processed += 1
            index = processed
        print_json(
            {
                "event": "processed",
                "message_id": delivery.message_id,
                "queue": delivery.queue_name,
                "attempt": delivery.attempt,
                "mode": delivery.delivery_mode.value,
                "receipt": delivery.receipt_handle[:12],
                "payload": delivery.payload,
            }
        )
        return not (args.fail_every and index % args.fail_every == 0)

    claim_budget = ClaimBudget(limit) if limit is not None else None
    pool = WorkerPool(
        queue,
        processor,
        workers=args.workers if args.workers is not None else config.worker_count,
        delivery_mode=mode,
        visibility_timeout=visibility_timeout,
        poll_interval=poll_interval,
        shutdown_mode=shutdown_mode,
        claim_budget=claim_budget,
    )
    sweeper = BackgroundSweeper(queue, interval=sweeper_interval) if args.sweeper else None
    start = time.monotonic()
    last_progress = start
    last_processed = 0
    try:
        if sweeper:
            sweeper.start()
        pool.start()
        while True:
            now = time.monotonic()
            with lock:
                count = processed
            if limit is not None and count >= limit:
                break
            if args.duration is not None and now - start >= args.duration:
                break
            stats = queue.stats(cached=False)
            if count != last_processed:
                last_processed = count
                last_progress = now
            # Idle drain-exit applies to any bounded run (limit or duration). With
            # neither flag set the consumer runs until interrupted (Ctrl-C / SIGTERM).
            bounded = limit is not None or args.duration is not None
            if (
                bounded
                and stats.current_depth == 0
                and stats.scheduled_count == 0
                and stats.in_flight_count == 0
            ):
                if now - last_progress >= idle_timeout:
                    break
            time.sleep(poll_interval)
    finally:
        stop_processing.set()
        pool.stop()
        if not pool.join(DEFAULT_CONSUME_JOIN_TIMEOUT):
            print(
                "simplequeue: warning: workers did not stop within join timeout",
                file=sys.stderr,
            )
        if sweeper:
            sweeper.stop()
            if not sweeper.join(DEFAULT_CONSUME_JOIN_TIMEOUT):
                print(
                    "simplequeue: warning: sweeper did not stop within join timeout",
                    file=sys.stderr,
                )
    print_json({"event": "consume_finished", "processed": processed, "stats": queue.stats(cached=False).to_dict()})
    return 0
