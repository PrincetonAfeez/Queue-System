""" Purge command. """

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from typing import Any

from simplequeue.cli._shared import make_queue, print_json
from simplequeue.config import QueueConfig
from simplequeue.core.queue import Queue
from simplequeue.core.validation import require_finite_non_negative


def register(subparsers: Any, parents: list[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "purge",
        parents=parents,
        help="delete old terminal message rows (acked/deleted; optional dead-lettered)",
    )
    parser.add_argument("--queue", help="queue name")
    parser.add_argument(
        "--all-queues",
        action="store_true",
        help="purge terminal rows for every queue in the database",
    )
    parser.add_argument(
        "--older-than-days",
        type=float,
        help="purge rows older than this many days (0 = through now; default: 7)",
    )
    parser.add_argument(
        "--older-than",
        help="ISO-8601 cutoff timestamp (mutually exclusive with --older-than-days)",
    )
    parser.add_argument(
        "--include-dead-lettered",
        action="store_true",
        help="also purge dead_lettered rows past the cutoff",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="count eligible rows without deleting",
    )
    parser.set_defaults(handler=run)


def _parse_older_than(args: argparse.Namespace, queue: Queue) -> datetime | None:
    if args.older_than is not None and args.older_than_days is not None:
        raise ValueError("pass only one of --older-than or --older-than-days")
    if args.older_than is not None:
        parsed = datetime.fromisoformat(args.older_than)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=queue.clock.now().tzinfo)
        return parsed
    if args.older_than_days is not None:
        require_finite_non_negative(args.older_than_days, field="older_than_days")
        now = queue.clock.now()
        if args.older_than_days == 0:
            return now
        return now - timedelta(days=args.older_than_days)
    return None


def run(args: argparse.Namespace, config: QueueConfig) -> int:
    all_queues = getattr(args, "all_queues", False)
    if all_queues and args.queue is not None:
        raise ValueError("pass only one of --queue or --all-queues")
    queue = make_queue(args, config)
    queue.init_schema()
    older_than = _parse_older_than(args, queue)
    queue_names = queue.list_queues() if all_queues else [queue.queue_name]
    results: list[dict[str, Any]] = []
    removed_total = 0
    dry_run = bool(getattr(args, "dry_run", False))
    for name in queue_names:
        removed = queue.purge_terminal(
            older_than=older_than,
            queue_name=name,
            include_dead_lettered=args.include_dead_lettered,
            dry_run=dry_run,
        )
        removed_total += removed
        results.append({"queue": name, "removed": removed})
    print_json(
        {
            "queues": results,
            "removed_total": removed_total,
            "dry_run": dry_run,
            "include_dead_lettered": args.include_dead_lettered,
            "older_than_days": args.older_than_days,
            "older_than": args.older_than,
            "all_queues": all_queues,
        }
    )
    return 0
