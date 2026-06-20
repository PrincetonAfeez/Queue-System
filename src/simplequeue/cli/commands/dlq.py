""" Dead-letter command. """

from __future__ import annotations

import argparse
from typing import Any

from simplequeue.cli._shared import _jsonable, make_queue, print_json
from simplequeue.config import QueueConfig


def register(subparsers: Any, parents: list[argparse.ArgumentParser]) -> None:
    listing = subparsers.add_parser("dlq", parents=parents, help="list dead-lettered messages")
    listing.add_argument("--queue", help="queue name")
    listing.add_argument(
        "--all-queues",
        action="store_true",
        help="list dead letters across every queue in the database",
    )
    listing.set_defaults(handler=run_list)

    requeue = subparsers.add_parser(
        "dlq-requeue", parents=parents, help="requeue a dead-lettered message"
    )
    requeue.add_argument("--message-id", type=int, required=True)
    requeue.add_argument("--queue", help="queue name")
    requeue.set_defaults(handler=run_requeue)


def run_list(args: argparse.Namespace, config: QueueConfig) -> int:
    all_queues = getattr(args, "all_queues", False)
    if all_queues and args.queue is not None:
        raise ValueError("pass only one of --queue or --all-queues")
    queue = make_queue(args, config)
    queue.init_schema()
    if all_queues:
        dead_letters = queue.list_dead_letters(all_queues=True)
    else:
        dead_letters = queue.list_dead_letters()
    print_json({"dead_letters": [_jsonable(dead) for dead in dead_letters]})
    return 0


def run_requeue(args: argparse.Namespace, config: QueueConfig) -> int:
    queue = make_queue(args, config)
    queue.init_schema()
    message_id = queue.requeue_dead_letter(args.message_id)
    print_json({"requeued_message_id": message_id})
    return 0
