""" Inspect command. """

from __future__ import annotations

import argparse
from typing import Any

from simplequeue.cli._shared import _jsonable, make_queue, print_json
from simplequeue.config import QueueConfig


def register(subparsers: Any, parents: list[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("inspect", parents=parents, help="inspect one message")
    parser.add_argument("--message-id", type=int, required=True)
    parser.add_argument("--queue", help="queue name")
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace, config: QueueConfig) -> int:
    queue = make_queue(args, config)
    queue.init_schema()
    details = queue.inspect(args.message_id)
    if details is None:
        print_json({"message_id": args.message_id, "found": False})
        return 3
    if details.message.queue_name != queue.queue_name:
        print_json(
            {
                "message_id": args.message_id,
                "found": False,
                "reason": f"message belongs to queue {details.message.queue_name!r}, not {queue.queue_name!r}",
            }
        )
        return 3
    print_json(_jsonable(details))
    return 0
