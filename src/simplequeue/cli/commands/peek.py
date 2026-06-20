""" Peek command. """

from __future__ import annotations

import argparse
from typing import Any

from simplequeue.cli._shared import _jsonable, make_queue, print_json
from simplequeue.config import QueueConfig


def register(subparsers: Any, parents: list[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("peek", parents=parents, help="peek at messages")
    parser.add_argument("--queue", help="queue name")
    parser.add_argument("--limit", type=int, default=10)
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace, config: QueueConfig) -> int:
    if args.limit < 1:
        raise ValueError("--limit must be >= 1")
    queue = make_queue(args, config)
    queue.init_schema()
    print_json({"messages": [_jsonable(message) for message in queue.peek(limit=args.limit)]})
    return 0
