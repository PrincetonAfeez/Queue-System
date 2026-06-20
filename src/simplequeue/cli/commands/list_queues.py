""" List queues command. """

from __future__ import annotations

import argparse
from typing import Any

from simplequeue.cli._shared import make_queue, print_json
from simplequeue.config import QueueConfig


def register(subparsers: Any, parents: list[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("list-queues", parents=parents, help="list queues")
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace, config: QueueConfig) -> int:
    queue = make_queue(args, config)
    queue.init_schema()
    print_json({"queues": queue.list_queues()})
    return 0
