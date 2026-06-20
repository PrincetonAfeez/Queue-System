""" Stats command. """

from __future__ import annotations

import argparse
from typing import Any

from simplequeue.cli._shared import make_queue, print_json
from simplequeue.config import QueueConfig


def register(subparsers: Any, parents: list[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("stats", parents=parents, help="show queue stats")
    parser.add_argument("--queue", help="queue name")
    parser.add_argument("--no-cache", action="store_true")
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace, config: QueueConfig) -> int:
    queue = make_queue(args, config)
    queue.init_schema()
    print_json(queue.stats(cached=not args.no_cache).to_dict())
    return 0
