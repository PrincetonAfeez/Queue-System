""" Demo command. """

from __future__ import annotations

import argparse
from typing import Any

from simplequeue.cli.demos import DEMOS, dumps_demo, run_demo
from simplequeue.config import QueueConfig


def register(subparsers: Any, parents: list[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("demo", parents=parents, help="run a reproducible demo")
    parser.add_argument("demo_name", choices=sorted(DEMOS))
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace, config: QueueConfig) -> int:
    explicit_db = getattr(args, "db", None) is not None or getattr(args, "config", None) is not None
    db_path = config.database_path if explicit_db else None
    result = run_demo(args.demo_name, db_path=db_path)
    print(dumps_demo(result))
    return 0
