""" Initialize the SQLite schema. """

from __future__ import annotations

import argparse
from typing import Any

from simplequeue.cli._shared import print_json
from simplequeue.config import QueueConfig
from simplequeue.storage.factory import create_backend


def register(subparsers: Any, parents: list[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("init-db", parents=parents, help="initialize the SQLite schema")
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace, config: QueueConfig) -> int:
    backend = create_backend(config)
    backend.init_schema()
    print_json({"db": config.database_path, "initialized": True})
    return 0
