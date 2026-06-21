""" Verify command. """

from __future__ import annotations

import argparse
from typing import Any

from simplequeue.cli._shared import print_json
from simplequeue.config import QueueConfig
from simplequeue.storage.factory import create_backend
from simplequeue.storage.sqlite_backend import SQLiteBackend


def register(subparsers: Any, parents: list[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "verify",
        parents=parents,
        help="check SQLite integrity, schema version, and table readability",
    )
    parser.set_defaults(handler=run)


def run(args: argparse.Namespace, config: QueueConfig) -> int:
    backend = create_backend(config)
    if not isinstance(backend, SQLiteBackend):
        raise ValueError(f"verify is only supported for sqlite backend, not {config.backend!r}")
    result = backend.verify_database()
    print_json(result.to_dict())
    return 0 if result.healthy else 1
