""" Main entry point for the CLI. """

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import fields
from typing import Any

from simplequeue import __version__
from simplequeue.cli.commands import (
    consume,
    demo,
    dlq,
    init_db,
    inspect,
    list_queues,
    peek,
    produce,
    purge,
    stats,
    sweep,
)
from simplequeue.cli.shutdown import install_graceful_shutdown_handlers
from simplequeue.config import QueueConfig, _validate_ranges, load_config
from simplequeue.core.exceptions import QueueError, StorageError


def main(argv: list[str] | None = None) -> int:
    install_graceful_shutdown_handlers()
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = _merged_config(args)
        logging.basicConfig(level=getattr(logging, config.logging_level.upper(), logging.INFO))
        if not hasattr(args, "handler"):
            parser.print_help()
            return 2
        return int(args.handler(args, config))
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    except QueueError as error:
        # Expected, domain-level failures (e.g. requeueing a non-DLQ message).
        print(f"simplequeue: {error}", file=sys.stderr)
        return 4
    except StorageError as error:
        print(f"simplequeue: {error}", file=sys.stderr)
        return 1
    except (ValueError, FileNotFoundError) as error:
        print(f"simplequeue: {error}", file=sys.stderr)
        return 2
    except Exception as error:
        print(f"simplequeue: {error}", file=sys.stderr)
        return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="simplequeue")
    parser.add_argument("--version", action="version", version=f"simplequeue {__version__}")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", help="JSON or TOML config file")
    common.add_argument("--db", help="SQLite database path")
    subparsers = parser.add_subparsers(dest="command")
    parents = [common]
    # Each command module owns its own argparse wiring and Queue-API handler.
    init_db.register(subparsers, parents)
    produce.register(subparsers, parents)
    consume.register(subparsers, parents)
    sweep.register(subparsers, parents)
    stats.register(subparsers, parents)
    list_queues.register(subparsers, parents)
    peek.register(subparsers, parents)
    inspect.register(subparsers, parents)
    dlq.register(subparsers, parents)
    purge.register(subparsers, parents)
    demo.register(subparsers, parents)
    return parser


def _merged_config(args: argparse.Namespace) -> QueueConfig:
    config = load_config(getattr(args, "config", None), command=getattr(args, "command", None))
    overrides: dict[str, Any] = {}
    if getattr(args, "db", None):
        overrides["database_path"] = args.db
    if getattr(args, "queue", None):
        overrides["queue_name"] = args.queue
    if getattr(args, "mode", None):
        overrides["delivery_mode"] = args.mode
    # Numeric flags use explicit None checks so an intentional 0 is not dropped.
    if getattr(args, "workers", None) is not None:
        overrides["worker_count"] = args.workers
    if getattr(args, "visibility_timeout", None) is not None:
        overrides["visibility_timeout"] = args.visibility_timeout
    if getattr(args, "max_attempts", None) is not None:
        overrides["max_attempts"] = args.max_attempts
    values = {field.name: getattr(config, field.name) for field in fields(config)}
    values.update(overrides)
    merged = QueueConfig(**values)
    _validate_ranges(merged, command=getattr(args, "command", None))
    return merged


if __name__ == "__main__":
    raise SystemExit(main())
