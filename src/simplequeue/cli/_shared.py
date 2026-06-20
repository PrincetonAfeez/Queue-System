""" Shared utilities for CLI commands. """

from __future__ import annotations

import argparse
import json
from dataclasses import fields, is_dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from simplequeue.config import QueueConfig
from simplequeue.core.queue import Queue
from simplequeue.storage.factory import create_queue


def make_queue(args: argparse.Namespace, config: QueueConfig) -> Queue:
    return create_queue(config, getattr(args, "queue", None))


def _jsonable(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _jsonable(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    return value


def print_json(value: Any) -> None:
    print(json.dumps(_jsonable(value), indent=2, sort_keys=True))
