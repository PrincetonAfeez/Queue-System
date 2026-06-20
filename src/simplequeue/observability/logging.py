""" Logging for the observability layer. """

from __future__ import annotations

import json
import logging
from typing import Any


def get_logger(name: str = "simplequeue") -> logging.Logger:
    return logging.getLogger(name)


def log_event(logger: logging.Logger, event: str, *, level: int = logging.INFO, **fields: Any) -> None:
    payload = {"event": event, **fields}
    logger.log(level, json.dumps(payload, default=str, sort_keys=True))
