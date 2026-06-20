""" Configuration for the library. """

from __future__ import annotations

import json
import logging
import tomllib
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from simplequeue.core.modes import DeliveryMode
from simplequeue.core.validation import require_finite, require_finite_positive, validate_queue_name
from simplequeue.defaults import (
    DEFAULT_CACHE_TTL,
    DEFAULT_DATABASE_PATH,
    DEFAULT_DELIVERY_MODE,
    DEFAULT_IDLE_TIMEOUT,
    DEFAULT_LOGGING_LEVEL,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_QUEUE_NAME,
    DEFAULT_SHUTDOWN_MODE,
    DEFAULT_SWEEPER_INTERVAL,
    DEFAULT_VISIBILITY_TIMEOUT,
    DEFAULT_WORKER_COUNT,
)
from simplequeue.workers.shutdown import ShutdownMode


@dataclass(frozen=True, slots=True)
class QueueConfig:
    queue_name: str = DEFAULT_QUEUE_NAME
    delivery_mode: str = DEFAULT_DELIVERY_MODE
    visibility_timeout: float = DEFAULT_VISIBILITY_TIMEOUT
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    backend: str = "sqlite"
    worker_count: int = DEFAULT_WORKER_COUNT
    cache_ttl: float = DEFAULT_CACHE_TTL
    database_path: str = DEFAULT_DATABASE_PATH
    logging_level: str = DEFAULT_LOGGING_LEVEL
    sweeper_interval: float = DEFAULT_SWEEPER_INTERVAL
    poll_interval: float = DEFAULT_POLL_INTERVAL
    idle_timeout: float = DEFAULT_IDLE_TIMEOUT
    shutdown_mode: str = DEFAULT_SHUTDOWN_MODE


def load_config(path: str | Path | None, *, command: str | None = None) -> QueueConfig:
    if path is None:
        config = QueueConfig()
        _validate_ranges(config, command=command)
        return config
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"config file does not exist: {config_path}")
    # utf-8-sig tolerates a UTF-8 BOM (common from Windows editors / PowerShell),
    # which a plain utf-8 read would leave in place and make json/tomllib choke on.
    if config_path.suffix.lower() == ".json":
        data = json.loads(config_path.read_text(encoding="utf-8-sig"))
    elif config_path.suffix.lower() in {".toml", ".tml"}:
        data = tomllib.loads(config_path.read_text(encoding="utf-8-sig"))
    else:
        raise ValueError("config files must be JSON or TOML (.json, .toml, or .tml)")
    if "queue" in data and isinstance(data["queue"], dict):
        data = data["queue"]
    allowed = set(QueueConfig.__dataclass_fields__)
    filtered: dict[str, Any] = {key: value for key, value in data.items() if key in allowed}
    config = replace(QueueConfig(), **_coerce(filtered))
    _validate_ranges(config, command=command)
    return config


_FLOAT_FIELDS = {
    "visibility_timeout",
    "cache_ttl",
    "sweeper_interval",
    "poll_interval",
    "idle_timeout",
}
_INT_FIELDS = {"max_attempts", "worker_count"}
_SUPPORTED_BACKENDS = {"sqlite"}


def _coerce(values: dict[str, Any]) -> dict[str, Any]:
    """Coerce and validate config values with a clear, early error.

    A typo such as ``visibility_timeout = "30"`` would otherwise flow through
    untyped and fail much later inside a ``timedelta`` call, and an invalid
    delivery mode or backend would only surface at consume time (or never).
    """

    coerced: dict[str, Any] = {}
    for key, value in values.items():
        try:
            if key in _FLOAT_FIELDS:
                coerced[key] = float(value)
            elif key in _INT_FIELDS:
                if isinstance(value, float) and not value.is_integer():
                    raise ValueError("expected a whole number")
                coerced[key] = int(value)
            else:
                coerced[key] = str(value)
        except (TypeError, ValueError) as error:
            expected = "a number" if key in _FLOAT_FIELDS or key in _INT_FIELDS else "a string"
            raise ValueError(f"config value for {key!r} must be {expected}, got {value!r}") from error

    if "delivery_mode" in coerced:
        DeliveryMode.parse(coerced["delivery_mode"])  # raises ValueError if unknown
    if "shutdown_mode" in coerced:
        ShutdownMode.parse(coerced["shutdown_mode"])
    if "backend" in coerced and coerced["backend"] not in _SUPPORTED_BACKENDS:
        raise ValueError(
            f"config value for 'backend' must be one of {sorted(_SUPPORTED_BACKENDS)}, "
            f"got {coerced['backend']!r}"
        )
    if "logging_level" in coerced and coerced["logging_level"].upper() not in logging.getLevelNamesMapping():
        raise ValueError(f"config value for 'logging_level' is not a valid level: {coerced['logging_level']!r}")
    if "queue_name" in coerced:
        validate_queue_name(coerced["queue_name"])
    for key in _FLOAT_FIELDS:
        if key in coerced:
            require_finite(coerced[key], field=key)
    return coerced


_ATTEMPT_COMMANDS = {None, "consume", "produce"}
_CACHE_COMMANDS = {
    None,
    "consume",
    "produce",
    "stats",
    "peek",
    "inspect",
    "list-queues",
    "sweep",
    "dlq",
    "dlq-requeue",
    "purge",
    "demo",
}


def _validate_ranges(config: QueueConfig, *, command: str | None = None) -> None:
    validate_queue_name(config.queue_name)
    if command in _ATTEMPT_COMMANDS and config.max_attempts < 1:
        raise ValueError("config value for 'max_attempts' must be >= 1")
    if command in _CACHE_COMMANDS:
        require_finite_positive(config.cache_ttl, field="cache_ttl")
    worker_commands = {None, "consume"}
    if command in worker_commands:
        if config.worker_count < 1:
            raise ValueError("config value for 'worker_count' must be >= 1")
        require_finite_positive(config.visibility_timeout, field="visibility_timeout")
        require_finite_positive(config.sweeper_interval, field="sweeper_interval")
        require_finite_positive(config.poll_interval, field="poll_interval")
        require_finite_positive(config.idle_timeout, field="idle_timeout")


def validate_library_config(config: QueueConfig) -> None:
    """Validate ``QueueConfig`` for library factory entry points (``create_queue``).

    Unlike CLI command-scoped validation, ``max_attempts < 1`` is allowed here
    because ``create_backend()`` clamps it to the library default.
    """
    validate_queue_name(config.queue_name)
    require_finite_positive(config.cache_ttl, field="cache_ttl")
    require_finite_positive(config.visibility_timeout, field="visibility_timeout")
    require_finite_positive(config.sweeper_interval, field="sweeper_interval")
    require_finite_positive(config.poll_interval, field="poll_interval")
    require_finite_positive(config.idle_timeout, field="idle_timeout")
    if config.worker_count < 1:
        raise ValueError("config value for 'worker_count' must be >= 1")
