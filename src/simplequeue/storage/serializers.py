""" Serializers for the storage layer. """

from __future__ import annotations

import json
from typing import Any

from simplequeue.core.exceptions import StorageError


def dumps(payload: Any) -> str:
    try:
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)
    except (TypeError, ValueError) as error:
        raise StorageError(f"payload is not JSON-serializable: {error}") from error


def loads(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError as error:
        raise StorageError(f"stored payload is not valid JSON: {error}") from error
