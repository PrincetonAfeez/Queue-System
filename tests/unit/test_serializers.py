from __future__ import annotations

from datetime import datetime

import pytest

from simplequeue.core.exceptions import StorageError
from simplequeue.storage.serializers import dumps, loads


def test_serializers_round_trip() -> None:
    payload = {"a": 1, "b": [2, 3]}
    assert loads(dumps(payload)) == payload


def test_dumps_rejects_non_serializable() -> None:
    with pytest.raises(StorageError, match="JSON-serializable"):
        dumps({"x": datetime.now()})


def test_loads_rejects_corrupt_json() -> None:
    with pytest.raises(StorageError, match="valid JSON"):
        loads("{not-json")
