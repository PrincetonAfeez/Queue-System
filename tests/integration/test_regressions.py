from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from simplequeue.scheduling.clock import FakeClock


def test_idempotency_key_is_reusable_after_message_completes(queue_factory) -> None:
    """A key only dedupes live work; once the message is acked it can be reused."""
    queue = queue_factory("idemp")
    first = queue.enqueue({"job": 1}, idempotency_key="k")
    queue.ack(queue.dequeue(worker_id="w").receipt_handle)

    second = queue.enqueue({"job": 1}, idempotency_key="k")
    assert second != first  # a brand-new message, not the terminal one

    redelivered = queue.dequeue(worker_id="w2")
    assert redelivered is not None
    assert redelivered.message_id == second


def test_idempotency_key_still_dedupes_live_messages(queue_factory) -> None:
    queue = queue_factory("idemp-live")
    first = queue.enqueue({"job": 1}, idempotency_key="k")
    second = queue.enqueue({"job": 1}, idempotency_key="k")  # still pending
    assert first == second
    assert queue.stats(cached=False).enqueued == 1


def test_enqueue_rejects_non_positive_max_attempts(queue_factory) -> None:
    queue = queue_factory("validate")
    with pytest.raises(ValueError):
        queue.enqueue({"x": 1}, max_attempts=0)


def test_stats_recent_window_uses_injected_clock(queue_factory) -> None:
    """recent_worker_ids/throughput must be computed from the queue clock, not wall time."""
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("stats-clock", clock=clock)
    queue.enqueue({"x": 1})
    delivery = queue.dequeue(worker_id="w1")
    assert delivery is not None
    queue.ack(delivery.receipt_handle)

    snapshot = queue.stats(cached=False)
    assert snapshot.acked == 1
    assert snapshot.recent_worker_ids >= 1  # fake-dated events fall inside the fake window
    assert snapshot.recent_throughput > 0.0


def test_fake_clock_wait_is_interruptible_and_does_not_advance_time() -> None:
    from threading import Event

    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    before = clock.now()
    stop = Event()
    stop.set()
    # A set event returns immediately; logical time must not move.
    assert clock.wait(stop, 5.0) is True
    assert clock.now() == before


def test_enqueue_uses_injected_clock_for_created_at(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("clock-domain", clock=clock)
    details = queue.inspect(queue.enqueue({"x": 1}))
    assert details is not None
    # created_at and available_at must share the injected clock domain.
    assert details.message.created_at == clock.now()
    assert details.message.available_at == clock.now()


def test_load_config_coerces_and_validates_types(tmp_path) -> None:
    from simplequeue.config import load_config

    good = tmp_path / "good.json"
    good.write_text(json.dumps({"visibility_timeout": "12", "worker_count": "3"}), encoding="utf-8")
    config = load_config(str(good))
    assert config.visibility_timeout == 12.0
    assert isinstance(config.visibility_timeout, float)
    assert config.worker_count == 3

    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"visibility_timeout": "soon"}), encoding="utf-8")
    with pytest.raises(ValueError):
        load_config(str(bad))


def test_load_config_rejects_invalid_enums_and_fractional_ints(tmp_path) -> None:
    from simplequeue.config import load_config

    for bad in (
        {"delivery_mode": "sometimes"},
        {"backend": "postgres"},
        {"logging_level": "LOUD"},
        {"worker_count": 2.5},
    ):
        path = tmp_path / "c.json"
        path.write_text(json.dumps(bad), encoding="utf-8")
        with pytest.raises(ValueError):
            load_config(str(path))


def test_load_config_tolerates_utf8_bom(tmp_path) -> None:
    from simplequeue.config import load_config

    path = tmp_path / "bom.toml"
    # A UTF-8 BOM, as a Windows editor or PowerShell Set-Content often writes.
    path.write_bytes(b"\xef\xbb\xbf" + b'queue_name = "emails"\nworker_count = 2\n')
    config = load_config(str(path))
    assert config.queue_name == "emails"
    assert config.worker_count == 2


def test_load_config_unwraps_queue_table(tmp_path) -> None:
    from simplequeue.config import load_config

    path = tmp_path / "wrapped.toml"
    path.write_text('[queue]\nqueue_name = "wrapped"\nvisibility_timeout = 9\n', encoding="utf-8")
    config = load_config(str(path))
    assert config.queue_name == "wrapped"
    assert config.visibility_timeout == 9.0


def test_load_config_ignores_unknown_keys(tmp_path) -> None:
    from simplequeue.config import load_config

    path = tmp_path / "extra.json"
    path.write_text('{"queue_name": "ok", "extra_field": "ignored"}', encoding="utf-8")
    config = load_config(str(path))
    assert config.queue_name == "ok"
    assert config.delivery_mode == "at-least-once"


def test_load_config_accepts_tml_extension(tmp_path) -> None:
    from simplequeue.config import load_config

    path = tmp_path / "config.tml"
    path.write_text('queue_name = "from-tml"\n', encoding="utf-8")
    config = load_config(str(path))
    assert config.queue_name == "from-tml"


def test_backend_operation_before_init_raises_storage_error(tmp_path) -> None:
    from simplequeue.core.exceptions import StorageError
    from simplequeue.storage.sqlite_backend import SQLiteBackend

    backend = SQLiteBackend(tmp_path / "noinit.db")  # schema never created
    with pytest.raises(StorageError):
        backend.enqueue("q", {"x": 1})


def test_dead_letters_declares_foreign_key_to_messages(tmp_path) -> None:
    import sqlite3

    from simplequeue.storage.sqlite_backend import SQLiteBackend

    db_path = tmp_path / "fk.db"
    SQLiteBackend(db_path).init_schema()
    conn = sqlite3.connect(db_path)
    try:
        foreign_keys = conn.execute("PRAGMA foreign_key_list(dead_letters)").fetchall()
    finally:
        conn.close()
    assert any(row[2] == "messages" for row in foreign_keys)  # row[2] = referenced table


def test_package_version_matches_pyproject() -> None:
    import pathlib
    import tomllib

    from simplequeue import __version__

    root = pathlib.Path(__file__).resolve().parents[2]
    pyproject = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    assert __version__ == pyproject["project"]["version"]


def test_log_event_emits_at_requested_level(caplog) -> None:
    import logging

    from simplequeue.observability.logging import log_event

    logger = logging.getLogger("simplequeue.leveltest")
    with caplog.at_level(logging.DEBUG, logger="simplequeue.leveltest"):
        log_event(logger, "heartbeat", level=logging.DEBUG)
        log_event(logger, "problem", level=logging.WARNING)
    seen = {record.levelno for record in caplog.records}
    assert logging.DEBUG in seen
    assert logging.WARNING in seen
