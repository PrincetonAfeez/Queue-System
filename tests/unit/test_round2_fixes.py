from __future__ import annotations

from contextlib import closing
from datetime import UTC, datetime

import pytest

from simplequeue.config import QueueConfig, _validate_ranges, load_config
from simplequeue.core.exceptions import StorageError
from simplequeue.storage.factory import create_backend
from simplequeue.storage.migrations import SCHEMA_VERSION
from simplequeue.storage.sqlite_backend import SQLiteBackend


def test_init_db_succeeds_with_max_attempts_zero_in_config(tmp_path) -> None:
    import argparse

    from simplequeue.cli.commands import init_db

    db = tmp_path / "init.db"
    config = QueueConfig(database_path=str(db), max_attempts=0)
    args = argparse.Namespace(config=None, db=str(db))
    assert init_db.run(args, config) == 0
    backend = create_backend(config)
    assert backend.default_max_attempts == 3


def test_load_config_init_db_allows_zero_cache_ttl(tmp_path) -> None:
    path = tmp_path / "init.json"
    path.write_text('{"cache_ttl": 0}', encoding="utf-8")
    config = load_config(str(path), command="init-db")
    assert config.cache_ttl == 0.0


def test_validate_ranges_cache_ttl_scoped_to_cache_commands() -> None:

    _validate_ranges(QueueConfig(cache_ttl=0), command="init-db")
    with pytest.raises(ValueError, match="cache_ttl"):
        _validate_ranges(QueueConfig(cache_ttl=0), command="stats")


def test_list_dead_letters_rejects_conflicting_scope(queue_factory) -> None:
    queue = queue_factory("conflict")
    with pytest.raises(ValueError, match="only one"):
        queue.list_dead_letters("other", all_queues=True)


def test_purge_terminal_default_retention_skips_recent_rows(queue_factory) -> None:
    from simplequeue.scheduling.clock import FakeClock

    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("retention", clock=clock)
    queue.enqueue({"x": 1})
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    queue.ack(delivery.receipt_handle)
    assert queue.purge_terminal() == 0
    assert queue.inspect(delivery.message_id) is not None


def test_purge_terminal_include_dead_lettered(queue_factory) -> None:
    from simplequeue.scheduling.clock import FakeClock

    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("purge-dlq", clock=clock)
    queue.enqueue({"x": 1}, max_attempts=1)
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    queue.nack(delivery.receipt_handle, reason="fail")
    clock.advance(8 * 86400)
    removed = queue.purge_terminal(include_dead_lettered=True)
    assert removed == 1
    assert queue.inspect(delivery.message_id) is None


def test_apply_schema_version_rejects_newer_database(tmp_path) -> None:
    backend = SQLiteBackend(tmp_path / "newer.db")
    backend.init_schema()
    with closing(backend._connect()) as conn:  # noqa: SLF001
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE schema_meta SET value = ? WHERE key = 'version'",
            (str(SCHEMA_VERSION + 1),),
        )
        conn.commit()
    with pytest.raises(StorageError, match="newer than library"):
        backend.init_schema()

def test_cli_purge_command(queue_factory, tmp_path, capsys) -> None:
    import argparse
    import json

    from simplequeue.cli.commands import purge

    db = str(tmp_path / "purge-cli.db")
    config = QueueConfig(database_path=db)
    queue = queue_factory("jobs", db_name="purge-cli.db")
    queue.enqueue({"x": 1})
    assert purge.run(
        argparse.Namespace(
            db=db,
            config=None,
            queue="jobs",
            all_queues=False,
            older_than_days=None,
            older_than=None,
            include_dead_lettered=False,
        ),
        config,
    ) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["removed_total"] == 0
    assert payload["queues"] == [{"queue": "jobs", "removed": 0}]
