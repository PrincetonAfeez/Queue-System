from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from simplequeue.cli.commands import verify
from simplequeue.config import QueueConfig
from simplequeue.storage.migrations import SCHEMA_VERSION
from simplequeue.storage.sqlite_backend import SQLiteBackend


def test_verify_healthy_after_init(tmp_path: Path) -> None:
    db = tmp_path / "healthy.db"
    backend = SQLiteBackend(db)
    backend.init_schema()
    backend.enqueue("jobs", {"x": 1})
    result = backend.verify_database()
    assert result.healthy
    assert result.integrity_check == "ok"
    assert result.foreign_key_check_ok
    assert result.schema_consistent
    assert result.schema_version == SCHEMA_VERSION
    assert result.row_counts["messages"] == 1


def test_verify_missing_database_file(tmp_path: Path) -> None:
    result = SQLiteBackend(tmp_path / "missing.db").verify_database()
    assert not result.healthy
    assert "does not exist" in result.errors[0]


def test_verify_uninitialized_empty_file(tmp_path: Path) -> None:
    db = tmp_path / "empty.db"
    db.write_bytes(b"")
    result = SQLiteBackend(db).verify_database()
    assert not result.healthy
    assert any("missing table" in error for error in result.errors)


def test_verify_schema_newer_than_library(tmp_path: Path) -> None:
    db = tmp_path / "newer.db"
    backend = SQLiteBackend(db)
    backend.init_schema()
    with backend._connect() as conn:  # noqa: SLF001
        conn.execute(
            "UPDATE schema_meta SET value = ? WHERE key = 'version'",
            (str(SCHEMA_VERSION + 1),),
        )
        conn.commit()
    result = backend.verify_database()
    assert not result.healthy
    assert not result.schema_consistent
    assert any("newer than library" in error for error in result.errors)


def test_verify_schema_older_than_library(tmp_path: Path) -> None:
    db = tmp_path / "older.db"
    backend = SQLiteBackend(db)
    backend.init_schema()
    with backend._connect() as conn:  # noqa: SLF001
        conn.execute(
            "UPDATE schema_meta SET value = ? WHERE key = 'version'",
            (str(max(0, SCHEMA_VERSION - 1)),),
        )
        conn.commit()
    result = backend.verify_database()
    if SCHEMA_VERSION > 0:
        assert not result.healthy
        assert any("older than library" in error for error in result.errors)


def test_verify_cli_success(tmp_path: Path, capsys) -> None:
    db = str(tmp_path / "cli-ok.db")
    SQLiteBackend(tmp_path / "cli-ok.db").init_schema()
    code = verify.run(argparse.Namespace(db=db, config=None), QueueConfig(database_path=db))
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["healthy"] is True


def test_verify_cli_failure(tmp_path: Path, capsys) -> None:
    db = str(tmp_path / "cli-bad.db")
    code = verify.run(argparse.Namespace(db=db, config=None), QueueConfig(database_path=db))
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["healthy"] is False
