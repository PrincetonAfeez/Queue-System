""" Schema migrations. """

from __future__ import annotations

import sqlite3
from pathlib import Path

from simplequeue.core.exceptions import StorageError

SCHEMA_VERSION = 1


def load_schema_sql() -> str:
    return (Path(__file__).with_name("schema.sql")).read_text(encoding="utf-8")


def apply_schema_version(conn: sqlite3.Connection) -> None:
    """Record or verify the schema version after ``schema.sql`` runs."""
    row = conn.execute(
        "SELECT value FROM schema_meta WHERE key = 'version'"
    ).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO schema_meta (key, value) VALUES ('version', ?)",
            (str(SCHEMA_VERSION),),
        )
        return
    stored = int(str(row["value"]))
    if stored > SCHEMA_VERSION:
        raise StorageError(
            f"database schema version {stored} is newer than library version "
            f"{SCHEMA_VERSION}; upgrade the simplequeue package"
        )
    if stored < SCHEMA_VERSION:
        _upgrade_schema(conn, stored)
        conn.execute(
            "UPDATE schema_meta SET value = ? WHERE key = 'version'",
            (str(SCHEMA_VERSION),),
        )


def _upgrade_schema(conn: sqlite3.Connection, from_version: int) -> None:
    """Apply incremental upgrades from ``from_version`` to ``SCHEMA_VERSION``.

    Add one ``if from_version < N`` block per release that changes ``schema.sql``.
    """
    if from_version < 1:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
