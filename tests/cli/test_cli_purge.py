from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


def run_cli(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "src")
    return subprocess.run(
        [sys.executable, "-m", "simplequeue.cli.main", *args],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def test_cli_purge_dry_run_returns_preview(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "purge-dry-cli.db")
    init = run_cli(root, "init-db", "--db", db)
    assert init.returncode == 0, init.stderr
    produced = run_cli(
        root,
        "produce",
        "--db",
        db,
        "--queue",
        "jobs",
        "--payload",
        '{"x":1}',
    )
    assert produced.returncode == 0, produced.stderr
    message_id = json.loads(produced.stdout)["message_ids"][0]
    consumed = run_cli(root, "consume", "--db", db, "--queue", "jobs", "--limit", "1")
    assert consumed.returncode == 0, consumed.stderr
    dry = run_cli(
        root,
        "purge",
        "--db",
        db,
        "--queue",
        "jobs",
        "--older-than-days",
        "0",
        "--dry-run",
    )
    assert dry.returncode == 0, dry.stderr
    payload = json.loads(dry.stdout)
    assert payload["dry_run"] is True
    assert payload["removed_total"] == 1
    inspect_before = run_cli(root, "inspect", "--db", db, "--queue", "jobs", "--message-id", str(message_id))
    assert inspect_before.returncode == 0, inspect_before.stderr
    assert json.loads(inspect_before.stdout)["message"]["id"] == message_id
    live = run_cli(
        root,
        "purge",
        "--db",
        db,
        "--queue",
        "jobs",
        "--older-than-days",
        "0",
    )
    assert live.returncode == 0, live.stderr
    inspect_after = run_cli(root, "inspect", "--db", db, "--queue", "jobs", "--message-id", str(message_id))
    assert inspect_after.returncode == 3
    assert json.loads(inspect_after.stdout)["found"] is False
