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


def test_cli_verify_healthy_database(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "verify.db")
    init = run_cli(root, "init-db", "--db", db)
    assert init.returncode == 0, init.stderr
    result = run_cli(root, "verify", "--db", db)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["healthy"] is True
    assert payload["integrity_check"] == "ok"
    assert payload["schema_consistent"] is True


def test_cli_verify_missing_database(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "no-such.db")
    result = run_cli(root, "verify", "--db", db)
    assert result.returncode == 1, result.stderr
    payload = json.loads(result.stdout)
    assert payload["healthy"] is False
