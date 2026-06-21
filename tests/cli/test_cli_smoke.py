"""CLI smoke tests."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
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


def test_cli_init_produce_consume_stats_and_peek(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "cli.db")

    init = run_cli(root, "init-db", "--db", db)
    assert init.returncode == 0, init.stderr

    produced = run_cli(
        root,
        "produce",
        "--db",
        db,
        "--queue",
        "emails",
        "--count",
        "2",
        "--payload-template",
        '{"job":"{n}"}',
    )
    assert produced.returncode == 0, produced.stderr
    assert json.loads(produced.stdout)["count"] == 2

    peek = run_cli(root, "peek", "--db", db, "--queue", "emails")
    assert peek.returncode == 0, peek.stderr
    assert len(json.loads(peek.stdout)["messages"]) == 2

    consumed = run_cli(root, "consume", "--db", db, "--queue", "emails", "--limit", "2")
    assert consumed.returncode == 0, consumed.stderr

    stats = run_cli(root, "stats", "--db", db, "--queue", "emails", "--no-cache")
    assert stats.returncode == 0, stats.stderr
    snapshot = json.loads(stats.stdout)
    assert snapshot["acked"] == 2
    assert snapshot["current_depth"] == 0
    assert "recent_worker_ids" in snapshot


def test_cli_version_flag(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    result = run_cli(root, "--version")
    assert result.returncode == 0, result.stderr
    assert "simplequeue" in result.stdout
    assert "0.2.1" in result.stdout


def test_cli_demo_receipt_handles(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "demo.db")
    demo = run_cli(root, "demo", "--db", db, "receipt-handle-stale-ack")
    assert demo.returncode == 0, demo.stderr
    payload = json.loads(demo.stdout)
    assert payload["stale_ack_success"] is False
    assert payload["fresh_ack_success"] is True


def test_cli_list_queues(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "queues.db")
    run_cli(root, "init-db", "--db", db)
    run_cli(root, "produce", "--db", db, "--queue", "alpha", "--payload", '{"a":1}')
    run_cli(root, "produce", "--db", db, "--queue", "beta", "--payload", '{"b":1}')
    result = run_cli(root, "list-queues", "--db", db)
    assert result.returncode == 0, result.stderr
    names = json.loads(result.stdout)["queues"]
    assert "alpha" in names
    assert "beta" in names


def test_cli_sweep(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "sweep.db")
    run_cli(root, "init-db", "--db", db)
    result = run_cli(root, "sweep", "--db", db)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert "expired" in payload
    assert "dead_lettered" in payload


def test_cli_purge_smoke(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "purge-smoke.db")
    run_cli(root, "init-db", "--db", db)
    run_cli(root, "produce", "--db", db, "--queue", "jobs", "--payload", '{"x":1}')
    run_cli(
        root,
        "consume",
        "--db",
        db,
        "--queue",
        "jobs",
        "--limit",
        "1",
    )
    result = run_cli(root, "purge", "--db", db, "--queue", "jobs", "--older-than-days", "0")
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["removed_total"] == 1


def test_cli_dlq_all_queues_smoke(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "dlq-all-smoke.db")
    run_cli(root, "init-db", "--db", db)
    run_cli(
        root,
        "produce",
        "--db",
        db,
        "--queue",
        "jobs",
        "--payload",
        '{"x":1}',
        "--max-attempts",
        "1",
    )
    run_cli(root, "consume", "--db", db, "--queue", "jobs", "--limit", "1", "--fail-every", "1")
    result = run_cli(root, "dlq", "--db", db, "--all-queues")
    assert result.returncode == 0, result.stderr
    assert len(json.loads(result.stdout)["dead_letters"]) == 1


def test_cli_purge_conflicting_older_than_flags_returns_exit_2(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "purge-conflict.db")
    run_cli(root, "init-db", "--db", db)
    result = run_cli(
        root,
        "purge",
        "--db",
        db,
        "--queue",
        "jobs",
        "--older-than-days",
        "0",
        "--older-than",
        "2026-01-01T00:00:00+00:00",
    )
    assert result.returncode == 2
    assert "only one" in result.stderr


def test_cli_inspect_not_found_and_wrong_queue(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "inspect.db")
    run_cli(root, "init-db", "--db", db)
    run_cli(root, "produce", "--db", db, "--queue", "alpha", "--payload", '{"x":1}')
    missing = run_cli(root, "inspect", "--db", db, "--queue", "alpha", "--message-id", "999")
    assert missing.returncode == 3

    produced = run_cli(root, "produce", "--db", db, "--queue", "beta", "--payload", '{"y":1}')
    message_id = json.loads(produced.stdout)["message_ids"][0]
    wrong_queue = run_cli(
        root, "inspect", "--db", db, "--queue", "alpha", "--message-id", str(message_id)
    )
    assert wrong_queue.returncode == 3


def test_cli_dlq_and_requeue(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "dlq.db")
    run_cli(root, "init-db", "--db", db)
    run_cli(
        root,
        "produce",
        "--db",
        db,
        "--queue",
        "jobs",
        "--payload",
        '{"x":1}',
        "--max-attempts",
        "1",
    )
    run_cli(
        root,
        "consume",
        "--db",
        db,
        "--queue",
        "jobs",
        "--limit",
        "1",
        "--fail-every",
        "1",
    )
    dlq = run_cli(root, "dlq", "--db", db, "--queue", "jobs")
    assert dlq.returncode == 0, dlq.stderr
    dead = json.loads(dlq.stdout)["dead_letters"]
    assert len(dead) == 1
    message_id = dead[0]["original_message_id"]
    requeue = run_cli(
        root, "dlq-requeue", "--db", db, "--queue", "jobs", "--message-id", str(message_id)
    )
    assert requeue.returncode == 0, requeue.stderr


def test_cli_dlq_requeue_invalid_returns_exit_4(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "dlq-invalid.db")
    run_cli(root, "init-db", "--db", db)
    result = run_cli(root, "dlq-requeue", "--db", db, "--queue", "jobs", "--message-id", "1")
    assert result.returncode == 4
    assert "simplequeue:" in result.stderr


def test_cli_config_file_loading(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "cfg.db")
    config = tmp_path / "queue.toml"
    config.write_text(
        'queue_name = "from-config"\nvisibility_timeout = 12\npoll_interval = 0.1\n',
        encoding="utf-8",
    )
    run_cli(root, "init-db", "--config", str(config), "--db", db)
    produced = run_cli(root, "produce", "--config", str(config), "--db", db, "--count", "1")
    assert produced.returncode == 0, produced.stderr
    assert json.loads(produced.stdout)["queue"] == "from-config"


def test_cli_produce_rejects_dual_payload_flags(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "produce.db")
    run_cli(root, "init-db", "--db", db)
    result = run_cli(
        root,
        "produce",
        "--db",
        db,
        "--payload",
        '{"a":1}',
        "--payload-template",
        '{"n":"{n}"}',
    )
    assert result.returncode == 2


def test_cli_invalid_count_returns_exit_2(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "count.db")
    run_cli(root, "init-db", "--db", db)
    result = run_cli(root, "produce", "--db", db, "--count", "0")
    assert result.returncode == 2


def test_cli_storage_error_returns_exit_1(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    bad_path = tmp_path / "not-a-db-file"
    bad_path.mkdir()
    result = run_cli(root, "stats", "--db", str(bad_path), "--no-cache")
    assert result.returncode == 1
    assert "storage operation" in result.stderr.lower()


def test_cli_demo_all_uses_shared_db(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "all-demo.db")
    result = run_cli(root, "demo", "--db", db, "all")
    assert result.returncode == 0, result.stderr
    assert Path(db).exists()
    payload = json.loads(result.stdout)
    assert "unsafe-double-claim" not in payload
    assert "basic" in payload


def test_main_returns_2_without_subcommand() -> None:
    from simplequeue.cli.main import main

    assert main([]) == 2


def test_main_keyboard_interrupt_returns_130(monkeypatch) -> None:
    from simplequeue.cli import main as cli_main

    def interrupt(*args: object, **kwargs: object) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr(cli_main, "_merged_config", interrupt)
    assert cli_main.main(["init-db", "--db", "queue.db"]) == 130


def test_cli_missing_config_returns_exit_2(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    missing = tmp_path / "missing.toml"
    result = run_cli(root, "init-db", "--config", str(missing))
    assert result.returncode == 2


def test_cli_invalid_config_extension_returns_exit_2(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    bad = tmp_path / "config.yaml"
    bad.write_text("queue_name: x\n", encoding="utf-8")
    result = run_cli(root, "init-db", "--config", str(bad))
    assert result.returncode == 2


def test_cli_inspect_success(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "inspect-ok.db")
    run_cli(root, "init-db", "--db", db)
    produced = run_cli(root, "produce", "--db", db, "--queue", "jobs", "--payload", '{"x":1}')
    message_id = json.loads(produced.stdout)["message_ids"][0]
    result = run_cli(root, "inspect", "--db", db, "--queue", "jobs", "--message-id", str(message_id))
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["message"]["payload"] == {"x": 1}


def test_cli_produce_idempotent_dedupes(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "idemp.db")
    run_cli(root, "init-db", "--db", db)
    args = ("produce", "--db", db, "--queue", "jobs", "--idempotent", "--payload", '{"k":1}')
    first = run_cli(root, *args)
    second = run_cli(root, *args)
    assert first.returncode == 0 and second.returncode == 0
    assert json.loads(second.stdout)["count"] == 1


def test_cli_stats_cache_path(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "cache-stats.db")
    run_cli(root, "init-db", "--db", db)
    run_cli(root, "produce", "--db", db, "--queue", "jobs", "--payload", '{"x":1}')
    first = run_cli(root, "stats", "--db", db, "--queue", "jobs")
    second = run_cli(root, "stats", "--db", db, "--queue", "jobs")
    assert first.returncode == 0 and second.returncode == 0
    assert json.loads(first.stdout)["enqueued"] == json.loads(second.stdout)["enqueued"] == 1


def test_cli_dlq_requeue_wrong_queue_returns_exit_4(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "dlq-wrong.db")
    run_cli(root, "init-db", "--db", db)
    run_cli(
        root,
        "produce",
        "--db",
        db,
        "--queue",
        "alpha",
        "--payload",
        '{"x":1}',
        "--max-attempts",
        "1",
    )
    run_cli(root, "consume", "--db", db, "--queue", "alpha", "--limit", "1", "--fail-every", "1")
    dlq = run_cli(root, "dlq", "--db", db, "--queue", "alpha")
    message_id = json.loads(dlq.stdout)["dead_letters"][0]["original_message_id"]
    result = run_cli(
        root, "dlq-requeue", "--db", db, "--queue", "beta", "--message-id", str(message_id)
    )
    assert result.returncode == 4


def test_cli_peek_limit_zero_returns_exit_2(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "peek.db")
    run_cli(root, "init-db", "--db", db)
    result = run_cli(root, "peek", "--db", db, "--queue", "jobs", "--limit", "0")
    assert result.returncode == 2


def test_cli_consume_workers_zero_returns_exit_2(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "workers.db")
    run_cli(root, "init-db", "--db", db)
    result = run_cli(root, "consume", "--db", db, "--queue", "jobs", "--workers", "0", "--limit", "1")
    assert result.returncode == 2


def test_cli_consume_limit_zero_returns_exit_2(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "limit.db")
    run_cli(root, "init-db", "--db", db)
    result = run_cli(root, "consume", "--db", db, "--queue", "jobs", "--limit", "0")
    assert result.returncode == 2


def test_cli_consume_fail_every_warning_at_most_once(tmp_path: Path, capsys) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "amo.db")
    run_cli(root, "init-db", "--db", db)
    run_cli(root, "produce", "--db", db, "--queue", "jobs", "--payload", '{"x":1}')
    result = run_cli(
        root,
        "consume",
        "--db",
        db,
        "--queue",
        "jobs",
        "--mode",
        "at-most-once",
        "--limit",
        "1",
        "--fail-every",
        "1",
    )
    assert result.returncode == 0
    assert "fail-every has no effect" in result.stderr


def test_cli_consume_with_sweeper(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "sweeper-consume.db")
    run_cli(root, "init-db", "--db", db)
    run_cli(root, "produce", "--db", db, "--queue", "jobs", "--payload", '{"x":1}')
    result = run_cli(
        root,
        "consume",
        "--db",
        db,
        "--queue",
        "jobs",
        "--limit",
        "1",
        "--sweeper",
        "--sweeper-interval",
        "0.1",
    )
    assert result.returncode == 0, result.stderr


def _console_script_path() -> Path | None:
    import site

    name = "simplequeue.exe" if os.name == "nt" else "simplequeue"
    version_dir = f"Python{sys.version_info.major}{sys.version_info.minor}"
    candidates = [
        Path(sys.executable).parent / ("Scripts" if os.name == "nt" else "bin") / name,
        Path(site.getuserbase()) / version_dir / ("Scripts" if os.name == "nt" else "bin") / name,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def test_console_script_entrypoint(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    install = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-e", str(root), "-q"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert install.returncode == 0, install.stderr
    executable = _console_script_path()
    assert executable is not None, "simplequeue console script not found after editable install"
    result = subprocess.run(
        [str(executable), "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "simplequeue" in result.stdout


def test_installed_entrypoint_smoke(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "src")
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from simplequeue.cli.main import main; raise SystemExit(main(['--version']))",
        ],
        cwd=root,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0
    assert "simplequeue" in result.stdout


def test_cli_consume_multi_worker_respects_limit(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "limit-workers.db")
    run_cli(root, "init-db", "--db", db)
    run_cli(
        root,
        "produce",
        "--db",
        db,
        "--queue",
        "jobs",
        "--count",
        "10",
        "--payload-template",
        '{"n":"{n}"}',
    )
    result = run_cli(
        root,
        "consume",
        "--db",
        db,
        "--queue",
        "jobs",
        "--workers",
        "4",
        "--limit",
        "3",
    )
    assert result.returncode == 0, result.stderr
    stats = run_cli(root, "stats", "--db", db, "--queue", "jobs", "--no-cache")
    assert stats.returncode == 0, stats.stderr
    assert json.loads(stats.stdout)["acked"] == 3
    assert json.loads(stats.stdout)["current_depth"] == 7


def test_cli_consume_negative_fail_every_returns_exit_2(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "fail.db")
    run_cli(root, "init-db", "--db", db)
    result = run_cli(root, "consume", "--db", db, "--queue", "jobs", "--fail-every", "-1")
    assert result.returncode == 2


def test_cli_consume_negative_duration_returns_exit_2(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "duration.db")
    run_cli(root, "init-db", "--db", db)
    result = run_cli(root, "consume", "--db", db, "--queue", "jobs", "--duration", "-1")
    assert result.returncode == 2


def test_cli_dlq_requeue_idempotency_conflict_returns_exit_4(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "idemp-cli.db")
    run_cli(root, "init-db", "--db", db)
    run_cli(
        root,
        "produce",
        "--db",
        db,
        "--queue",
        "jobs",
        "--payload",
        '{"x":1}',
        "--idempotency-key",
        "k",
        "--max-attempts",
        "1",
    )
    run_cli(
        root,
        "consume",
        "--db",
        db,
        "--queue",
        "jobs",
        "--limit",
        "1",
        "--fail-every",
        "1",
    )
    run_cli(
        root,
        "produce",
        "--db",
        db,
        "--queue",
        "jobs",
        "--payload",
        '{"y":2}',
        "--idempotency-key",
        "k",
    )
    dlq = run_cli(root, "dlq", "--db", db, "--queue", "jobs")
    message_id = json.loads(dlq.stdout)["dead_letters"][0]["original_message_id"]
    result = run_cli(
        root, "dlq-requeue", "--db", db, "--queue", "jobs", "--message-id", str(message_id)
    )
    assert result.returncode == 4


def test_cli_consume_shutdown_mode_nack_current(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "shutdown.db")
    run_cli(root, "init-db", "--db", db)
    run_cli(root, "produce", "--db", db, "--queue", "jobs", "--payload", '{"x":1}')
    # Single worker, long process-time so we can stop mid-flight; nack-current on stop
    # is covered at worker level — here verify CLI accepts the flag and completes.
    result = run_cli(
        root,
        "consume",
        "--db",
        db,
        "--queue",
        "jobs",
        "--shutdown-mode",
        "nack-current",
        "--limit",
        "1",
        "--workers",
        "1",
    )
    assert result.returncode == 0, result.stderr


def test_cli_produce_invalid_json_returns_exit_2(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "json.db")
    run_cli(root, "init-db", "--db", db)
    result = run_cli(root, "produce", "--db", db, "--queue", "jobs", "--payload", "{bad")
    assert result.returncode == 2


def test_cli_init_db_ignores_invalid_worker_count_in_config(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    config_path = tmp_path / "queue.toml"
    config_path.write_text("worker_count = 0\n", encoding="utf-8")
    db = str(tmp_path / "init.db")
    result = run_cli(root, "init-db", "--config", str(config_path), "--db", db)
    assert result.returncode == 0, result.stderr


def test_cli_consume_idle_timeout_exits(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "idle.db")
    run_cli(root, "init-db", "--db", db)
    run_cli(root, "produce", "--db", db, "--queue", "jobs", "--payload", '{"x":1}')
    result = run_cli(
        root,
        "consume",
        "--db",
        db,
        "--queue",
        "jobs",
        "--limit",
        "10",
        "--idle-timeout",
        "0.2",
        "--poll-interval",
        "0.05",
    )
    assert result.returncode == 0, result.stderr
    stats = run_cli(root, "stats", "--db", db, "--queue", "jobs", "--no-cache")
    assert json.loads(stats.stdout)["acked"] == 1


def test_cli_consume_duration_exits(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "duration-pos.db")
    run_cli(root, "init-db", "--db", db)
    run_cli(
        root,
        "produce",
        "--db",
        db,
        "--queue",
        "jobs",
        "--count",
        "100",
        "--payload-template",
        '{"n":"{n}"}',
    )
    start = time.monotonic()
    result = run_cli(
        root,
        "consume",
        "--db",
        db,
        "--queue",
        "jobs",
        "--duration",
        "0.3",
        "--poll-interval",
        "0.05",
    )
    elapsed = time.monotonic() - start
    assert result.returncode == 0, result.stderr
    assert elapsed < 2.0


def test_cli_consume_no_sweeper_warning(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "warn.db")
    run_cli(root, "init-db", "--db", db)
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "src")
    proc = subprocess.Popen(
        [sys.executable, "-m", "simplequeue.cli.main", "consume", "--db", db, "--queue", "jobs"],
        cwd=root,
        env=env,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        _, stderr = proc.communicate(timeout=0.5)
    except subprocess.TimeoutExpired:
        proc.kill()
        _, stderr = proc.communicate()
    assert "without --sweeper" in stderr


def test_cli_produce_idempotent_count_warning(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "idemp-warn.db")
    run_cli(root, "init-db", "--db", db)
    result = run_cli(
        root,
        "produce",
        "--db",
        db,
        "--queue",
        "jobs",
        "--idempotent",
        "--count",
        "3",
        "--payload",
        '{"x":1}',
    )
    assert result.returncode == 0, result.stderr
    assert "dedupes" in result.stderr
    assert json.loads(result.stdout)["count"] == 1


def test_cli_produce_idempotency_key_suffixes(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "suffix.db")
    run_cli(root, "init-db", "--db", db)
    result = run_cli(
        root,
        "produce",
        "--db",
        db,
        "--queue",
        "jobs",
        "--count",
        "3",
        "--idempotency-key",
        "my-key",
        "--payload-template",
        '{"n":"{n}"}',
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["count"] == 3


def test_cli_consume_poll_interval_zero_returns_exit_2(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    db = str(tmp_path / "poll.db")
    run_cli(root, "init-db", "--db", db)
    result = run_cli(root, "consume", "--db", db, "--queue", "jobs", "--poll-interval", "0")
    assert result.returncode == 2


def test_cli_demo_unsafe_double_claim(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    result = run_cli(root, "demo", "unsafe-double-claim")
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["double_claimed"] is True
