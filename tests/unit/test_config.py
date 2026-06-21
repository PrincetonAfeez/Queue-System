"""Config tests."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from simplequeue.config import QueueConfig, load_config


def test_load_config_defaults() -> None:
    config = load_config(None)
    assert config.queue_name == "default"
    assert config.delivery_mode == "at-least-once"
    assert config.backend == "sqlite"


def test_load_config_json_file(tmp_path: Path) -> None:
    path = tmp_path / "queue.json"
    path.write_text(
        json.dumps({"queue_name": "from-json", "visibility_timeout": 15, "worker_count": 2}),
        encoding="utf-8",
    )
    config = load_config(path, command="consume")
    assert config.queue_name == "from-json"
    assert config.visibility_timeout == 15.0
    assert config.worker_count == 2


def test_load_config_json_with_bom(tmp_path: Path) -> None:
    path = tmp_path / "queue.json"
    path.write_bytes(b"\xef\xbb\xbf" + b'{"queue_name": "bom-queue"}')
    config = load_config(path)
    assert config.queue_name == "bom-queue"


def test_load_config_toml_file(tmp_path: Path) -> None:
    path = tmp_path / "queue.toml"
    path.write_text('queue_name = "from-toml"\ncache_ttl = 2.5\n', encoding="utf-8")
    config = load_config(path)
    assert config.queue_name == "from-toml"
    assert config.cache_ttl == 2.5


def test_load_config_tml_extension(tmp_path: Path) -> None:
    path = tmp_path / "queue.tml"
    path.write_text('queue_name = "tml"\n', encoding="utf-8")
    assert load_config(path).queue_name == "tml"


def test_load_config_nested_queue_key(tmp_path: Path) -> None:
    path = tmp_path / "queue.json"
    path.write_text(json.dumps({"queue": {"queue_name": "nested"}}), encoding="utf-8")
    assert load_config(path).queue_name == "nested"


def test_load_config_ignores_unknown_keys(tmp_path: Path) -> None:
    path = tmp_path / "queue.json"
    path.write_text(json.dumps({"queue_name": "ok", "unknown_field": 99}), encoding="utf-8")
    assert load_config(path).queue_name == "ok"


def test_load_config_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="does not exist"):
        load_config(tmp_path / "missing.json")


def test_load_config_invalid_extension_raises(tmp_path: Path) -> None:
    path = tmp_path / "queue.yaml"
    path.write_text("queue_name: x\n", encoding="utf-8")
    with pytest.raises(ValueError, match="JSON or TOML"):
        load_config(path)


def test_load_config_coerce_string_numbers(tmp_path: Path) -> None:
    path = tmp_path / "queue.json"
    path.write_text(
        json.dumps(
            {
                "visibility_timeout": "12.5",
                "max_attempts": "4",
                "worker_count": "2",
            }
        ),
        encoding="utf-8",
    )
    config = load_config(path, command="consume")
    assert config.visibility_timeout == 12.5
    assert config.max_attempts == 4
    assert config.worker_count == 2


def test_load_config_coerce_invalid_type_raises(tmp_path: Path) -> None:
    path = tmp_path / "queue.json"
    path.write_text(json.dumps({"visibility_timeout": "not-a-number"}), encoding="utf-8")
    with pytest.raises(ValueError, match="visibility_timeout"):
        load_config(path)


def test_load_config_coerce_non_integer_float_raises(tmp_path: Path) -> None:
    path = tmp_path / "queue.json"
    path.write_text(json.dumps({"max_attempts": 2.5}), encoding="utf-8")
    with pytest.raises(ValueError, match="max_attempts"):
        load_config(path)


def test_load_config_invalid_delivery_mode_raises(tmp_path: Path) -> None:
    path = tmp_path / "queue.json"
    path.write_text(json.dumps({"delivery_mode": "bogus"}), encoding="utf-8")
    with pytest.raises(ValueError, match="unknown delivery mode"):
        load_config(path)


def test_load_config_invalid_shutdown_mode_raises(tmp_path: Path) -> None:
    path = tmp_path / "queue.json"
    path.write_text(json.dumps({"shutdown_mode": "bogus"}), encoding="utf-8")
    with pytest.raises(ValueError, match="unknown shutdown mode"):
        load_config(path)


def test_load_config_invalid_backend_raises(tmp_path: Path) -> None:
    path = tmp_path / "queue.json"
    path.write_text(json.dumps({"backend": "postgres"}), encoding="utf-8")
    with pytest.raises(ValueError, match="backend"):
        load_config(path)


def test_load_config_invalid_logging_level_raises(tmp_path: Path) -> None:
    path = tmp_path / "queue.json"
    path.write_text(json.dumps({"logging_level": "NOT_A_LEVEL"}), encoding="utf-8")
    with pytest.raises(ValueError, match="logging_level"):
        load_config(path)


def test_validate_ranges_max_attempts() -> None:
    from simplequeue.config import _validate_ranges

    with pytest.raises(ValueError, match="max_attempts"):
        _validate_ranges(QueueConfig(max_attempts=0), command="consume")
    _validate_ranges(QueueConfig(max_attempts=0), command="init-db")


def test_validate_ranges_cache_ttl() -> None:
    from simplequeue.config import _validate_ranges

    _validate_ranges(QueueConfig(cache_ttl=0), command="init-db")
    with pytest.raises(ValueError, match="cache_ttl"):
        _validate_ranges(QueueConfig(cache_ttl=0), command="stats")


def test_validate_ranges_consume_command_only() -> None:
    from simplequeue.config import _validate_ranges

    bad = QueueConfig(worker_count=0, visibility_timeout=0, sweeper_interval=0, poll_interval=0, idle_timeout=0)
    _validate_ranges(bad, command="init-db")  # worker_count=0 allowed for init-db
    with pytest.raises(ValueError, match="worker_count"):
        _validate_ranges(bad, command="consume")
    with pytest.raises(ValueError, match="visibility_timeout"):
        _validate_ranges(replace(bad, worker_count=1), command="consume")
    with pytest.raises(ValueError, match="sweeper_interval"):
        _validate_ranges(
            replace(bad, worker_count=1, visibility_timeout=1, sweeper_interval=0),
            command="consume",
        )
    with pytest.raises(ValueError, match="poll_interval"):
        _validate_ranges(
            replace(bad, worker_count=1, visibility_timeout=1, sweeper_interval=1, poll_interval=0),
            command="consume",
        )
    with pytest.raises(ValueError, match="idle_timeout"):
        _validate_ranges(
            replace(
                bad,
                worker_count=1,
                visibility_timeout=1,
                sweeper_interval=1,
                poll_interval=1,
                idle_timeout=0,
            ),
            command="consume",
        )


def test_queue_config_is_frozen_dataclass() -> None:
    config = QueueConfig()
    with pytest.raises(AttributeError):
        config.queue_name = "other"  # type: ignore[misc]
