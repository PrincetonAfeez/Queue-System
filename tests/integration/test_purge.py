from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime

from simplequeue.cli.commands import purge
from simplequeue.config import QueueConfig
from simplequeue.scheduling.clock import FakeClock


def test_purge_dry_run_counts_without_deleting(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("dry-run", clock=clock)
    message_id = queue.enqueue({"x": 1})
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    queue.ack(delivery.receipt_handle)
    preview = queue.purge_terminal(older_than=clock.now(), dry_run=True)
    assert preview == 1
    assert queue.inspect(message_id) is not None
    removed = queue.purge_terminal(older_than=clock.now(), dry_run=False)
    assert removed == 1
    assert queue.inspect(message_id) is None


def test_purge_dry_run_include_dead_lettered(queue_factory) -> None:
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("dry-run-dlq", clock=clock)
    queue.enqueue({"x": 1}, max_attempts=1)
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    queue.nack(delivery.receipt_handle, reason="fail")
    preview = queue.purge_terminal(
        older_than=clock.now(),
        include_dead_lettered=True,
        dry_run=True,
    )
    assert preview == 1
    assert queue.list_dead_letters()
    removed = queue.purge_terminal(
        older_than=clock.now(),
        include_dead_lettered=True,
        dry_run=False,
    )
    assert removed == 1
    assert queue.list_dead_letters() == []


def test_purge_cli_dry_run_leaves_rows(queue_factory, tmp_path, capsys) -> None:
    db = str(tmp_path / "purge-dry.db")
    config = QueueConfig(database_path=db)
    clock = FakeClock.starting_at(datetime(2026, 1, 1, tzinfo=UTC))
    queue = queue_factory("jobs", db_name="purge-dry.db", clock=clock)
    message_id = queue.enqueue({"x": 1})
    delivery = queue.dequeue(worker_id="w")
    assert delivery is not None
    queue.ack(delivery.receipt_handle)
    code = purge.run(
        argparse.Namespace(
            db=db,
            config=None,
            queue="jobs",
            all_queues=False,
            older_than_days=0,
            older_than=None,
            include_dead_lettered=False,
            dry_run=True,
        ),
        config,
    )
    assert code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["removed_total"] == 1
    assert queue.inspect(message_id) is not None
