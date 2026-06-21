from __future__ import annotations

import signal
from datetime import UTC, datetime

import pytest

from simplequeue.cli.shutdown import install_graceful_shutdown_handlers
from simplequeue.storage.base import StorageBackend
from simplequeue.workers.worker import Worker


def test_sigterm_handler_raises_keyboard_interrupt() -> None:
    install_graceful_shutdown_handlers()
    handler = signal.getsignal(signal.SIGTERM)
    with pytest.raises(KeyboardInterrupt):
        handler(signal.SIGTERM, None)


def test_storage_backend_abstract_methods_raise_not_implemented() -> None:
    """Exercise NotImplementedError bodies via a complete stub subclass."""

    class StubBackend(StorageBackend):
        def init_schema(self) -> None:
            StorageBackend.init_schema(self)

        def enqueue(self, *args, **kwargs) -> int:
            return StorageBackend.enqueue(self, *args, **kwargs)  # type: ignore[call-arg]

        def claim_next(self, *args, **kwargs):
            return StorageBackend.claim_next(self, *args, **kwargs)  # type: ignore[call-arg]

        def ack(self, *args, **kwargs):
            return StorageBackend.ack(self, *args, **kwargs)  # type: ignore[call-arg]

        def nack(self, *args, **kwargs):
            return StorageBackend.nack(self, *args, **kwargs)  # type: ignore[call-arg]

        def release_expired_leases(self, *args, **kwargs):
            return StorageBackend.release_expired_leases(self, *args, **kwargs)  # type: ignore[call-arg]

        def move_exhausted_to_dlq(self, *args, **kwargs) -> int:
            return StorageBackend.move_exhausted_to_dlq(self, *args, **kwargs)  # type: ignore[call-arg]

        def requeue_dead_letter(self, *args, **kwargs) -> int:
            return StorageBackend.requeue_dead_letter(self, *args, **kwargs)  # type: ignore[call-arg]

        def peek(self, *args, **kwargs):
            return StorageBackend.peek(self, *args, **kwargs)  # type: ignore[call-arg]

        def inspect(self, *args, **kwargs):
            return StorageBackend.inspect(self, *args, **kwargs)  # type: ignore[call-arg]

        def stats(self, *args, **kwargs):
            return StorageBackend.stats(self, *args, **kwargs)  # type: ignore[call-arg]

        def list_queues(self):
            return StorageBackend.list_queues(self)

        def list_dead_letters(self, *args, **kwargs):
            return StorageBackend.list_dead_letters(self, *args, **kwargs)  # type: ignore[call-arg]

        def purge_terminal_messages(self, *args, **kwargs) -> int:
            return StorageBackend.purge_terminal_messages(self, *args, **kwargs)  # type: ignore[call-arg]

    backend = StubBackend()
    now = datetime(2026, 1, 1, tzinfo=UTC)
    with pytest.raises(NotImplementedError):
        backend.init_schema()
    with pytest.raises(NotImplementedError):
        backend.enqueue("q", {})
    with pytest.raises(NotImplementedError):
        backend.claim_next("q", None, None, "w", now)  # type: ignore[arg-type]
    with pytest.raises(NotImplementedError):
        backend.ack("rh", now)
    with pytest.raises(NotImplementedError):
        backend.nack("rh", now)
    with pytest.raises(NotImplementedError):
        backend.release_expired_leases(now)
    with pytest.raises(NotImplementedError):
        backend.move_exhausted_to_dlq(now)
    with pytest.raises(NotImplementedError):
        backend.requeue_dead_letter(1, "q", now)
    with pytest.raises(NotImplementedError):
        backend.peek("q")
    with pytest.raises(NotImplementedError):
        backend.inspect(1)
    with pytest.raises(NotImplementedError):
        backend.stats("q")
    with pytest.raises(NotImplementedError):
        backend.list_queues()
    with pytest.raises(NotImplementedError):
        backend.list_dead_letters()
    with pytest.raises(NotImplementedError):
        backend.purge_terminal_messages("q", now)


def test_worker_safe_nack_survives_storage_exception(queue_factory, monkeypatch, caplog) -> None:
    import logging

    from simplequeue.core.delivery import Delivery
    from simplequeue.core.modes import DeliveryMode

    queue = queue_factory("nack-exc")
    worker = Worker(queue, lambda _d: True, visibility_timeout=30)
    delivery = Delivery(
        message_id=1,
        receipt_handle="rh",
        queue_name="nack-exc",
        payload={},
        attempt=1,
        delivery_mode=DeliveryMode.AT_LEAST_ONCE,
        leased_at=datetime(2026, 1, 1, tzinfo=UTC),
        lease_expires_at=None,
    )

    def boom(_handle: str, *, reason: str | None = None):
        raise RuntimeError("storage down")

    monkeypatch.setattr(queue, "nack", boom)
    with caplog.at_level(logging.INFO, logger="simplequeue"):
        worker._safe_nack(delivery, "reason")
    assert any("nack failed" in record.message for record in caplog.records)
