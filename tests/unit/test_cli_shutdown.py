from __future__ import annotations

import signal

from simplequeue.cli.shutdown import install_graceful_shutdown_handlers


def test_install_graceful_shutdown_handlers_registers_sigterm() -> None:
    install_graceful_shutdown_handlers()
    handler = signal.getsignal(signal.SIGTERM)
    assert callable(handler)
