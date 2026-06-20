""" Shutdown handlers for the CLI. """

from __future__ import annotations

import signal


def install_graceful_shutdown_handlers() -> None:
    """Map SIGTERM to ``KeyboardInterrupt`` so CLI commands can shut down cleanly."""

    def _raise_interrupt(_signum: int, _frame: object) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _raise_interrupt)
