""" Receipt handle generation. """

from __future__ import annotations

import uuid


def new_receipt_handle() -> str:
    return uuid.uuid4().hex
