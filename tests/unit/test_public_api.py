"""Public API tests."""

from __future__ import annotations

import importlib

import pytest

import simplequeue


def test_public_api_exports_are_importable() -> None:
    for name in simplequeue.__all__:
        obj = getattr(simplequeue, name)
        assert obj is not None, name


def test_version_matches_package() -> None:
    assert simplequeue.__version__ == "0.2.1"


@pytest.mark.parametrize("name", simplequeue.__all__)
def test_public_api_symbols_exist(name: str) -> None:
    importlib.import_module("simplequeue")
    assert hasattr(simplequeue, name)
