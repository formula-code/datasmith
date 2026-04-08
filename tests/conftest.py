"""Shared test fixtures."""

from __future__ import annotations

import os

import pytest


@pytest.fixture(autouse=True)
def _set_test_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure test env vars are set so Settings doesn't fail."""
    monkeypatch.setenv("SUPABASE_URL", os.environ.get("SUPABASE_URL", "http://127.0.0.1:54321"))
    monkeypatch.setenv("SUPABASE_KEY", os.environ.get("SUPABASE_KEY", "test-key"))
    monkeypatch.setenv("GH_TOKENS", os.environ.get("GH_TOKENS", "ghp_test1,ghp_test2,ghp_test3"))
