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


@pytest.fixture(autouse=True)
def _block_real_supabase(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the test suite incapable of reaching a real Supabase instance.

    `_set_test_env` above points SUPABASE_URL at the local instance, so any test
    that reaches `get_client()` without patching it writes to the real database.
    That is not hypothetical: after TRY_DEFAULT gained outcome logging, two
    pre-existing tests in tests/agents/test_synthesizer.py began inserting four
    `error_logs` rows per run, under owner/repo#42. They passed while doing it,
    because the logging swallows its own errors.

    Patching every call site is not a fix, because the next unpatched call site
    is one commit away. Blocking client construction makes the whole class
    impossible, and turns a silent write into a loud failure naming the fix.

    A test that genuinely needs a database should patch `get_client` in the
    module under test, which is what every correct test here already does.
    """
    from datasmith.utils import db

    # get_client caches a singleton, so a client built by an earlier test would
    # bypass the block entirely.
    monkeypatch.setattr(db, "_client", None, raising=False)

    def _refuse(*args: object, **kwargs: object) -> None:
        raise RuntimeError(
            "This test tried to open a real Supabase connection. Patch get_client "
            "in the module under test, for example "
            "patch('datasmith.agents.synthesizer.get_client')."
        )

    monkeypatch.setattr(db, "create_client", _refuse)
