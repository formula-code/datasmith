"""Tests for datasmith.utils.core — Settings, logging, retry."""

from __future__ import annotations

import logging

import pytest

from datasmith.utils.core import Settings, get_logger, with_backoff


class TestSettings:
    def test_settings_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SUPABASE_URL", "http://localhost:54321")
        monkeypatch.setenv("SUPABASE_KEY", "my-key")
        monkeypatch.setenv("GH_TOKENS", "tok1,tok2")
        s = Settings()
        assert s.supabase_url == "http://localhost:54321"
        assert s.supabase_key == "my-key"
        assert s.gh_tokens == "tok1,tok2"

    def test_settings_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DSPY_MAX_TOKENS", raising=False)
        monkeypatch.delenv("DOCKERHUB_USERNAME", raising=False)
        s = Settings(_env_file=None)  # type: ignore[call-arg]
        assert s.dspy_max_tokens == 16000
        assert isinstance(s.dockerhub_username, str)


class TestGetLogger:
    def test_returns_named_logger(self) -> None:
        logger = get_logger("mymod")
        assert logger.name == "datasmith.mymod"
        assert isinstance(logger, logging.Logger)

    def test_returns_root_logger(self) -> None:
        logger = get_logger()
        assert logger.name == "datasmith"


class TestWithBackoff:
    def test_retries_then_succeeds(self) -> None:
        call_count = 0

        @with_backoff(max_retries=3, base_delay=0.01)
        def flaky() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("transient")
            return "ok"

        assert flaky() == "ok"
        assert call_count == 3

    def test_max_retries_exceeded(self) -> None:
        @with_backoff(max_retries=2, base_delay=0.01)
        def always_fail() -> None:
            raise ValueError("permanent")

        with pytest.raises(ValueError, match="permanent"):
            always_fail()

    def test_no_retry_on_success(self) -> None:
        call_count = 0

        @with_backoff(max_retries=3, base_delay=0.01)
        def ok() -> str:
            nonlocal call_count
            call_count += 1
            return "fine"

        assert ok() == "fine"
        assert call_count == 1
