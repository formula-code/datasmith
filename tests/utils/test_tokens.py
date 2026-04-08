"""Tests for datasmith.utils.tokens — TokenPool."""

from __future__ import annotations

import threading
import time

import pytest

from datasmith.utils.tokens import TokenPool

_TOK1 = "tok1"
_TOK2 = "tok2"


class TestTokenPool:
    def test_get_token_returns_available(self) -> None:
        pool = TokenPool(tokens=["tok1", "tok2", "tok3"])
        token = pool.get_token()
        assert token in ["tok1", "tok2", "tok3"]

    def test_skips_rate_limited_token(self) -> None:
        pool = TokenPool(tokens=["tok1", "tok2"])
        # Rate-limit tok1 far in the future
        pool.report_rate_limit("tok1", remaining=0, reset_at=time.time() + 3600)
        token = pool.get_token()
        assert token == _TOK2

    def test_all_tokens_exhausted_blocks(self) -> None:
        pool = TokenPool(tokens=["tok1"])
        # Rate-limit with very short reset
        pool.report_rate_limit("tok1", remaining=0, reset_at=time.time() + 0.2)

        start = time.time()
        token = pool.get_token()
        elapsed = time.time() - start

        assert token == _TOK1
        assert elapsed >= 0.1  # Should have waited

    def test_report_rate_limit_updates_state(self) -> None:
        pool = TokenPool(tokens=["tok1", "tok2"])
        reset_time = time.time() + 100
        pool.report_rate_limit("tok1", remaining=0, reset_at=reset_time)

        # tok1 should be skipped
        token = pool.get_token()
        assert token == _TOK2

    def test_thread_safety_10_threads(self) -> None:
        pool = TokenPool(tokens=["tok1", "tok2", "tok3"])
        results: list[str] = []
        errors: list[Exception] = []

        def worker() -> None:
            try:
                for _ in range(10):
                    t = pool.get_token()
                    results.append(t)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert not errors
        assert len(results) == 100
        assert all(t in ["tok1", "tok2", "tok3"] for t in results)

    def test_token_rotation_after_reset(self) -> None:
        pool = TokenPool(tokens=["tok1"])
        # Rate-limit with immediate reset
        pool.report_rate_limit("tok1", remaining=0, reset_at=time.time() - 1)
        token = pool.get_token()
        assert token == _TOK1

    def test_no_tokens_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("GH_TOKENS", raising=False)
        monkeypatch.delenv("GH_TOKEN", raising=False)
        with pytest.raises(ValueError, match="No GitHub tokens"):
            TokenPool(tokens=[])

    def test_size_property(self) -> None:
        pool = TokenPool(tokens=["a", "b"])
        assert pool.size == 2
