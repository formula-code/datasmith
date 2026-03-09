"""GitHub token pool with rotation and rate-limit awareness."""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass


@dataclass
class _RateLimit:
    remaining: int = 5000
    reset_at: float = 0.0


class TokenPool:
    """Thread-safe pool of GitHub tokens with automatic rotation.

    Tokens are read from the ``GH_TOKENS`` environment variable (comma-separated)
    or can be passed directly.
    """

    def __init__(self, tokens: list[str] | None = None) -> None:
        if tokens is None:
            raw = os.environ.get("GH_TOKENS", os.environ.get("GH_TOKEN", ""))
            tokens = [t.strip() for t in raw.split(",") if t.strip()]
        if not tokens:
            raise ValueError("No GitHub tokens provided (set GH_TOKENS env var)")
        self._tokens = tokens
        self._lock = threading.Lock()
        self._rate_limits: dict[str, _RateLimit] = {t: _RateLimit() for t in tokens}

    @property
    def size(self) -> int:
        return len(self._tokens)

    def get_token(self) -> str:
        """Return a token that is not currently rate-limited.

        Blocks if all tokens are exhausted until the earliest reset time.
        """
        while True:
            with self._lock:
                now = time.time()
                for token in self._tokens:
                    rl = self._rate_limits[token]
                    if rl.remaining > 0 or rl.reset_at <= now:
                        # Reset if window has passed
                        if rl.reset_at <= now:
                            rl.remaining = 5000
                        return token

                # All exhausted — find earliest reset
                earliest = min(rl.reset_at for rl in self._rate_limits.values())

            wait = max(0.1, earliest - time.time())
            time.sleep(min(wait, 5.0))  # cap sleep to re-check periodically

    def report_rate_limit(self, token: str, remaining: int = 0, reset_at: float = 0.0) -> None:
        """Update rate-limit state for a token (called on 429/403)."""
        with self._lock:
            if token in self._rate_limits:
                self._rate_limits[token].remaining = remaining
                self._rate_limits[token].reset_at = reset_at
