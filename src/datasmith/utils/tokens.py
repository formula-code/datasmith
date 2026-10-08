"""GitHub token pool with rotation and rate-limit awareness."""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass

# Ceiling on how far into the future a reported rate-limit reset may sit.
# GitHub's real windows are an hour; anything longer came from a malformed
# header, and trusting it verbatim wedges the pool until the process restarts.
DATASMITH_TOKEN_MAX_RESET_S: float = float(os.environ.get("DATASMITH_TOKEN_MAX_RESET_S", "3600"))


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

    def try_get_token(self) -> tuple[str | None, float]:
        """Return ``(token, 0.0)`` if one is usable now, else ``(None, seconds to wait)``.

        The non-blocking half of :meth:`get_token`, so an async caller can
        ``await`` the wait instead of sleeping the thread it is running on.
        See :meth:`get_token` for why that distinction is load-bearing.
        """
        with self._lock:
            now = time.time()
            for token in self._tokens:
                rl = self._rate_limits[token]
                if rl.remaining > 0 or rl.reset_at <= now:
                    if rl.reset_at <= now:
                        rl.remaining = 5000
                    return token, 0.0
            earliest = min(rl.reset_at for rl in self._rate_limits.values())
        return None, max(0.1, earliest - time.time())

    def get_token(self) -> str:
        """Return a token that is not currently rate-limited, blocking if none is.

        NOT safe to call from a coroutine.  This sleeps the calling thread, so
        on an event loop it stops everything: no logging, no other requests, no
        progress, and no way to tell the difference from a crash.  A stage 3
        run wedged exactly this way -- silent for twenty minutes with GitHub
        reporting a full 5 000/5 000 budget, because one bad rate-limit report
        had marked the only token exhausted.  Async callers must use
        :meth:`try_get_token` and await the wait themselves.
        """
        while True:
            token, wait = self.try_get_token()
            if token is not None:
                return token
            time.sleep(min(wait, 5.0))  # cap sleep to re-check periodically

    def report_rate_limit(self, token: str, remaining: int = 0, reset_at: float = 0.0) -> None:
        """Update rate-limit state for a token (called on 429/403).

        ``reset_at`` is clamped to at most :data:`DATASMITH_TOKEN_MAX_RESET_S`
        from now.  It comes from a response header, and a malformed or
        misparsed one used to be trusted verbatim: a single bogus far-future
        epoch marked the token dead for hours, with no way to recover short of
        restarting the process.  The real GitHub windows are an hour, so
        anything beyond that is a bug in the input, not a genuine wait.
        """
        with self._lock:
            if token in self._rate_limits:
                ceiling = time.time() + DATASMITH_TOKEN_MAX_RESET_S
                self._rate_limits[token].remaining = remaining
                self._rate_limits[token].reset_at = min(reset_at, ceiling)
