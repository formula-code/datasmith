"""Async runner base: bounded concurrency, progress rows, and a loud ending.

``BaseRunner`` catches every per-item exception and keeps going, which is the
behaviour the pipeline wants — one unreachable repository must not abort a
stage.  The cost is that a stage which achieved nothing looks exactly like a
stage that achieved everything.  A real run reported "154/154 repositories,
zero failures" while storing 35 PRs, and had no way to say that was wrong.

So every run ends with a summary line: total, succeeded, failed, and a count
per distinct exception type.  It is logged unconditionally, at ``info``,
including on the zero-failure runs — the motivating case *had* no failures, so
a summary that only speaks up when something raised would have missed it
entirely (spec section 5).
"""

from __future__ import annotations

import asyncio
import time
import traceback
import uuid
from abc import ABC, abstractmethod
from collections import Counter
from typing import Any, TypeVar

from datasmith.utils import get_client, get_logger

logger = get_logger("runners.base")

T = TypeVar("T")


class BaseRunner(ABC):
    """Abstract async runner with Supabase progress tracking."""

    def __init__(self, name: str, n_concurrent: int = 10) -> None:
        self.name = name
        self.runner_id = f"{name}-{uuid.uuid4().hex[:8]}"
        self._n_concurrent = n_concurrent
        self._completed = 0
        self._failed = 0
        self._total = 0
        self._last_progress_update = 0.0
        self._error_types: Counter[str] = Counter()

    @abstractmethod
    async def _process_item(self, item: Any) -> None: ...

    async def run(self, items: list[Any]) -> None:
        """Run the runner on a list of items with bounded concurrency."""
        self._total = len(items)
        self._completed = 0
        self._failed = 0
        self._error_types = Counter()

        self._init_progress()

        sem = asyncio.Semaphore(self._n_concurrent)

        async def _wrapped(item: Any) -> None:
            async with sem:
                try:
                    await self._process_item(item)
                    self._completed += 1
                except Exception as exc:
                    self._failed += 1
                    self._error_types[type(exc).__name__] += 1
                    self._log_failure(item, exc)
                    logger.exception("Failed processing item %s", self._item_id(item))
                finally:
                    self._maybe_update_progress()

        tasks = [asyncio.create_task(_wrapped(item)) for item in items]
        try:
            await asyncio.gather(*tasks)
        except (KeyboardInterrupt, asyncio.CancelledError):
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        finally:
            self._update_progress(force=True)
            self._log_summary()

    def _log_summary(self) -> None:
        """Say how the stage went, whether or not anything failed.

        Unconditional on purpose.  Silence is what made a stage that stored
        35 of 461 PRs indistinguishable from a healthy one, so the end of a
        run always states the three counts and the error types behind them.
        """
        if self._error_types:
            breakdown = ", ".join(f"{name}={count}" for name, count in sorted(self._error_types.most_common()))
        else:
            breakdown = "none"
        unaccounted = self._total - self._completed - self._failed
        logger.info(
            "%s finished: %d item(s), %d succeeded, %d failed; error types: %s%s",
            self.name,
            self._total,
            self._completed,
            self._failed,
            breakdown,
            f"; {unaccounted} unaccounted for (cancelled)" if unaccounted else "",
        )

    def _item_id(self, item: Any) -> str:
        if hasattr(item, "cache_key"):
            return str(item.cache_key)
        return str(item)

    def _init_progress(self) -> None:
        try:
            client = get_client()
            client.table("runner_progress").upsert({
                "runner_id": self.runner_id,
                "runner_name": self.name,
                "total": self._total,
                "completed": 0,
                "failed": 0,
            }).execute()
        except Exception:
            logger.warning("Failed to initialize progress tracking")

    def _maybe_update_progress(self) -> None:
        now = time.time()
        if (self._completed + self._failed) % 10 == 0 or now - self._last_progress_update > 30:
            self._update_progress()

    def _update_progress(self, force: bool = False) -> None:
        self._last_progress_update = time.time()
        try:
            client = get_client()
            client.table("runner_progress").upsert({
                "runner_id": self.runner_id,
                "runner_name": self.name,
                "total": self._total,
                "completed": self._completed,
                "failed": self._failed,
            }).execute()
        except Exception:
            logger.warning("Failed to update progress")

    def _log_failure(self, item: Any, exc: Exception) -> None:
        try:
            client = get_client()
            client.table("runner_failures").insert({
                "runner_id": self.runner_id,
                "item_id": self._item_id(item),
                "error_message": str(exc),
                "traceback": traceback.format_exc(),
            }).execute()
        except Exception:
            logger.warning("Failed to log failure for %s", self._item_id(item))
