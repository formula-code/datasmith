from __future__ import annotations

import asyncio
import time
import traceback
import uuid
from abc import ABC, abstractmethod
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

    @abstractmethod
    async def _process_item(self, item: Any) -> None: ...

    async def run(self, items: list[Any]) -> None:
        """Run the runner on a list of items with bounded concurrency."""
        self._total = len(items)
        self._completed = 0
        self._failed = 0

        self._init_progress()

        sem = asyncio.Semaphore(self._n_concurrent)

        async def _wrapped(item: Any) -> None:
            async with sem:
                try:
                    await self._process_item(item)
                    self._completed += 1
                except Exception as exc:
                    self._failed += 1
                    self._log_failure(item, exc)
                    logger.exception("Failed processing item %s", self._item_id(item))
                finally:
                    self._maybe_update_progress()

        tasks = [asyncio.create_task(_wrapped(item)) for item in items]
        await asyncio.gather(*tasks)
        self._update_progress(force=True)

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
