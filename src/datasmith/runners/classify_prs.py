"""Stage 3: fetch the diff, apply the size gate, then classify.

The patch size check is the only part of the symbolic screen that needs a diff,
so it runs here, next to the classifier it protects. Concurrency is small
because one token allows 5 000 REST requests an hour, so a monthly run waits
out at least one rate-limit reset; each wait is logged while it happens.
"""

from __future__ import annotations

import asyncio
import functools
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from datasmith.filters import check_patch_size
from datasmith.runners.base import BaseRunner
from datasmith.utils import get_client, get_logger

logger = get_logger("runners.classify_prs")

# These settings pace GitHub, not the machine, so they are separate from stage 4's.

# Concurrent diff fetches; low because GitHub's secondary limit punishes bursts.
DATASMITH_CLASSIFY_DIFF_CONCURRENCY: int = int(os.environ.get("DATASMITH_CLASSIFY_DIFF_CONCURRENCY", "4"))

# Minimum spacing between diff fetches; 0.75 s is about 4 800 requests an hour,
# just inside the 5 000 limit. Set to 0 to disable pacing.
DATASMITH_CLASSIFY_DIFF_MIN_INTERVAL_S: float = float(os.environ.get("DATASMITH_CLASSIFY_DIFF_MIN_INTERVAL_S", "0.75"))

# How often to log that a diff fetch is still waiting, since a rate-limit reset
# can be an hour away.
DATASMITH_CLASSIFY_DIFF_STALL_LOG_S: float = float(os.environ.get("DATASMITH_CLASSIFY_DIFF_STALL_LOG_S", "30"))

# Threads for the blocking DSPy classify calls; this paces the LLM backend.
# An explicit pool, because the default executor's size varies by host.
DATASMITH_CLASSIFY_LLM_WORKERS: int = int(os.environ.get("DATASMITH_CLASSIFY_LLM_WORKERS", "8"))


class ClassifyPRsRunner(BaseRunner):
    """Fetch each PR's diff, screen it on size, and classify what survives."""

    def __init__(
        self,
        classifier: Any,
        judge: Any,
        github_client: Any = None,
        n_concurrent: int = 5,
        max_workers: int | None = None,
    ) -> None:
        super().__init__(name="classify_prs", n_concurrent=n_concurrent)
        self._classifier = classifier
        self._judge = judge
        self._gh = github_client
        self._diff_sem = asyncio.Semaphore(max(1, DATASMITH_CLASSIFY_DIFF_CONCURRENCY))
        self._pace_lock = asyncio.Lock()
        self._next_fetch_at = 0.0
        self._max_workers = max(1, DATASMITH_CLASSIFY_LLM_WORKERS if max_workers is None else max_workers)
        self._executor: ThreadPoolExecutor | None = None

    async def run(self, items: list[Any]) -> None:
        """Run the stage on a thread pool that is created here and shut down at the end."""
        self._executor = ThreadPoolExecutor(
            max_workers=self._max_workers,
            thread_name_prefix="classify-prs",
        )
        logger.info(
            "Classifying with %d LLM worker thread(s), %d concurrent item(s), %d concurrent diff fetch(es)",
            self._max_workers,
            self._n_concurrent,
            max(1, DATASMITH_CLASSIFY_DIFF_CONCURRENCY),
        )
        try:
            await super().run(items)
        finally:
            self._executor.shutdown(wait=True)
            self._executor = None

    async def _process_item(self, item: Any) -> None:
        """Process a PR dict with owner, repo, issue_number, description, patch."""
        owner = item["owner"]
        repo = item["repo"]
        issue_number = item["issue_number"]
        description = item.get("description", "")
        file_change_summary = item.get("file_change_summary", "")

        patch = item.get("patch") or ""
        fetched = False
        if not patch:
            from datasmith.github.client import DiffStatus

            diff = await self._fetch_diff(owner, repo, issue_number)
            if diff.status is not DiffStatus.OK:
                # GitHub will never serve this diff (404/406/410/451): record it so a
                # resumed run does not refetch. The symbolic verdict stays unset.
                logger.warning(
                    "%s/%s#%d: GitHub will not serve this diff (%s, HTTP %d); "
                    "recording it as not a performance commit without an LLM call",
                    owner,
                    repo,
                    issue_number,
                    diff.status.value,
                    diff.status_code,
                )
                self._update_pr(owner, repo, issue_number, {"is_performance_commit": False})
                return
            patch = diff.text
            fetched = True

        # Off the event loop, because tokenizing a large diff is CPU bound.
        size_ok = await asyncio.get_running_loop().run_in_executor(self._executor, check_patch_size, patch)
        if not size_ok:
            # Completes the symbolic screen. The patch is not stored, because large
            # patches across the table can make PostgREST run out of memory.
            logger.info(
                "%s/%s#%d: patch fails the size gate; recorded without an LLM call",
                owner,
                repo,
                issue_number,
            )
            self._update_pr(
                owner,
                repo,
                issue_number,
                {"is_performance_commit": False, "is_performance_commit_symbolic": False},
            )
            return

        if fetched:
            # Store the patch before the LLM call, so a classifier failure does not
            # cost another fetch on resume. Only after the size gate, to keep large patches out.
            self._update_pr(owner, repo, issue_number, {"patch": patch})

        loop = asyncio.get_running_loop()

        is_perf, _reason = await loop.run_in_executor(
            self._executor, functools.partial(self._classifier.classify, description, patch, file_change_summary)
        )

        update: dict[str, Any] = {"is_performance_commit": is_perf}

        if is_perf:
            decision = await loop.run_in_executor(
                self._executor, functools.partial(self._judge.classify, description, patch)
            )
            update["classification"] = decision.category
            update["difficulty"] = decision.difficulty

        self._update_pr(owner, repo, issue_number, update)

        logger.info("Classified %s/%s#%d: perf=%s", owner, repo, issue_number, is_perf)

    def _update_pr(self, owner: str, repo: str, issue_number: int, update: dict[str, Any]) -> None:
        """Write one PR's classification outcome."""
        client = get_client()
        client.table("pull_requests").update(update).eq("owner", owner).eq("repo", repo).eq(
            "issue_number", issue_number
        ).execute()

    async def _fetch_diff(self, owner: str, repo: str, issue_number: int) -> Any:
        """Fetch one diff under the semaphore and the pacer.

        Both are released before the LLM call, so diff slots do not limit classifier throughput.
        """
        if self._gh is None:
            raise RuntimeError(
                f"{owner}/{repo}#{issue_number} has no stored patch and ClassifyPRsRunner was built "
                "without a github_client, so stage 3 cannot fetch the diff the size gate and the "
                "classifier both need"
            )
        async with self._diff_sem:
            await self._pace()
            return await self._await_loudly(
                self._gh.fetch_diff(owner, repo, issue_number),
                f"{owner}/{repo}#{issue_number}",
            )

    async def _pace(self) -> None:
        """Space fetch starts by at least the configured interval.

        Reads the module global on each call, so environment values and test patches apply.
        """
        interval = DATASMITH_CLASSIFY_DIFF_MIN_INTERVAL_S
        if interval <= 0:
            return
        loop = asyncio.get_running_loop()
        async with self._pace_lock:
            now = loop.time()
            wait = self._next_fetch_at - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._next_fetch_at = max(now, self._next_fetch_at) + interval

    async def _await_loudly(self, coro: Any, label: str) -> Any:
        """Await *coro*, logging periodically while it is blocked.

        ``TokenPool.get_token`` can block up to an hour for a rate-limit reset.
        """
        task = asyncio.ensure_future(coro)
        started = time.monotonic()
        try:
            while True:
                done, _ = await asyncio.wait({task}, timeout=DATASMITH_CLASSIFY_DIFF_STALL_LOG_S)
                if done:
                    return await task
                logger.warning(
                    "Still waiting %.0fs on the diff for %s — most likely a GitHub rate-limit reset "
                    "(the token pool holds %d token(s); a monthly run crosses a reset by design)",
                    time.monotonic() - started,
                    label,
                    getattr(getattr(self._gh, "_pool", None), "size", 0),
                )
        finally:
            # Cancel a pending task, so it does not keep a connection open.
            if not task.done():
                task.cancel()
