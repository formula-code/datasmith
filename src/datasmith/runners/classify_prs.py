"""Stage 3 — fetch the diff, apply the size gate, then classify.

The diff fetch used to live in stage 2, next to the rest of the symbolic
pre-screen.  Measured on 4 000 PRs merged since 2024, the pre-screen's three
components reject 40.4% on the title, 4.3% on file compliance, and 3.0% on
patch size — and only the last needs the diff.  Every one of those 121
patch-size rejections was "too large", against a ``MAX_PATCH_TOKENS`` of
16 000 that ``PerfClassifier.truncate_patch`` then truncates to anyway.  The
gate exists to protect the classifier, so it belongs with the classifier.

Moving it here also stops stage 2 spending a REST call on PRs that never get
classified, and lets the rate-limit waits overlap with LLM latency.

Budget, and why the concurrency here is deliberately small: the token pool
holds one token, REST core allows 5 000 requests an hour, and July 2026 needs
roughly 5 616 diff fetches.  A monthly run therefore crosses at least one
rate-limit reset by design — waiting is a correctness requirement, not an
optimisation.  The failure mode to avoid is a *silent* wait, so every stall is
logged while it is happening rather than after it ends.
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

# ---------------------------------------------------------------------------
# Tunable knobs.  These pace GitHub, not the machine: stage 4 is the stage that
# scales with cores.  Keeping the two budgets on separate dials means raising
# one cannot trip the other's rate limits (spec section 6.4).
# ---------------------------------------------------------------------------

# Concurrent diff fetches.  Low on purpose — one token, 5 000 REST requests an
# hour, and GitHub's *secondary* limit punishes bursts even while the primary
# budget still has room.
DATASMITH_CLASSIFY_DIFF_CONCURRENCY: int = int(os.environ.get("DATASMITH_CLASSIFY_DIFF_CONCURRENCY", "4"))

# Minimum spacing between diff fetches.  0.75 s is about 4 800 requests an
# hour, just inside the 5 000 core budget, so a run paces itself rather than
# sprinting into a 403 and then sleeping out the remainder of the window.
# Set to 0 to disable pacing.
DATASMITH_CLASSIFY_DIFF_MIN_INTERVAL_S: float = float(os.environ.get("DATASMITH_CLASSIFY_DIFF_MIN_INTERVAL_S", "0.75"))

# How often to say out loud that a diff fetch is still waiting.  A rate-limit
# reset can be an hour away, and an hour of silence is indistinguishable from
# a hang.
DATASMITH_CLASSIFY_DIFF_STALL_LOG_S: float = float(os.environ.get("DATASMITH_CLASSIFY_DIFF_STALL_LOG_S", "30"))

# ---------------------------------------------------------------------------
# A third budget, and the one dial that does *not* pace GitHub.  The two
# ``classify`` calls are blocking DSPy requests, so they run on threads; the
# thing they queue against is the LLM backend, not the REST budget and not the
# disk.  It gets its own knob for the same reason stages 2 and 4 have theirs:
# raising the dial that governs one backend must not silently raise the load
# on another.
#
# The pool is explicit because ``run_in_executor(None, ...)`` is the
# interpreter default sized ``min(32, cpu_count + 4)`` — a number that means
# something different on every host, and that every other library in the
# process shares (spec section 6.4).
# ---------------------------------------------------------------------------
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
        """Run the stage against a pool this runner owns and shuts down.

        Built here rather than in ``__init__`` so a runner that is constructed
        and never run leaves no threads behind, and so the pool dies with the
        stage even when the stage raises.
        """
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
                # GitHub answered definitively that it will not serve this
                # diff (404/406/410/451).  That is a decided outcome, not a
                # failure: recording it terminally stops a resumed run paying
                # the same REST call again forever.  A request that *failed*
                # never reaches here — it raises out of the client and
                # BaseRunner writes a runner_failures row instead.
                #
                # is_performance_commit_symbolic is deliberately left alone.
                # The size gate was never evaluated, so writing False would
                # claim a verdict the screen never reached.
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

        if not check_patch_size(patch):
            # The last component of the symbolic screen, evaluated where the
            # diff is.  Stage 2 wrote is_performance_commit_symbolic from the
            # title filter and file compliance alone; this completes it.
            #
            # The patch is not persisted.  Every measured rejection was "too
            # large", and storing large patches table-wide is what took
            # PostgREST down with an out-of-memory abort.
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
            # Persist what the REST call bought *before* spending an LLM call
            # on it.  The resume predicate is ``is_performance_commit IS
            # NULL``, so a classifier or judge failure re-selects this row on
            # the next run; writing the patch afterwards would mean the row
            # comes back with an empty patch and buys the same diff again out
            # of a budget that is already short (roughly 5 616 fetches for
            # July against 5 000 REST requests an hour, on one token).
            #
            # This write is deliberately after the size gate, never before it.
            # Every measured size rejection was "too large", and storing large
            # patches table-wide is what took PostgREST down with an
            # out-of-memory abort.
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

        Both are released before the LLM call: the REST budget is the scarce
        resource, and holding a diff slot across a multi-second DSPy call would
        make GitHub's dial govern the classifier's throughput too.
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

        The module global is read here, not cached on the instance, so an
        operator's ``tokens.env`` value and a test's monkeypatch both take
        effect.
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
        """Await *coro*, reporting periodically for as long as it is blocked.

        With one token a monthly run has to wait out a rate-limit reset, and
        that wait happens inside the client — ``TokenPool.get_token`` blocks
        until the earliest reset when every token is exhausted.  Without this
        the stage prints nothing for up to an hour, which reads exactly like a
        hang.
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
            # A raising fetch must not leave the task pending: pytest reports
            # that as a warning, and in production it keeps a connection open.
            if not task.done():
                task.cancel()
