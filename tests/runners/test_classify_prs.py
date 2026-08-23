"""Tests for datasmith.runners.classify_prs — ClassifyPRsRunner."""

from __future__ import annotations

import asyncio
import itertools
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datasmith.filters import MAX_PATCH_TOKENS, estimate_tokens
from datasmith.github.client import DiffResult, DiffStatus
from datasmith.runners.classify_prs import ClassifyPRsRunner

# Long enough to clear MIN_PATCH_TOKENS (5) under the len//4 estimate that
# applies when tiktoken is not installed.
_BIG_ENOUGH_PATCH = "diff --git a/m.py b/m.py\n--- a/m.py\n+++ b/m.py\n-slow()\n+fast()\n"

# Comfortably past MAX_PATCH_TOKENS under either estimator. A run of one
# repeated character is not enough: tiktoken compresses it to roughly an
# eighth of what the len//4 fallback predicts, so the gate would not fire.
_OVERSIZED_PATCH = "\n".join(f"+    result[{i}] = compute_value(row, column_{i})" for i in range(20_000))


def _mock_supabase() -> MagicMock:
    """Create a mock Supabase client with fluent API."""
    client = MagicMock()
    table = MagicMock()
    client.table.return_value = table
    table.upsert.return_value = table
    table.insert.return_value = table
    table.update.return_value = table
    table.eq.return_value = table
    table.execute.return_value = MagicMock()
    return client


def _make_item(owner: str = "numpy", repo: str = "numpy", issue: int = 42, **kwargs: Any) -> dict[str, Any]:
    item: dict[str, Any] = {
        "owner": owner,
        "repo": repo,
        "issue_number": issue,
        "description": "Optimize array slicing",
        "patch": "diff --git a/numpy/core.py ...",
    }
    item.update(kwargs)
    return item


class TestClassifyPerf:
    async def test_classify_perf_commit(self) -> None:
        """Classifier marks as perf, judge provides category/difficulty."""
        mock_client = _mock_supabase()

        classifier = MagicMock()
        classifier.classify.return_value = (True, "Uses vectorized operations")

        judge = MagicMock()
        decision = MagicMock()
        decision.category = "algorithmic"
        decision.difficulty = "medium"
        judge.classify.return_value = decision

        with (
            patch("datasmith.runners.classify_prs.get_client", return_value=mock_client),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = ClassifyPRsRunner(classifier=classifier, judge=judge, n_concurrent=1)
            await runner.run([_make_item()])

        # Verify classifier was called (3rd arg is file_change_summary, defaults to "")
        classifier.classify.assert_called_once_with("Optimize array slicing", "diff --git a/numpy/core.py ...", "")
        # Judge should also be called for perf commits
        judge.classify.assert_called_once()

        # Verify pull_requests table was updated
        pr_calls = [call for call in mock_client.table.call_args_list if call.args[0] == "pull_requests"]
        assert len(pr_calls) >= 1

    async def test_classify_non_perf_commit(self) -> None:
        """Classifier marks as non-perf, judge is not called."""
        mock_client = _mock_supabase()

        classifier = MagicMock()
        classifier.classify.return_value = (False, "Documentation change")

        judge = MagicMock()

        with (
            patch("datasmith.runners.classify_prs.get_client", return_value=mock_client),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = ClassifyPRsRunner(classifier=classifier, judge=judge, n_concurrent=1)
            await runner.run([_make_item()])

        # Judge should NOT be called for non-perf commits
        judge.classify.assert_not_called()

        # pull_requests table should still be updated with is_performance_commit=False
        pr_calls = [call for call in mock_client.table.call_args_list if call.args[0] == "pull_requests"]
        assert len(pr_calls) >= 1

    async def test_file_change_summary_forwarded(self) -> None:
        """file_change_summary from item dict is passed to classifier.classify()."""
        mock_client = _mock_supabase()

        classifier = MagicMock()
        classifier.classify.return_value = (False, "Not perf")
        judge = MagicMock()

        summary = (
            "| File | Lines Added | Lines Removed |\n|------|-------------|----------------|\n| core.py | 10 | 5 |"
        )
        item = _make_item(file_change_summary=summary)

        with (
            patch("datasmith.runners.classify_prs.get_client", return_value=mock_client),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = ClassifyPRsRunner(classifier=classifier, judge=judge, n_concurrent=1)
            await runner.run([item])

        classifier.classify.assert_called_once_with("Optimize array slicing", "diff --git a/numpy/core.py ...", summary)


class TestDiffFetchAndSizeGate:
    """Stage 3 owns the diff: fetch it, screen it on size, then call the LLM.

    The gate moved here from stage 2 because it exists to protect the
    classifier — measured on 4 000 PRs it rejects 3.0%, every one of them for
    being too large, against a limit ``PerfClassifier.truncate_patch``
    truncates to anyway.
    """

    @staticmethod
    def _gh(status: DiffStatus = DiffStatus.OK, text: str = _BIG_ENOUGH_PATCH, code: int = 200) -> MagicMock:
        gh = MagicMock()
        gh.fetch_diff = AsyncMock(return_value=DiffResult(status=status, text=text, status_code=code))
        return gh

    @staticmethod
    async def _run(item: dict[str, Any], gh: MagicMock, classifier: MagicMock, judge: MagicMock) -> MagicMock:
        client = _mock_supabase()
        with (
            patch("datasmith.runners.classify_prs.get_client", return_value=client),
            patch("datasmith.runners.base.get_client", return_value=client),
            patch("datasmith.runners.classify_prs.DATASMITH_CLASSIFY_DIFF_MIN_INTERVAL_S", 0.0),
        ):
            runner = ClassifyPRsRunner(classifier, judge, github_client=gh, n_concurrent=1)
            await runner.run([item])
        return client

    @staticmethod
    def _update(client: MagicMock) -> dict[str, Any]:
        """The payload handed to ``pull_requests.update``."""
        return dict(client.table.return_value.update.call_args.args[0])

    async def test_missing_patch_is_fetched_then_classified(self) -> None:
        gh = self._gh()
        classifier = MagicMock()
        classifier.classify.return_value = (True, "vectorised")
        judge = MagicMock()
        judge.classify.return_value = MagicMock(category="algorithmic", difficulty="medium")

        item = _make_item()
        item["patch"] = ""
        client = await self._run(item, gh, classifier, judge)

        gh.fetch_diff.assert_awaited_once_with("numpy", "numpy", 42)
        classifier.classify.assert_called_once_with("Optimize array slicing", _BIG_ENOUGH_PATCH, "")
        # The fetched patch is persisted, so a resumed run does not spend a
        # second REST call out of a budget that is already short.
        assert self._update(client)["patch"] == _BIG_ENOUGH_PATCH

    async def test_stored_patch_is_not_refetched(self) -> None:
        gh = self._gh()
        classifier = MagicMock()
        classifier.classify.return_value = (False, "docs")

        await self._run(_make_item(), gh, classifier, MagicMock())

        gh.fetch_diff.assert_not_awaited()

    async def test_oversized_patch_skips_the_llm(self) -> None:
        """The size gate rejects before any model is asked anything."""
        gh = self._gh(text=_OVERSIZED_PATCH)
        classifier = MagicMock()
        judge = MagicMock()

        item = _make_item()
        item["patch"] = ""
        client = await self._run(item, gh, classifier, judge)

        assert estimate_tokens(_OVERSIZED_PATCH) > MAX_PATCH_TOKENS
        classifier.classify.assert_not_called()
        judge.classify.assert_not_called()
        update = self._update(client)
        assert update["is_performance_commit"] is False
        # The third component of the symbolic screen, finally evaluated.
        assert update["is_performance_commit_symbolic"] is False
        # An oversized patch is exactly what must not be written table-wide.
        assert "patch" not in update

    @pytest.mark.parametrize(
        ("status", "code"),
        [(DiffStatus.NOT_FOUND, 404), (DiffStatus.UNAVAILABLE, 410)],
    )
    async def test_unavailable_diff_is_recorded_not_failed(self, status: DiffStatus, code: int) -> None:
        """GitHub refusing a diff is a decided outcome, distinct from a failure."""
        gh = self._gh(status=status, text="", code=code)
        classifier = MagicMock()

        item = _make_item()
        item["patch"] = ""
        client = await self._run(item, gh, classifier, MagicMock())

        classifier.classify.assert_not_called()
        update = self._update(client)
        assert update == {"is_performance_commit": False}
        # The size gate never ran, so the symbolic verdict is left alone rather
        # than being asserted on evidence nobody gathered.
        assert "is_performance_commit_symbolic" not in update
        # Nothing was recorded as a runner failure: that is reserved for a
        # request that failed, which raises out of the client instead.
        assert not any(c.args[0] == "runner_failures" for c in client.table.call_args_list)

    async def test_failed_request_becomes_a_runner_failure(self) -> None:
        """A raising fetch is reported, never quietly recorded as a verdict."""
        gh = MagicMock()
        gh.fetch_diff = AsyncMock(side_effect=RuntimeError("connection reset"))
        classifier = MagicMock()

        item = _make_item()
        item["patch"] = ""
        client = await self._run(item, gh, classifier, MagicMock())

        classifier.classify.assert_not_called()
        client.table.return_value.update.assert_not_called()
        assert any(c.args[0] == "runner_failures" for c in client.table.call_args_list)

    async def test_no_github_client_is_a_loud_error(self) -> None:
        """A runner built without a client says so instead of classifying ''."""
        classifier = MagicMock()
        item = _make_item()
        item["patch"] = ""
        client = _mock_supabase()

        with (
            patch("datasmith.runners.classify_prs.get_client", return_value=client),
            patch("datasmith.runners.base.get_client", return_value=client),
        ):
            runner = ClassifyPRsRunner(classifier, MagicMock(), n_concurrent=1)
            with pytest.raises(RuntimeError, match="github_client"):
                await runner._process_item(item)

        classifier.classify.assert_not_called()


class TestRestPacing:
    """One token, 5 000 REST calls an hour, ~5 616 diffs for a month.

    The dial is about GitHub, not about the machine, so it is a
    ``DATASMITH_`` constant rather than anything derived from ``cpu_count()``.
    """

    async def test_concurrency_cap_bounds_diff_fetches(self) -> None:
        peak = 0
        live = 0

        async def _fetch(owner: str, repo: str, number: int) -> DiffResult:
            nonlocal peak, live
            live += 1
            peak = max(peak, live)
            await asyncio.sleep(0.01)
            live -= 1
            return DiffResult(status=DiffStatus.OK, text=_BIG_ENOUGH_PATCH, status_code=200)

        gh = MagicMock()
        gh.fetch_diff = _fetch
        classifier = MagicMock()
        classifier.classify.return_value = (False, "no")
        client = _mock_supabase()

        items = [_make_item(issue=n, patch="") for n in range(12)]
        with (
            patch("datasmith.runners.classify_prs.get_client", return_value=client),
            patch("datasmith.runners.base.get_client", return_value=client),
            patch("datasmith.runners.classify_prs.DATASMITH_CLASSIFY_DIFF_CONCURRENCY", 2),
            patch("datasmith.runners.classify_prs.DATASMITH_CLASSIFY_DIFF_MIN_INTERVAL_S", 0.0),
        ):
            runner = ClassifyPRsRunner(classifier, MagicMock(), github_client=gh, n_concurrent=12)
            await runner.run(items)

        assert peak <= 2, f"{peak} concurrent diff fetches against a cap of 2"

    async def test_fetches_are_paced_apart(self) -> None:
        starts: list[float] = []

        async def _fetch(owner: str, repo: str, number: int) -> DiffResult:
            starts.append(asyncio.get_running_loop().time())
            return DiffResult(status=DiffStatus.OK, text=_BIG_ENOUGH_PATCH, status_code=200)

        gh = MagicMock()
        gh.fetch_diff = _fetch
        classifier = MagicMock()
        classifier.classify.return_value = (False, "no")
        client = _mock_supabase()

        items = [_make_item(issue=n, patch="") for n in range(3)]
        with (
            patch("datasmith.runners.classify_prs.get_client", return_value=client),
            patch("datasmith.runners.base.get_client", return_value=client),
            patch("datasmith.runners.classify_prs.DATASMITH_CLASSIFY_DIFF_MIN_INTERVAL_S", 0.05),
        ):
            runner = ClassifyPRsRunner(classifier, MagicMock(), github_client=gh, n_concurrent=3)
            await runner.run(items)

        assert len(starts) == 3
        gaps = [b - a for a, b in itertools.pairwise(starts)]
        assert all(gap >= 0.04 for gap in gaps), gaps

    async def test_a_stall_is_logged_while_it_is_happening(self) -> None:
        """A silent wait is the failure mode; the log must not wait for the end."""
        release = asyncio.Event()

        async def _fetch(owner: str, repo: str, number: int) -> DiffResult:
            await release.wait()
            return DiffResult(status=DiffStatus.OK, text=_BIG_ENOUGH_PATCH, status_code=200)

        gh = MagicMock()
        gh.fetch_diff = _fetch
        gh._pool = MagicMock(size=1)
        classifier = MagicMock()
        classifier.classify.return_value = (False, "no")
        client = _mock_supabase()
        logs: list[str] = []

        mock_logger = MagicMock()
        mock_logger.warning.side_effect = lambda msg, *a: logs.append(msg % a if a else msg)

        with (
            patch("datasmith.runners.classify_prs.get_client", return_value=client),
            patch("datasmith.runners.base.get_client", return_value=client),
            patch("datasmith.runners.classify_prs.logger", mock_logger),
            patch("datasmith.runners.classify_prs.DATASMITH_CLASSIFY_DIFF_MIN_INTERVAL_S", 0.0),
            patch("datasmith.runners.classify_prs.DATASMITH_CLASSIFY_DIFF_STALL_LOG_S", 0.01),
        ):
            runner = ClassifyPRsRunner(classifier, MagicMock(), github_client=gh, n_concurrent=1)
            task = asyncio.create_task(runner.run([_make_item(patch="")]))
            await asyncio.sleep(0.06)
            stalled_logs = [line for line in logs if "Still waiting" in line]
            release.set()
            await task

        assert stalled_logs, "a stalled diff fetch produced no log while it was stalled"
        assert "rate-limit reset" in stalled_logs[0]

    async def test_a_raising_fetch_leaves_no_pending_task(self) -> None:
        """The stall watchdog is cancelled in a finally, not leaked."""
        gh = MagicMock()
        gh.fetch_diff = AsyncMock(side_effect=RuntimeError("boom"))
        client = _mock_supabase()

        with (
            patch("datasmith.runners.classify_prs.get_client", return_value=client),
            patch("datasmith.runners.base.get_client", return_value=client),
            patch("datasmith.runners.classify_prs.DATASMITH_CLASSIFY_DIFF_MIN_INTERVAL_S", 0.0),
        ):
            runner = ClassifyPRsRunner(MagicMock(), MagicMock(), github_client=gh, n_concurrent=1)
            before = len(asyncio.all_tasks())
            with pytest.raises(RuntimeError, match="boom"):
                await runner._process_item(_make_item(patch=""))
            await asyncio.sleep(0)
            assert len(asyncio.all_tasks()) <= before
