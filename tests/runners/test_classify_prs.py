"""Tests for datasmith.runners.classify_prs — ClassifyPRsRunner."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from datasmith.runners.classify_prs import ClassifyPRsRunner


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
