"""`--tasks` must override the date window for stage 6, not narrow it.

A pinned spec names the exact PR an operator wants built. If the spec set were
merely intersected with the date window, pinning a task outside that window
would select nothing, and the log would read "Synthesizing images for 0 PRs" —
which reads as "no work to do" rather than "you asked for a task the filter
excluded". That is the failure this selector prevents.
"""

from __future__ import annotations

from unittest.mock import patch

from datasmith.update.pipeline import _select_pinned_prs

_SELECT = "owner, repo, issue_number"


def _row(owner: str, repo: str, number: int) -> dict:
    return {"owner": owner, "repo": repo, "issue_number": number}


class TestSelectPinnedPrs:
    def test_keeps_only_the_pinned_pr(self):
        rows = [_row("networkx", "networkx", 8148), _row("optuna", "optuna", 4128)]
        with patch("datasmith.update.pipeline.fetch_all") as fetch:
            out = _select_pinned_prs(rows, {("networkx", "networkx", 8148)}, _SELECT)
        fetch.assert_not_called()
        assert out == [_row("networkx", "networkx", 8148)]

    def test_fetches_a_pr_outside_the_date_window(self):
        """The override. The row is absent from `rows` but must still be built."""
        rows = [_row("optuna", "optuna", 4128)]
        wanted = {("pandas-dev", "pandas", 43524)}
        with patch("datasmith.update.pipeline.fetch_all") as fetch:
            fetch.return_value = [_row("pandas-dev", "pandas", 43524)]
            out = _select_pinned_prs(rows, wanted, _SELECT)
        assert out == [_row("pandas-dev", "pandas", 43524)]
        assert fetch.call_count == 1
        assert fetch.call_args.kwargs["filters"] == {
            "owner": "pandas-dev",
            "repo": "pandas",
            "issue_number": 43524,
        }

    def test_a_spec_that_matches_nothing_is_dropped_with_a_warning(self):
        wanted = {("nope", "nope", 1)}
        with (
            patch("datasmith.update.pipeline.fetch_all", return_value=[]),
            patch("datasmith.update.pipeline.logger") as log,
        ):
            out = _select_pinned_prs([], wanted, _SELECT)
        assert out == []
        assert log.warning.called
        assert "not found" in log.warning.call_args.args[0]

    def test_order_is_deterministic(self):
        rows = [
            _row("b", "b", 2),
            _row("a", "a", 1),
            _row("c", "c", 3),
        ]
        wanted = {("c", "c", 3), ("a", "a", 1), ("b", "b", 2)}
        with patch("datasmith.update.pipeline.fetch_all"):
            out = _select_pinned_prs(rows, wanted, _SELECT)
        assert [(r["owner"], r["issue_number"]) for r in out] == [("a", 1), ("b", 2), ("c", 3)]

    def test_mixed_present_and_absent(self):
        rows = [_row("networkx", "networkx", 8148)]
        wanted = {("networkx", "networkx", 8148), ("apache", "arrow", 44236)}
        with patch("datasmith.update.pipeline.fetch_all") as fetch:
            fetch.return_value = [_row("apache", "arrow", 44236)]
            out = _select_pinned_prs(rows, wanted, _SELECT)
        assert len(out) == 2
        assert fetch.call_count == 1  # only the absent one is queried


class TestReportDropped:
    """A pinned task must never vanish silently between selection and assembly.

    bottleneck#468 was selected by --tasks and then dropped, and the log read
    "Synthesizing images for 0 PRs". Both skip reasons logged at DEBUG, so at
    normal level the task simply disappeared. The cause was an empty
    candidate_prs row, a requirement that only matters when an LLM runs.
    """

    def test_a_dropped_pinned_task_is_named_in_a_warning(self):
        from datasmith.update.pipeline import _report_dropped

        pinned = {("pydata", "bottleneck", 468), ("networkx", "networkx", 8148)}
        items = [_row("networkx", "networkx", 8148)]
        with patch("datasmith.update.pipeline.logger") as log:
            _report_dropped(items, pinned, skipped_no_pkg=0, skipped_no_ctx=1)
        assert log.warning.called
        message = log.warning.call_args.args[0] % log.warning.call_args.args[1:]
        assert "pydata/bottleneck#468" in message
        assert "networkx/networkx#8148" not in message

    def test_no_warning_when_every_pinned_task_survived(self):
        from datasmith.update.pipeline import _report_dropped

        pinned = {("networkx", "networkx", 8148)}
        items = [_row("networkx", "networkx", 8148)]
        with patch("datasmith.update.pipeline.logger") as log:
            _report_dropped(items, pinned, skipped_no_pkg=0, skipped_no_ctx=0)
        assert not log.warning.called

    def test_skip_counts_are_reported_at_info(self):
        from datasmith.update.pipeline import _report_dropped

        with patch("datasmith.update.pipeline.logger") as log:
            _report_dropped([], None, skipped_no_pkg=12, skipped_no_ctx=4065)
        assert log.info.called
        assert log.info.call_args.args[1:] == (12, 4065)

    def test_nothing_is_logged_when_there_is_nothing_to_say(self):
        from datasmith.update.pipeline import _report_dropped

        with patch("datasmith.update.pipeline.logger") as log:
            _report_dropped([], None, skipped_no_pkg=0, skipped_no_ctx=0)
        assert not log.info.called
        assert not log.warning.called


class TestEveryStageThatShouldHonourTasks:
    """`--tasks` must reach stage 4 as well as stages 6 and 7.

    Stage 4 ignored it. The only way to re-resolve one repository's
    dependencies was to re-resolve the whole date window, which made a thin
    `env_payload` impractical to iterate on. dwavesystems/dimod's recent rows
    carry one dependency where an older row carries 22 -- exactly the shape
    that makes a benchmark module fail to import -- and there was no cheap way
    to re-run just that repo.
    """

    @staticmethod
    def _source() -> str:
        from pathlib import Path

        import datasmith.update.pipeline as mod

        return Path(mod.__file__).read_text(encoding="utf-8")

    def test_resolve_packages_applies_the_pin(self):
        src = self._source()
        body = src[src.index("async def _resolve_packages") : src.index("async def _resolve_packages") + 2500]
        assert "self._task_specs" in body, "stage 4 must honour --tasks"
        assert "_select_pinned_prs" in body

    def test_resolve_packages_selects_issue_number(self):
        """_select_pinned_prs keys on issue_number, so the query must fetch it."""
        src = self._source()
        body = src[src.index("async def _resolve_packages") : src.index("async def _resolve_packages") + 1200]
        assert "issue_number" in body, "the pin cannot key rows without issue_number"

    def test_the_pin_is_applied_before_the_already_resolved_skip(self):
        """Otherwise a pinned task is filtered by the window before it is seen."""
        src = self._source()
        start = src.index("async def _resolve_packages")
        body = src[start : start + 3000]
        assert body.index("_select_pinned_prs") < body.index("Skip items already in the packages table")
