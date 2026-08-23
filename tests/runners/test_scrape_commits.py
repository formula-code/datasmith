"""Tests for datasmith.runners.scrape_commits — ScrapeCommitsRunner.

Stage 2 now asks GitHub for the PRs merged in the window instead of paging
every PR by creation order and filtering client-side, so these tests are
written against ``fetch_merged_prs`` returning a list rather than
``paginate_merged_prs`` yielding pages.

The window contract is the point of the file: a PR merged inside the window is
stored no matter when it was created.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from datasmith.github.client import Truncated
from datasmith.runners.scrape_commits import ScrapeCommitsRunner

_SINCE = "2024-06-01"
_UNTIL = "2024-07-01"

_CORE_FILES: list[dict[str, Any]] = [{"filename": "src/core.py", "additions": 20, "deletions": 20}]


def _mock_supabase() -> MagicMock:
    """A Supabase client mock with the fluent API BaseRunner uses."""
    client = MagicMock()
    table = MagicMock()
    client.table.return_value = table
    table.upsert.return_value = table
    table.insert.return_value = table
    table.execute.return_value = MagicMock()
    return client


def _make_pr(
    number: int,
    *,
    title: str | None = None,
    created_at: str = "2024-06-14T00:00:00Z",
    merged_at: str | None = "2024-06-15T12:00:00Z",
    merge_commit_sha: str | None = None,
    file_changes: list[dict[str, Any]] | None = None,
    changed_files: int | None = None,
) -> dict[str, Any]:
    """Build a PR dict shaped like ``_normalise_pr_node``'s output."""
    files = _CORE_FILES if file_changes is None else file_changes
    return {
        "number": number,
        "title": title if title is not None else f"PERF: speed up thing {number}",
        "body": f"Body of PR #{number}",
        "state": "closed",
        "created_at": created_at,
        "merged_at": merged_at,
        "closed_at": "2024-06-15T12:00:00Z",
        "merge_commit_sha": merge_commit_sha if merge_commit_sha is not None else f"abc{number}",
        "base": {"sha": f"base{number}"},
        "head": {"sha": f"head{number}"},
        "labels": [{"name": "perf"}, {"name": "bug"}],
        "file_changes": files,
        "changed_files": len(files) if changed_files is None else changed_files,
    }


def _make_gh_client(prs: list[dict[str, Any]] | None = None, **kwargs: Any) -> MagicMock:
    """A GitHub client mock whose only used method is ``fetch_merged_prs``."""
    gh = MagicMock()
    gh.fetch_merged_prs = AsyncMock(return_value=list(prs or []), **kwargs)
    gh.get_diff = AsyncMock(return_value="--- a/f.py\n+++ b/f.py\n-old\n+new")
    gh.get_files = AsyncMock(return_value=_CORE_FILES)
    return gh


class _Harness:
    """The mocks a stage-2 run writes through."""

    def __init__(self, existing: list[dict[str, Any]] | None) -> None:
        self.afetch_all = AsyncMock(return_value=list(existing or []))
        self.abatch_upsert = AsyncMock(side_effect=lambda table, rows, **kw: len(rows))
        self.db = _mock_supabase()
        self.runner: ScrapeCommitsRunner | None = None

    @property
    def records(self) -> list[dict[str, Any]]:
        """Every row handed to ``abatch_upsert`` for ``pull_requests``."""
        out: list[dict[str, Any]] = []
        for c in self.abatch_upsert.call_args_list:
            table, rows = c.args[0], c.args[1]
            if table == "pull_requests":
                out.extend(rows)
        return out


async def _run(
    gh: MagicMock,
    items: list[tuple[str, str]],
    *,
    existing: list[dict[str, Any]] | None = None,
    since: str = _SINCE,
    until: str = _UNTIL,
) -> _Harness:
    """Run the stage against mocked GitHub and database layers."""
    harness = _Harness(existing)
    with (
        patch("datasmith.runners.scrape_commits.afetch_all", harness.afetch_all),
        patch("datasmith.runners.scrape_commits.abatch_upsert", harness.abatch_upsert),
        patch("datasmith.runners.base.get_client", return_value=harness.db),
    ):
        runner = ScrapeCommitsRunner(github_client=gh, since=since, until=until, n_concurrent=1)
        harness.runner = runner
        await runner.run(items)
    return harness


class TestWindowContract:
    """The window means merged_at, half-open [since, until)."""

    async def test_pr_created_before_the_window_is_stored(self) -> None:
        """Created outside, merged inside → stored.

        This inverts the former ``test_early_termination``, which required that
        a PR merged inside the window be *dropped* because pagination in
        created-order had already stopped.  That assertion encoded the defect:
        46 of 81 in-window PRs were lost on the measured day, biased towards
        repositories whose review takes longer than a day.
        """
        gh = _make_gh_client([
            _make_pr(1, created_at="2024-01-05T00:00:00Z", merged_at="2024-06-15T12:00:00Z"),
            _make_pr(2, created_at="2024-06-10T00:00:00Z", merged_at="2024-06-10T18:00:00Z"),
        ])

        harness = await _run(gh, [("pandas-dev", "pandas")])

        assert [r["issue_number"] for r in harness.records] == [1, 2]
        assert harness.records[0]["created_at"] == "2024-01-05T00:00:00Z"

    async def test_window_is_expressed_in_the_query(self) -> None:
        """The bounds go to GitHub as timezone-aware datetimes, upper bound exclusive."""
        gh = _make_gh_client([_make_pr(1)])

        await _run(gh, [("pandas-dev", "pandas")])

        args = gh.fetch_merged_prs.call_args.args
        assert args[0] == "pandas-dev"
        assert args[1] == "pandas"
        assert args[2] == datetime(2024, 6, 1, tzinfo=UTC)
        assert args[3] == datetime(2024, 7, 1, tzinfo=UTC)

    async def test_naive_and_zulu_bounds_are_read_as_utc(self) -> None:
        """A naive bound is UTC, not a per-repository ValueError from the fetcher."""
        gh = _make_gh_client([_make_pr(1)])

        await _run(
            gh,
            [("pandas-dev", "pandas")],
            since="2024-06-01T06:00:00",
            until="2024-07-01T06:00:00Z",
        )

        args = gh.fetch_merged_prs.call_args.args
        assert args[2] == datetime(2024, 6, 1, 6, tzinfo=UTC)
        assert args[3] == datetime(2024, 7, 1, 6, tzinfo=UTC)

    def test_missing_bound_is_refused(self) -> None:
        """There is no unwindowed fetch to fall back to."""
        with pytest.raises(ValueError, match="explicit start date"):
            ScrapeCommitsRunner(github_client=_make_gh_client(), since="", until=_UNTIL)

    def test_subsecond_bound_is_refused(self) -> None:
        """merged: has one-second resolution; a sub-second bound shortens the window."""
        with pytest.raises(ValueError, match="whole-second"):
            ScrapeCommitsRunner(
                github_client=_make_gh_client(),
                since="2024-06-01T00:00:00.500000",
                until=_UNTIL,
            )

    def test_empty_window_is_refused(self) -> None:
        """The window is half-open, so start == end selects nothing."""
        with pytest.raises(ValueError, match="empty merge window"):
            ScrapeCommitsRunner(github_client=_make_gh_client(), since=_SINCE, until=_SINCE)


class TestGraphQLOnly:
    """Stage 2 spends no REST budget: no diff here, and no patch stored."""

    async def test_never_fetches_a_diff(self) -> None:
        gh = _make_gh_client([_make_pr(1), _make_pr(2)])

        harness = await _run(gh, [("pandas-dev", "pandas")])

        gh.get_diff.assert_not_called()
        gh.get_files.assert_not_called()
        assert len(harness.records) == 2
        assert all("patch" not in r for r in harness.records)

    async def test_file_changes_come_from_graphql(self) -> None:
        """The file list arrives on the PR node, so no REST call is needed for it."""
        files = [{"filename": "src/f.py", "additions": 5, "deletions": 3}]
        gh = _make_gh_client([_make_pr(1, file_changes=files)])

        harness = await _run(gh, [("pandas-dev", "pandas")])

        assert harness.records[0]["file_changes"] == files

    async def test_record_fields_are_carried_through(self) -> None:
        gh = _make_gh_client([_make_pr(7, title="PERF: faster groupby")])

        record = (await _run(gh, [("pandas-dev", "pandas")])).records[0]

        assert record["owner"] == "pandas-dev"
        assert record["repo"] == "pandas"
        assert record["issue_number"] == 7
        assert record["title"] == "PERF: faster groupby"
        assert record["merged_at"] == "2024-06-15T12:00:00Z"
        assert record["merge_commit_sha"] == "abc7"
        assert record["base_sha"] == "base7"
        assert record["head_sha"] == "head7"
        assert record["labels"] == ["perf", "bug"]


class TestSymbolicCompliance:
    """symbolic is written from the two components GraphQL can answer."""

    async def test_perf_title_sets_symbolic_true(self) -> None:
        gh = _make_gh_client([_make_pr(1, title="PERF: speed up sort")])

        harness = await _run(gh, [("pandas-dev", "pandas")])

        assert harness.records[0]["is_performance_commit_symbolic"] is True

    async def test_non_perf_title_sets_symbolic_false(self) -> None:
        gh = _make_gh_client([_make_pr(1, title="Update docs and README")])

        harness = await _run(gh, [("pandas-dev", "pandas")])

        assert harness.records[0]["is_performance_commit_symbolic"] is False

    async def test_test_only_changes_fail_file_compliance(self) -> None:
        gh = _make_gh_client([
            _make_pr(
                1,
                title="PERF: speed up tests",
                file_changes=[{"filename": "tests/test_x.py", "additions": 5, "deletions": 3}],
            )
        ])

        harness = await _run(gh, [("pandas-dev", "pandas")])

        assert harness.records[0]["is_performance_commit_symbolic"] is False

    async def test_truncated_file_list_is_not_trusted(self) -> None:
        """files(first: 100) truncated and the REST fallback came back empty.

        The 500-file guard cannot fire and the 40 000-line sum undercounts, so
        the screen would pass a PR it must reject.  totalCount stays truthful,
        so a short list is refused rather than believed.
        """
        gh = _make_gh_client([
            _make_pr(
                1,
                title="PERF: enormous refactor",
                file_changes=[{"filename": f"src/f{i}.py", "additions": 1, "deletions": 1} for i in range(100)],
                changed_files=3000,
            )
        ])

        harness = await _run(gh, [("pandas-dev", "pandas")])

        assert harness.records[0]["is_performance_commit_symbolic"] is False

    async def test_patch_size_component_is_not_applied(self) -> None:
        """No patch is available, so the third component is skipped, not guessed.

        A PR whose title and files pass is symbolic-True with no patch stored;
        stage 3 fetches the diff and applies check_patch_size there.
        """
        gh = _make_gh_client([_make_pr(1, title="PERF: speed up sort")])

        harness = await _run(gh, [("pandas-dev", "pandas")])

        record = harness.records[0]
        assert record["is_performance_commit_symbolic"] is True
        assert "patch" not in record
        gh.get_diff.assert_not_called()


class TestBatchedUpsert:
    async def test_one_batched_upsert_per_repository(self) -> None:
        gh = _make_gh_client([_make_pr(i) for i in range(1, 6)])

        harness = await _run(gh, [("pandas-dev", "pandas")])

        assert harness.abatch_upsert.await_count == 1
        assert harness.abatch_upsert.call_args.args[0] == "pull_requests"
        assert len(harness.abatch_upsert.call_args.args[1]) == 5

    async def test_each_repository_gets_its_own_batch(self) -> None:
        gh = _make_gh_client([_make_pr(1), _make_pr(2)])

        harness = await _run(gh, [("pandas-dev", "pandas"), ("numpy", "numpy")])

        assert harness.abatch_upsert.await_count == 2
        owners = {r["owner"] for r in harness.records}
        assert owners == {"pandas-dev", "numpy"}

    async def test_every_record_carries_the_same_keys(self) -> None:
        """PostgREST rejects a bulk upsert whose objects disagree on their keys."""
        gh = _make_gh_client([
            _make_pr(1, file_changes=_CORE_FILES),
            _make_pr(2, file_changes=[]),
        ])

        harness = await _run(gh, [("pandas-dev", "pandas")])

        key_sets = {frozenset(r) for r in harness.records}
        assert len(key_sets) == 1
        assert harness.records[1]["file_changes"] == []


class TestWindowScopedSkipSet:
    async def test_skip_set_is_scoped_to_the_repository_and_window(self) -> None:
        """The predecessor read every stored issue_number for the repository."""
        gh = _make_gh_client([_make_pr(1)])

        harness = await _run(gh, [("pandas-dev", "pandas")])

        kwargs = harness.afetch_all.call_args.kwargs
        assert harness.afetch_all.call_args.args[0] == "pull_requests"
        assert kwargs["select"] == "issue_number"
        assert kwargs["filters"] == {"owner": "pandas-dev", "repo": "pandas"}
        assert kwargs["gte_filters"] == {"merged_at": "2024-06-01T00:00:00+00:00"}
        assert kwargs["lt_filters"] == {"merged_at": "2024-07-01T00:00:00+00:00"}

    async def test_skips_prs_already_stored(self) -> None:
        gh = _make_gh_client([_make_pr(1), _make_pr(2), _make_pr(3)])

        harness = await _run(
            gh,
            [("pandas-dev", "pandas")],
            existing=[{"issue_number": 1}, {"issue_number": 2}],
        )

        assert [r["issue_number"] for r in harness.records] == [3]

    async def test_no_database_read_when_nothing_merged(self) -> None:
        """Most repositories merge nothing in a short window; they cost one query."""
        gh = _make_gh_client([])

        harness = await _run(gh, [("pandas-dev", "pandas")])

        harness.afetch_all.assert_not_called()
        harness.abatch_upsert.assert_not_called()


class TestDeduplication:
    async def test_deduplicates_by_issue_number(self) -> None:
        """One batch per repository means a repeated primary key is fatal.

        Postgres refuses an upsert that affects the same row twice in one
        statement, so identity — the PR number — is what has to be unique in
        the payload.
        """
        gh = _make_gh_client([_make_pr(1), _make_pr(1)])

        harness = await _run(gh, [("pandas-dev", "pandas")])

        assert [r["issue_number"] for r in harness.records] == [1]

    async def test_prs_sharing_a_merge_commit_are_both_stored(self) -> None:
        """Deduplicating on merge_commit_sha dropped real PRs.

        A merge-queue batch lands several PRs under one merge commit, and
        decision 3 is to store every merged PR.
        """
        gh = _make_gh_client([
            _make_pr(1, merge_commit_sha="deadbeef"),
            _make_pr(2, merge_commit_sha="deadbeef"),
        ])

        harness = await _run(gh, [("pandas-dev", "pandas")])

        assert [r["issue_number"] for r in harness.records] == [1, 2]


class TestSkipsUnmerged:
    async def test_unmerged_pr_is_not_stored(self) -> None:
        """``is:merged`` should make this impossible; a NULL merged_at row would lie."""
        gh = _make_gh_client([_make_pr(1, merged_at=None), _make_pr(2)])

        harness = await _run(gh, [("pandas-dev", "pandas")])

        assert [r["issue_number"] for r in harness.records] == [2]


class TestFailureVisibility:
    async def test_truncation_fails_the_repository_not_the_stage(self) -> None:
        """Decision 4: the repository fails, the stage continues."""

        async def _fetch(owner: str, repo: str, since: Any, until: Any, **kwargs: Any) -> list[dict[str, Any]]:
            if repo == "posthog":
                raise Truncated("posthog: got 1000 of 5166 PRs")
            return [_make_pr(1)]

        gh = MagicMock()
        gh.fetch_merged_prs = AsyncMock(side_effect=_fetch)

        harness = await _run(gh, [("PostHog", "posthog"), ("pandas-dev", "pandas")])

        assert harness.runner is not None
        assert harness.runner._failed == 1
        assert harness.runner._completed == 1
        assert [r["repo"] for r in harness.records] == ["pandas"]
        assert call("runner_failures") in harness.db.table.call_args_list
        failure = harness.db.table.return_value.insert.call_args.args[0]
        assert "posthog" in failure["item_id"]
        assert "5166" in failure["error_message"]
