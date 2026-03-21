"""Tests for datasmith.runners.scrape_commits — ScrapeCommitsRunner."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from datasmith.runners.scrape_commits import ScrapeCommitsRunner

_SAMPLE_DIFF = "--- a/src/core.py\n+++ b/src/core.py\n-old\n+new\n" + "\n".join(
    f"-line{i}\n+fast{i}" for i in range(20)
)
_SAMPLE_FILES = [
    {"filename": "src/core.py", "additions": 20, "deletions": 20},
]


def _mock_supabase() -> MagicMock:
    """Create a mock Supabase client with fluent API."""
    client = MagicMock()
    table = MagicMock()
    client.table.return_value = table
    table.upsert.return_value = table
    table.insert.return_value = table
    table.execute.return_value = MagicMock()
    return client


def _make_repo_response(default_branch: str = "main") -> MagicMock:
    """Create a mock response for the /repos/{owner}/{repo} endpoint."""
    resp = MagicMock()
    resp.json.return_value = {"default_branch": default_branch}
    return resp


def _make_gh_client(
    pages: list[list[dict[str, Any]]],
    diff: str = _SAMPLE_DIFF,
    files: list[dict[str, Any]] | None = None,
    default_branch: str = "main",
) -> MagicMock:
    """Create a mock GitHub client with paginate, _request, get_diff, get_files."""
    gh_client = MagicMock()

    # Mock _request for the /repos/{owner}/{repo} call
    gh_client._request = AsyncMock(return_value=_make_repo_response(default_branch))

    # Mock paginate as an async generator
    async def _paginate(*args: Any, **kwargs: Any):  # type: ignore[no-untyped-def]
        for page in pages:
            yield page

    gh_client.paginate = _paginate

    gh_client.get_diff = AsyncMock(return_value=diff)
    gh_client.get_files = AsyncMock(return_value=files if files is not None else _SAMPLE_FILES)
    return gh_client


def _make_pr(
    number: int,
    merged: bool = True,
    title: str | None = None,
    created_at: str = "2024-01-01T00:00:00Z",
    merged_at: str | None = None,
    merge_commit_sha: str | None = None,
) -> dict[str, Any]:
    """Create a mock PR data dict."""
    pr: dict[str, Any] = {
        "number": number,
        "title": title if title is not None else f"PR #{number}",
        "body": f"Body of PR #{number}",
        "state": "closed",
        "created_at": created_at,
        "closed_at": "2024-01-02T00:00:00Z",
        "merge_commit_sha": merge_commit_sha if merge_commit_sha is not None else f"abc{number}",
        "base": {"sha": f"base{number}"},
        "head": {"sha": f"head{number}"},
        "labels": [{"name": "perf"}, {"name": "bug"}],
    }
    if merged:
        pr["merged_at"] = merged_at or "2024-01-02T00:00:00Z"
    else:
        pr["merged_at"] = None
    return pr


def _pr_records(mock_client: MagicMock) -> list[dict[str, Any]]:
    """Extract PR upsert records (filter out runner_progress upserts)."""
    return [c.args[0] for c in mock_client.table.return_value.upsert.call_args_list if "issue_number" in c.args[0]]


class TestStoresPRData:
    async def test_stores_pr_data(self) -> None:
        """Mock GitHub API with merged PRs, verify upsert to pull_requests."""
        mock_client = _mock_supabase()
        gh_client = _make_gh_client([[_make_pr(1), _make_pr(2)]])

        with (
            patch("datasmith.runners.scrape_commits.get_client", return_value=mock_client),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = ScrapeCommitsRunner(github_client=gh_client, n_concurrent=1)
            await runner.run([("pandas-dev", "pandas")])

        assert len(_pr_records(mock_client)) == 2

    async def test_stores_patch_and_file_changes(self) -> None:
        """Diff and file list from GitHub should be stored in the record."""
        mock_client = _mock_supabase()
        gh_client = _make_gh_client(
            [[_make_pr(1, title="PERF: speed up sort")]],
            diff="--- a/f.py\n+++ b/f.py\n-old\n+new",
            files=[{"filename": "src/f.py", "additions": 5, "deletions": 3}],
        )

        with (
            patch("datasmith.runners.scrape_commits.get_client", return_value=mock_client),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = ScrapeCommitsRunner(github_client=gh_client, n_concurrent=1)
            await runner.run([("pandas-dev", "pandas")])

        records = _pr_records(mock_client)
        assert len(records) == 1
        assert records[0]["patch"] == "--- a/f.py\n+++ b/f.py\n-old\n+new"
        assert records[0]["file_changes"] == [{"filename": "src/f.py", "additions": 5, "deletions": 3}]

    async def test_empty_diff_omits_patch(self) -> None:
        """When get_diff returns empty string, patch/file_changes keys should be absent."""
        mock_client = _mock_supabase()
        gh_client = _make_gh_client([[_make_pr(1)]], diff="", files=[])

        with (
            patch("datasmith.runners.scrape_commits.get_client", return_value=mock_client),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = ScrapeCommitsRunner(github_client=gh_client, n_concurrent=1)
            await runner.run([("pandas-dev", "pandas")])

        records = _pr_records(mock_client)
        assert len(records) == 1
        assert "patch" not in records[0]
        assert "file_changes" not in records[0]


class TestSymbolicCompliance:
    async def test_perf_title_sets_symbolic_true(self) -> None:
        """PR with a performance keyword in title should set is_performance_commit_symbolic=True."""
        mock_client = _mock_supabase()
        gh_client = _make_gh_client([[_make_pr(1, title="PERF: speed up sort")]])

        with (
            patch("datasmith.runners.scrape_commits.get_client", return_value=mock_client),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = ScrapeCommitsRunner(github_client=gh_client, n_concurrent=1)
            await runner.run([("pandas-dev", "pandas")])

        records = _pr_records(mock_client)
        assert len(records) == 1
        assert records[0]["is_performance_commit_symbolic"] is True

    async def test_non_perf_title_sets_symbolic_false(self) -> None:
        """PR without performance keywords should set is_performance_commit_symbolic=False."""
        mock_client = _mock_supabase()
        gh_client = _make_gh_client([[_make_pr(1, title="Update docs and README")]])

        with (
            patch("datasmith.runners.scrape_commits.get_client", return_value=mock_client),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = ScrapeCommitsRunner(github_client=gh_client, n_concurrent=1)
            await runner.run([("pandas-dev", "pandas")])

        records = _pr_records(mock_client)
        assert len(records) == 1
        assert records[0]["is_performance_commit_symbolic"] is False

    async def test_symbolic_uses_all_inputs(self) -> None:
        """Symbolic compliance should account for patch size and file changes."""
        mock_client = _mock_supabase()
        # Perf title but only test files → should fail file compliance
        gh_client = _make_gh_client(
            [[_make_pr(1, title="PERF: speed up tests")]],
            diff=_SAMPLE_DIFF,
            files=[{"filename": "tests/test_x.py", "additions": 5, "deletions": 3}],
        )

        with (
            patch("datasmith.runners.scrape_commits.get_client", return_value=mock_client),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = ScrapeCommitsRunner(github_client=gh_client, n_concurrent=1)
            await runner.run([("pandas-dev", "pandas")])

        records = _pr_records(mock_client)
        assert len(records) == 1
        assert records[0]["is_performance_commit_symbolic"] is False


class TestSkipsUnmerged:
    async def test_skips_unmerged(self) -> None:
        """Mock PRs without merged_at, verify they are not stored."""
        mock_client = _mock_supabase()
        gh_client = _make_gh_client([
            [
                _make_pr(1, merged=False),
                _make_pr(2, merged=False),
                _make_pr(3, merged=True),
            ]
        ])

        with (
            patch("datasmith.runners.scrape_commits.get_client", return_value=mock_client),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = ScrapeCommitsRunner(github_client=gh_client, n_concurrent=1)
            await runner.run([("numpy", "numpy")])

        # Only the merged PR (#3) should produce a pull_requests upsert
        assert len(_pr_records(mock_client)) == 1


class TestPagination:
    async def test_paginates_multiple_pages(self) -> None:
        """PRs from multiple pages should all be upserted."""
        mock_client = _mock_supabase()
        page1 = [_make_pr(i) for i in range(1, 4)]
        page2 = [_make_pr(i) for i in range(4, 6)]
        gh_client = _make_gh_client([page1, page2])

        with (
            patch("datasmith.runners.scrape_commits.get_client", return_value=mock_client),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = ScrapeCommitsRunner(github_client=gh_client, n_concurrent=1)
            await runner.run([("pandas-dev", "pandas")])

        assert len(_pr_records(mock_client)) == 5


class TestDateFiltering:
    async def test_date_filtering_skips_out_of_range(self) -> None:
        """PRs with merged_at outside [since, until) are skipped."""
        mock_client = _mock_supabase()
        gh_client = _make_gh_client([
            [
                _make_pr(1, created_at="2024-06-15T00:00:00Z", merged_at="2024-06-15T12:00:00Z"),
                _make_pr(2, created_at="2024-05-01T00:00:00Z", merged_at="2024-05-01T12:00:00Z"),
                _make_pr(3, created_at="2024-07-01T00:00:00Z", merged_at="2024-07-01T12:00:00Z"),
            ]
        ])

        with (
            patch("datasmith.runners.scrape_commits.get_client", return_value=mock_client),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = ScrapeCommitsRunner(
                github_client=gh_client, n_concurrent=1, since="2024-06-01", until="2024-07-01"
            )
            await runner.run([("pandas-dev", "pandas")])

        records = _pr_records(mock_client)
        assert len(records) == 1
        assert records[0]["issue_number"] == 1

    async def test_early_termination(self) -> None:
        """Pagination stops when created_at < since (sorted created desc)."""
        mock_client = _mock_supabase()
        page1 = [
            _make_pr(1, created_at="2024-06-15T00:00:00Z", merged_at="2024-06-15T12:00:00Z"),
        ]
        # Page 2 has a PR created before our window — should trigger early stop
        page2 = [
            _make_pr(2, created_at="2024-05-01T00:00:00Z", merged_at="2024-05-01T12:00:00Z"),
        ]
        # Page 3 should never be reached
        page3 = [
            _make_pr(3, created_at="2024-06-10T00:00:00Z", merged_at="2024-06-10T12:00:00Z"),
        ]
        gh_client = _make_gh_client([page1, page2, page3])

        with (
            patch("datasmith.runners.scrape_commits.get_client", return_value=mock_client),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = ScrapeCommitsRunner(
                github_client=gh_client, n_concurrent=1, since="2024-06-01", until="2024-07-01"
            )
            await runner.run([("pandas-dev", "pandas")])

        records = _pr_records(mock_client)
        # Only PR #1 from page 1 should be stored; PR #3 on page 3 is never reached
        assert len(records) == 1
        assert records[0]["issue_number"] == 1


class TestDeduplication:
    async def test_deduplication_by_sha(self) -> None:
        """Duplicate merge_commit_sha across pages should only produce one upsert."""
        mock_client = _mock_supabase()
        gh_client = _make_gh_client([
            [_make_pr(1, merge_commit_sha="deadbeef"), _make_pr(2, merge_commit_sha="deadbeef")],
        ])

        with (
            patch("datasmith.runners.scrape_commits.get_client", return_value=mock_client),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = ScrapeCommitsRunner(github_client=gh_client, n_concurrent=1)
            await runner.run([("pandas-dev", "pandas")])

        assert len(_pr_records(mock_client)) == 1
