"""Tests for datasmith.github.links — extract_references and scrape_links."""

from __future__ import annotations

from datasmith.github.links import extract_references, scrape_links
from datasmith.github.models import PR, IssueExpanded


class TestExtractReferences:
    def test_extract_hash_reference(self) -> None:
        refs = extract_references("Fixes #123 and #456", "owner", "repo")
        assert ("owner", "repo", 123) in refs
        assert ("owner", "repo", 456) in refs

    def test_extract_cross_repo(self) -> None:
        refs = extract_references("See pandas-dev/pandas#789", "owner", "repo")
        assert ("pandas-dev", "pandas", 789) in refs

    def test_extract_full_url(self) -> None:
        text = "Related: https://github.com/numpy/numpy/issues/100"
        refs = extract_references(text, "owner", "repo")
        assert ("numpy", "numpy", 100) in refs

    def test_extract_pull_url(self) -> None:
        text = "See https://github.com/org/lib/pull/42"
        refs = extract_references(text, "owner", "repo")
        assert ("org", "lib", 42) in refs

    def test_no_duplicates(self) -> None:
        text = "#10 and #10 and https://github.com/owner/repo/issues/10"
        refs = extract_references(text, "owner", "repo")
        matching = [r for r in refs if r == ("owner", "repo", 10)]
        assert len(matching) == 1

    def test_empty_text(self) -> None:
        refs = extract_references("", "owner", "repo")
        assert refs == []

    def test_no_references(self) -> None:
        refs = extract_references("No issues here", "owner", "repo")
        assert refs == []

    def test_mixed_references(self) -> None:
        text = "Fixes #1, see other-org/other-repo#2 and https://github.com/x/y/issues/3"
        refs = extract_references(text, "myorg", "myrepo")
        assert ("myorg", "myrepo", 1) in refs
        assert ("other-org", "other-repo", 2) in refs
        assert ("x", "y", 3) in refs


class TestScrapeLinks:
    async def test_bfs_basic(self) -> None:
        """Basic BFS from a PR with one hash reference."""
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            title="Fix for #10",
            body="",
        )

        issue_10 = IssueExpanded(
            number=10,
            title="Slow query",
            description="The query is slow",
        )

        async def get_issue_fn(o: str, r: str, n: int) -> IssueExpanded | None:
            if n == 10:
                return issue_10
            return None

        results = await scrape_links(pr, get_issue_fn, depth=1)
        assert len(results) == 1
        assert results[0].number == 10

    async def test_bfs_no_cycles(self) -> None:
        """A refers to B, B refers to A — should not loop."""
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            title="See #10",
            body="",
        )

        issue_a = IssueExpanded(
            number=10,
            title="Issue A",
            description="See #20",
        )
        issue_b = IssueExpanded(
            number=20,
            title="Issue B",
            description="See #10",
        )

        async def get_issue_fn(o: str, r: str, n: int) -> IssueExpanded | None:
            if n == 10:
                return issue_a
            if n == 20:
                return issue_b
            return None

        results = await scrape_links(pr, get_issue_fn, depth=3)
        numbers = [r.number for r in results]
        assert numbers.count(10) == 1
        assert numbers.count(20) == 1

    async def test_bfs_depth_limit(self) -> None:
        """With depth=0, only direct references from the PR are fetched."""
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            title="See #10",
            body="",
        )

        issue_10 = IssueExpanded(
            number=10,
            title="Points to #20",
            description="See #20",
        )
        issue_20 = IssueExpanded(
            number=20,
            title="Deep issue",
            description="",
        )

        async def get_issue_fn(o: str, r: str, n: int) -> IssueExpanded | None:
            if n == 10:
                return issue_10
            if n == 20:
                return issue_20
            return None

        results = await scrape_links(pr, get_issue_fn, depth=0)
        # depth=0 means we fetch #10 from the PR seed, but don't follow its refs
        assert len(results) == 1
        assert results[0].number == 10

    async def test_bfs_count_limit(self) -> None:
        """Limit the total number of results."""
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            title="See #10 #20 #30 #40 #50",
            body="",
        )

        async def get_issue_fn(o: str, r: str, n: int) -> IssueExpanded | None:
            return IssueExpanded(number=n, title=f"Issue {n}")

        results = await scrape_links(pr, get_issue_fn, depth=0, limit=3)
        assert len(results) == 3

    async def test_bfs_only_issues_filter(self) -> None:
        """When only_issues=True, skip items that have a merged_at attribute."""
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            title="See #10 #20",
            body="",
        )

        class FakePR:
            """Simulates a PR-like result with merged_at."""

            def __init__(self, number: int) -> None:
                self.number = number
                self.title = f"PR {number}"
                self.description = ""
                self.comments: list[str] = []
                self.merged_at = "2024-01-01"

        issue_10 = IssueExpanded(number=10, title="Real issue")

        async def get_issue_fn(o: str, r: str, n: int) -> IssueExpanded | None:
            if n == 10:
                return issue_10
            if n == 20:
                return FakePR(20)  # type: ignore[return-value]
            return None

        results = await scrape_links(pr, get_issue_fn, depth=0, only_issues=True)
        # #20 has merged_at so it should be skipped
        assert len(results) == 1
        assert results[0].number == 10

    async def test_bfs_get_issue_returns_none(self) -> None:
        """When get_issue_fn returns None for a ref, it is skipped gracefully."""
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            title="See #10 #999",
            body="",
        )

        issue_10 = IssueExpanded(number=10, title="Exists")

        async def get_issue_fn(o: str, r: str, n: int) -> IssueExpanded | None:
            if n == 10:
                return issue_10
            return None

        results = await scrape_links(pr, get_issue_fn, depth=1)
        assert len(results) == 1
        assert results[0].number == 10

    async def test_bfs_cross_repo_references(self) -> None:
        """References to other repos are followed."""
        pr = PR(
            repository="owner/repo",
            issue_number=1,
            title="",
            body="See numpy/numpy#42",
        )

        issue_42 = IssueExpanded(number=42, title="Numpy issue")

        async def get_issue_fn(o: str, r: str, n: int) -> IssueExpanded | None:
            if o == "numpy" and r == "numpy" and n == 42:
                return issue_42
            return None

        results = await scrape_links(pr, get_issue_fn, depth=1)
        assert len(results) == 1
        assert results[0].number == 42
