"""Tests for datasmith.runners.render_problems — RenderProblemsRunner."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from datasmith.runners.render_problems import RenderProblemsRunner


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


def _make_item(
    owner: str = "numpy",
    repo: str = "numpy",
    issue: int = 42,
    title: str = "Speed up sort",
    body: str = "This PR speeds up sorting.",
) -> dict[str, object]:
    return {
        "owner": owner,
        "repo": repo,
        "issue_number": issue,
        "title": title,
        "body": body,
        "repo_description": "A scientific computing library",
    }


class TestRendersAndStoresProblem:
    async def test_renders_and_stores_problem_statement(self) -> None:
        """render_problem scrapes issues and writes to DB."""
        mock_client = _mock_supabase()

        gh = AsyncMock()
        gh.get_issue_expanded = AsyncMock(return_value=None)

        with (
            patch("datasmith.runners.render_problems.get_logger"),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
            patch("datasmith.runners.render_problems.get_client", return_value=mock_client),
            patch(
                "datasmith.github.render.render_problem_statement",
                return_value="Rendered problem text",
            ) as mock_render,
        ):
            runner = RenderProblemsRunner(gh=gh, n_concurrent=1)
            await runner.run([_make_item()])

        # render_problem_statement was called with a PR and rendering options
        assert mock_render.called
        call_kwargs = mock_render.call_args
        assert call_kwargs.kwargs["anonymize"] is True
        assert call_kwargs.kwargs["extract"] is False
        assert call_kwargs.kwargs["repo_description"] == "A scientific computing library"

        # rendered_problem was persisted to DB
        mock_client.table.assert_any_call("pull_requests")


class TestIncludesScrapedIssues:
    async def test_render_includes_scraped_issues(self) -> None:
        """scrape_links results are passed through to render_problem_statement."""
        mock_client = _mock_supabase()

        from datasmith.github.models import IssueExpanded

        fake_issue = IssueExpanded(
            number=99,
            title="Slow sort",
            description="Sorting is too slow",
            comments=["Confirmed"],
        )
        gh = AsyncMock()
        gh.get_issue_expanded = AsyncMock(return_value=fake_issue)

        item = _make_item(body="Fixes #99 — sorting is slow")

        with (
            patch("datasmith.runners.render_problems.get_logger"),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
            patch("datasmith.runners.render_problems.get_client", return_value=mock_client),
            patch(
                "datasmith.github.render.render_problem_statement",
                return_value="Rendered with issues",
            ) as mock_render,
        ):
            runner = RenderProblemsRunner(gh=gh, n_concurrent=1)
            await runner.run([item])

        # The scraped issue should have been passed to render
        call_kwargs = mock_render.call_args
        issues_arg = call_kwargs.kwargs["issues"]
        assert len(issues_arg) == 1
        assert issues_arg[0].number == 99
        assert issues_arg[0].title == "Slow sort"


class TestProblemExtractorIsHoisted:
    """One extractor per stage, not one per PR.

    ``ProblemExtractor()`` was being constructed inside every
    ``asyncio.to_thread`` call, so each item rebuilt the dspy.Signature
    subclass and re-ran ``ensure_configured`` before it could ask anything.
    """

    async def test_one_extractor_serves_every_item(self) -> None:
        mock_client = _mock_supabase()
        gh = AsyncMock()
        gh.get_issue_expanded = AsyncMock(return_value=None)

        extraction = MagicMock()
        extraction.to_problem_markdown.return_value = "problem"
        extraction.initial_observations = "obs"
        extraction.triage_attempts = None
        extraction.solution_overview = None
        extraction.solution_observations = None

        instances: list[MagicMock] = []

        def _factory() -> MagicMock:
            inst = MagicMock()
            inst.extract_problem.return_value = extraction
            instances.append(inst)
            return inst

        with (
            patch("datasmith.agents.extractors.ProblemExtractor", _factory),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
            patch("datasmith.runners.render_problems.get_client", return_value=mock_client),
            patch("datasmith.github.render.render_problem_statement", return_value="Rendered"),
        ):
            runner = RenderProblemsRunner(gh=gh, n_concurrent=2)
            await runner.run([_make_item(issue=n) for n in (1, 2, 3)])

        assert len(instances) == 1, f"{len(instances)} extractors built for 3 PRs"
        assert instances[0].extract_problem.call_count == 3
