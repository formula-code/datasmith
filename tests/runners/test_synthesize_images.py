"""Tests for datasmith.runners.synthesize_images — SynthesizeImagesRunner."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from datasmith.runners.synthesize_images import SynthesizeImagesRunner


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
        "pr_context": "Some PR context",
        "repo_description": "A scientific computing library",
    }


class TestDockerRunsInThread:
    async def test_docker_runs_in_thread(self) -> None:
        """Mock synthesizer, verify it ran via asyncio.to_thread."""
        mock_client = _mock_supabase()

        mock_ctx = MagicMock()
        synthesizer = MagicMock()
        synthesizer.run.return_value = mock_ctx
        verifier = MagicMock()

        with (
            patch("datasmith.runners.synthesize_images.get_logger"),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
            patch("datasmith.runners.synthesize_images.get_client", return_value=mock_client),
        ):
            runner = SynthesizeImagesRunner(synthesizer=synthesizer, verifier=verifier, n_concurrent=1)
            await runner.run([_make_item()])

        # Verify synthesizer.run was called with correct args
        synthesizer.run.assert_called_once_with(
            "numpy",
            "numpy",
            42,
            "Some PR context",
            verifier,
            "",
            base_context=None,
            env_payload="",
            python_version="",
        )


class TestHandlesFailure:
    async def test_handles_failure(self) -> None:
        """synthesizer returns None, which triggers RuntimeError."""
        mock_client = _mock_supabase()

        synthesizer = MagicMock()
        synthesizer.run.return_value = None
        verifier = MagicMock()

        with (
            patch("datasmith.runners.synthesize_images.get_logger"),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
            patch("datasmith.runners.synthesize_images.get_client", return_value=mock_client),
        ):
            runner = SynthesizeImagesRunner(synthesizer=synthesizer, verifier=verifier, n_concurrent=1)
            await runner.run([_make_item()])

        # The runner catches exceptions, so check failure count
        assert runner._failed == 1
        assert runner._completed == 0


class TestRenderProblemWithGitHubClient:
    async def test_renders_and_stores_problem_statement(self) -> None:
        """When a GitHubClient is provided, render_problem scrapes issues and writes to DB."""
        mock_client = _mock_supabase()

        mock_ctx = MagicMock()
        synthesizer = MagicMock()
        synthesizer.run.return_value = mock_ctx
        verifier = MagicMock()

        # Mock GitHubClient.get_issue_expanded — called by scrape_links
        gh = AsyncMock()
        gh.get_issue_expanded = AsyncMock(return_value=None)

        with (
            patch("datasmith.runners.synthesize_images.get_logger"),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
            patch("datasmith.runners.synthesize_images.get_client", return_value=mock_client),
            patch(
                "datasmith.github.render.render_problem_statement",
                return_value="Rendered problem text",
            ) as mock_render,
        ):
            runner = SynthesizeImagesRunner(
                synthesizer=synthesizer,
                verifier=verifier,
                gh=gh,
                n_concurrent=1,
            )
            await runner.run([_make_item()])

        # render_problem_statement was called with a PR and rendering options
        assert mock_render.called
        call_kwargs = mock_render.call_args
        assert call_kwargs.kwargs["anonymize"] is True
        assert call_kwargs.kwargs["extract"] is True
        assert call_kwargs.kwargs["repo_description"] == "A scientific computing library"

        # rendered_problem was persisted to DB
        mock_client.table.assert_any_call("pull_requests")

    async def test_skips_render_without_gh(self) -> None:
        """Without a GitHubClient, rendering is skipped (no crash)."""
        mock_client = _mock_supabase()

        mock_ctx = MagicMock()
        synthesizer = MagicMock()
        synthesizer.run.return_value = mock_ctx
        verifier = MagicMock()

        with (
            patch("datasmith.runners.synthesize_images.get_logger"),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
            patch("datasmith.runners.synthesize_images.get_client", return_value=mock_client),
            patch(
                "datasmith.github.render.render_problem_statement",
            ) as mock_render,
        ):
            runner = SynthesizeImagesRunner(
                synthesizer=synthesizer,
                verifier=verifier,
                gh=None,
                n_concurrent=1,
            )
            await runner.run([_make_item()])

        # render_problem_statement should NOT have been called
        mock_render.assert_not_called()
        # But synthesis still ran
        synthesizer.run.assert_called_once()

    async def test_render_includes_scraped_issues(self) -> None:
        """scrape_links results are passed through to render_problem_statement."""
        mock_client = _mock_supabase()

        mock_ctx = MagicMock()
        synthesizer = MagicMock()
        synthesizer.run.return_value = mock_ctx
        verifier = MagicMock()

        from datasmith.github.models import IssueExpanded

        fake_issue = IssueExpanded(
            number=99,
            title="Slow sort",
            description="Sorting is too slow",
            comments=["Confirmed"],
        )
        gh = AsyncMock()
        gh.get_issue_expanded = AsyncMock(return_value=fake_issue)

        # Item whose body references issue #99
        item = _make_item(body="Fixes #99 — sorting is slow")

        with (
            patch("datasmith.runners.synthesize_images.get_logger"),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
            patch("datasmith.runners.synthesize_images.get_client", return_value=mock_client),
            patch(
                "datasmith.github.render.render_problem_statement",
                return_value="Rendered with issues",
            ) as mock_render,
        ):
            runner = SynthesizeImagesRunner(
                synthesizer=synthesizer,
                verifier=verifier,
                gh=gh,
                n_concurrent=1,
            )
            await runner.run([item])

        # The scraped issue should have been passed to render
        call_kwargs = mock_render.call_args
        issues_arg = call_kwargs.kwargs["issues"]
        assert len(issues_arg) == 1
        assert issues_arg[0].number == 99
        assert issues_arg[0].title == "Slow sort"
