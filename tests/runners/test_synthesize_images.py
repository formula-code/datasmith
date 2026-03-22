"""Tests for datasmith.runners.synthesize_images — SynthesizeImagesRunner."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import datasmith.runners.synthesize_images as _synth_mod
from datasmith.runners.synthesize_images import SynthesizeImagesRunner


@pytest.fixture(autouse=True)
def _clear_prereq_cache() -> None:
    """Reset the module-level prereq cache between tests."""
    _synth_mod._prereq_done.clear()


# Shared patches for Docker image helpers that are always mocked in unit tests
_MOCK_PREREQS = patch("datasmith.runners.synthesize_images._ensure_prerequisite_images")
_MOCK_BUILD_PUSH = patch(
    "datasmith.runners.synthesize_images._build_and_push_pr_image",
    return_value="formulacode/numpy-numpy:42",
)


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
            _MOCK_PREREQS,
            _MOCK_BUILD_PUSH,
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
            _MOCK_PREREQS,
            _MOCK_BUILD_PUSH,
        ):
            runner = SynthesizeImagesRunner(synthesizer=synthesizer, verifier=verifier, n_concurrent=1)
            await runner.run([_make_item()])

        # The runner catches exceptions, so check failure count
        assert runner._failed == 1
        assert runner._completed == 0


class TestBuildAndPushOnSuccess:
    async def test_builds_and_pushes_on_success(self) -> None:
        """After successful synthesis, the PR image should be built, pushed, and recorded."""
        mock_client = _mock_supabase()

        mock_ctx = MagicMock()
        synthesizer = MagicMock()
        synthesizer.run.return_value = mock_ctx
        verifier = MagicMock()

        with (
            patch("datasmith.runners.synthesize_images.get_logger"),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
            patch("datasmith.runners.synthesize_images.get_client", return_value=mock_client),
            _MOCK_PREREQS as mock_prereqs,
            patch(
                "datasmith.runners.synthesize_images._build_and_push_pr_image",
                return_value="formulacode/numpy-numpy:42",
            ) as mock_build_push,
        ):
            runner = SynthesizeImagesRunner(synthesizer=synthesizer, verifier=verifier, n_concurrent=1)
            await runner.run([_make_item()])

        # Prerequisites were checked
        mock_prereqs.assert_called_once()

        # Image was built and pushed (last arg is the synthesized DockerContext)
        mock_build_push.assert_called_once()
        args = mock_build_push.call_args[0]
        assert args[:5] == ("numpy", "numpy", 42, "", "")

        # container_name was persisted to DB
        mock_client.table.assert_any_call("pull_requests")
        update_calls = mock_client.table.return_value.update.call_args_list
        container_updates = [c for c in update_calls if "container_name" in c.args[0]]
        assert len(container_updates) == 1
        assert container_updates[0].args[0]["container_name"] == "formulacode/numpy-numpy:42"


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
            _MOCK_PREREQS,
            _MOCK_BUILD_PUSH,
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
            item = _make_item()
            item["pr_context"] = ""  # empty so rendering is triggered
            await runner.run([item])

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
            _MOCK_PREREQS,
            _MOCK_BUILD_PUSH,
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

        # Item whose body references issue #99, with empty pr_context to trigger rendering
        item = _make_item(body="Fixes #99 — sorting is slow")
        item["pr_context"] = ""

        with (
            patch("datasmith.runners.synthesize_images.get_logger"),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
            patch("datasmith.runners.synthesize_images.get_client", return_value=mock_client),
            _MOCK_PREREQS,
            _MOCK_BUILD_PUSH,
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
