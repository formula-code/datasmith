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
_MOCK_BUILD = patch(
    "datasmith.runners.synthesize_images._build_pr_image",
    return_value="formulacode/numpy-numpy:42",
)
_MOCK_PUSH = patch("datasmith.runners.synthesize_images._push_pr_image")
_MOCK_REPO_IMAGE = patch(
    "datasmith.docker.images.get_repo_image_name",
    return_value="formulacode/numpy-numpy:latest",
)
# _do_process_item looks up formulacode_task_overrides for benchmark_dest.
# fetch_all resolves its own client, so without this the "mocked" runner tests
# open a real Supabase connection -- passing only because a local DB happens to
# be running, and failing on any machine without one.
_MOCK_OVERRIDES = patch(
    "datasmith.runners.synthesize_images.fetch_overrides",
    return_value={},
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

        with (
            patch("datasmith.runners.synthesize_images.get_logger"),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
            patch("datasmith.runners.synthesize_images.get_client", return_value=mock_client),
            _MOCK_PREREQS,
            _MOCK_BUILD,
            _MOCK_PUSH,
            _MOCK_REPO_IMAGE,
            _MOCK_OVERRIDES,
        ):
            runner = SynthesizeImagesRunner(synthesizer=synthesizer, n_concurrent=1)
            await runner.run([_make_item()])

        # Verify synthesizer.run was called with correct args
        synthesizer.run.assert_called_once_with(
            "numpy",
            "numpy",
            42,
            "Some PR context",
            "",
            repo_image="formulacode/numpy-numpy:latest",
            env_payload="",
            python_version="",
            base_sha="",
            solution_patch="",
        )


class TestHandlesFailure:
    async def test_handles_failure(self) -> None:
        """synthesizer returns None, which triggers RuntimeError."""
        mock_client = _mock_supabase()

        synthesizer = MagicMock()
        synthesizer.run.return_value = None

        with (
            patch("datasmith.runners.synthesize_images.get_logger"),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
            patch("datasmith.runners.synthesize_images.get_client", return_value=mock_client),
            _MOCK_PREREQS,
            _MOCK_BUILD,
            _MOCK_PUSH,
            _MOCK_REPO_IMAGE,
            _MOCK_OVERRIDES,
        ):
            runner = SynthesizeImagesRunner(synthesizer=synthesizer, n_concurrent=1)
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
        with (
            patch("datasmith.runners.synthesize_images.get_logger"),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
            patch("datasmith.runners.synthesize_images.get_client", return_value=mock_client),
            _MOCK_PREREQS as mock_prereqs,
            patch(
                "datasmith.runners.synthesize_images._build_pr_image",
                return_value="formulacode/numpy-numpy:42",
            ) as mock_build,
            patch("datasmith.runners.synthesize_images._push_pr_image") as mock_push,
            _MOCK_REPO_IMAGE,
            _MOCK_OVERRIDES,
        ):
            runner = SynthesizeImagesRunner(synthesizer=synthesizer, n_concurrent=1)
            await runner.run([_make_item()])

        # Prerequisites were checked
        mock_prereqs.assert_called_once()

        # Image was built and pushed
        mock_build.assert_called_once()
        mock_push.assert_called_once()

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

        # Mock GitHubClient.get_issue_expanded — called by scrape_links
        gh = AsyncMock()
        gh.get_issue_expanded = AsyncMock(return_value=None)

        with (
            patch("datasmith.runners.synthesize_images.get_logger"),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
            patch("datasmith.runners.synthesize_images.get_client", return_value=mock_client),
            _MOCK_PREREQS,
            _MOCK_BUILD,
            _MOCK_PUSH,
            _MOCK_REPO_IMAGE,
            _MOCK_OVERRIDES,
            patch(
                "datasmith.github.render.render_problem_statement",
                return_value="Rendered problem text",
            ) as mock_render,
        ):
            runner = SynthesizeImagesRunner(
                synthesizer=synthesizer,
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

        with (
            patch("datasmith.runners.synthesize_images.get_logger"),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
            patch("datasmith.runners.synthesize_images.get_client", return_value=mock_client),
            _MOCK_PREREQS,
            _MOCK_BUILD,
            _MOCK_PUSH,
            _MOCK_REPO_IMAGE,
            _MOCK_OVERRIDES,
            patch(
                "datasmith.github.render.render_problem_statement",
            ) as mock_render,
        ):
            runner = SynthesizeImagesRunner(
                synthesizer=synthesizer,
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
            _MOCK_BUILD,
            _MOCK_PUSH,
            _MOCK_REPO_IMAGE,
            _MOCK_OVERRIDES,
            patch(
                "datasmith.github.render.render_problem_statement",
                return_value="Rendered with issues",
            ) as mock_render,
        ):
            runner = SynthesizeImagesRunner(
                synthesizer=synthesizer,
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


class TestTheRowReachesTheBuilder:
    """A row's ``python_version`` and ``primary_root`` must arrive at the image.

    Stage 4 discovers both and the bug both tasks exist to fix is that neither
    survived the trip to the builder. Pinning the name helper and the
    ``ImageManager`` leaves the middle of that trip deletable with a green
    suite, so these tests walk the item dict the pipeline actually builds.
    """

    def _item(self) -> dict[str, object]:
        item = _make_item(owner="Qiskit", repo="qiskit", issue=99)
        item["sha"] = "deadbeef"
        item["python_version"] = "3.12"
        item["primary_root"] = "qiskit_pkg"
        return item

    async def test_the_interpreter_and_the_root_reach_the_prereq_call(self) -> None:
        synthesizer = MagicMock()
        synthesizer.run.return_value = None  # stop before any DB write

        with patch.object(SynthesizeImagesRunner, "_ensure_prereqs") as prereqs:
            runner = SynthesizeImagesRunner(synthesizer=synthesizer, n_concurrent=1)
            with pytest.raises(RuntimeError):
                await runner._do_process_item(self._item())

        prereqs.assert_called_once_with("Qiskit", "qiskit", "3.12", "qiskit_pkg")

    async def test_the_synthesizer_is_handed_the_image_that_carries_the_root(self) -> None:
        from datasmith.docker.images import get_repo_image_name

        synthesizer = MagicMock()
        synthesizer.run.return_value = None

        with patch.object(SynthesizeImagesRunner, "_ensure_prereqs"):
            runner = SynthesizeImagesRunner(synthesizer=synthesizer, n_concurrent=1)
            with pytest.raises(RuntimeError):
                await runner._do_process_item(self._item())

        expected = get_repo_image_name("Qiskit", "qiskit", "3.12", "qiskit_pkg")
        assert synthesizer.run.call_args.kwargs["repo_image"] == expected

    async def test_the_build_and_the_push_name_the_same_parent(self) -> None:
        mock_client = _mock_supabase()
        synthesizer = MagicMock()
        synthesizer.run.return_value = MagicMock()

        with (
            patch("datasmith.runners.base.get_client", return_value=mock_client),
            patch("datasmith.runners.synthesize_images.get_client", return_value=mock_client),
            patch.object(SynthesizeImagesRunner, "_ensure_prereqs"),
            patch.object(SynthesizeImagesRunner, "_enqueue_neighbors"),
            patch(
                "datasmith.runners.synthesize_images._build_pr_image",
                return_value="formulacode/qiskit-qiskit:99",
            ) as build,
            patch("datasmith.runners.synthesize_images._push_pr_image") as push,
        ):
            runner = SynthesizeImagesRunner(synthesizer=synthesizer, n_concurrent=1)
            await runner._do_process_item(self._item())

        assert build.call_args.args[6] == "3.12"
        assert build.call_args.kwargs["build_root"] == "qiskit_pkg"
        # The push resolves the repo parent from these two arguments; a parent
        # named differently from the one just built is pushed as a warning.
        assert push.call_args.args == ("Qiskit", "qiskit", "formulacode/qiskit-qiskit:99", "3.12", "qiskit_pkg")


class TestOneImagePerRootAndInterpreter:
    """Dedup must skip only the work already done for *this* row's image."""

    def test_two_roots_of_one_repository_build_two_images(self) -> None:
        with patch("datasmith.runners.synthesize_images._ensure_prerequisite_images") as build:
            SynthesizeImagesRunner._ensure_prereqs("Qiskit", "qiskit", "3.12", ".")
            SynthesizeImagesRunner._ensure_prereqs("Qiskit", "qiskit", "3.12", "qiskit_pkg")

        assert build.call_count == 2

    def test_two_interpreters_of_one_repository_build_two_images(self) -> None:
        with patch("datasmith.runners.synthesize_images._ensure_prerequisite_images") as build:
            SynthesizeImagesRunner._ensure_prereqs("Qiskit", "qiskit", "3.11", "qiskit_pkg")
            SynthesizeImagesRunner._ensure_prereqs("Qiskit", "qiskit", "3.12", "qiskit_pkg")

        assert build.call_count == 2

    def test_the_same_image_is_built_once(self) -> None:
        with patch("datasmith.runners.synthesize_images._ensure_prerequisite_images") as build:
            SynthesizeImagesRunner._ensure_prereqs("apache", "arrow", "3.11", "python")
            SynthesizeImagesRunner._ensure_prereqs("apache", "arrow", "3.11", "./python")

        assert build.call_count == 1


class TestPatchSelection:
    def test_patch_is_in_the_pull_requests_select(self):
        from pathlib import Path

        src = (Path(__file__).parents[2] / "src" / "datasmith" / "runners" / "synthesize_images.py").read_text()
        select_lines = [ln for ln in src.splitlines() if "merge_commit_sha, base_sha" in ln]
        assert select_lines, "could not find the pull_requests select"
        assert any("patch" in ln for ln in select_lines), "select does not fetch `patch`"

    def test_pipeline_select_agrees(self):
        from pathlib import Path

        src = (Path(__file__).parents[2] / "src" / "datasmith" / "update" / "pipeline.py").read_text()
        select_lines = [ln for ln in src.splitlines() if "merge_commit_sha, base_sha" in ln]
        assert select_lines, "could not find the pull_requests select in pipeline.py"
        assert any("patch" in ln for ln in select_lines), "pipeline select does not fetch `patch`"
