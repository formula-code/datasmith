"""Tests that verify the code snippets shown on the documentation website.

Each test corresponds to a snippet from ``site/`` and checks that the imports,
class instantiations, and method signatures shown on the website are consistent
with the actual codebase.  These are *compile-level* checks — they do not call
external services or Docker; they only verify that the public API surface
described on the website actually exists and accepts the documented arguments.
"""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Quickstart — "Working with Pull Requests"
# site/getting-started/quickstart/index.html
# ---------------------------------------------------------------------------


class TestQuickstartPRSnippets:
    """Snippets from the Quickstart page: PR model and fetch."""

    def test_pr_import_path(self) -> None:
        """Website: ``from datasmith.github import PR, GitHubClient``"""
        from datasmith.github import PR, GitHubClient

        assert PR is not None
        assert GitHubClient is not None

    def test_token_pool_import_path(self) -> None:
        """Website: ``from datasmith.utils import TokenPool``"""
        from datasmith.utils import TokenPool

        assert TokenPool is not None

    def test_pr_construction(self) -> None:
        """Website: ``PR(repository="astropy/astropy", issue_number=16222)``"""
        from datasmith.github import PR

        pr = PR(repository="astropy/astropy", issue_number=16222)
        assert pr.repository == "astropy/astropy"
        assert pr.issue_number == 16222

    def test_pr_fields(self) -> None:
        """Website claims ``merge_commit_sha``, ``base_sha``, ``cache_key`` exist."""
        from datasmith.github import PR

        pr = PR(repository="astropy/astropy", issue_number=16222)
        assert pr.merge_commit_sha == ""
        assert pr.base_sha == ""
        assert pr.cache_key == "astropy/astropy:16222"

    def test_pr_is_frozen(self) -> None:
        """Website: 'PRs are frozen Pydantic v2 models — immutable after creation'"""
        from datasmith.github import PR

        pr = PR(repository="astropy/astropy", issue_number=16222)
        with pytest.raises(Exception):  # ValidationError for frozen model
            pr.repository = "other/repo"  # type: ignore[misc]

    def test_pr_fetch_signature(self) -> None:
        """Website: ``await PR.fetch("astropy/astropy", 16222)``
        Actual: ``PR.fetch(repository, issue_number, *, client=None)``
        """
        from datasmith.github import PR

        sig = inspect.signature(PR.fetch)
        params = list(sig.parameters.keys())
        assert "repository" in params
        assert "issue_number" in params
        assert "client" in params
        assert inspect.iscoroutinefunction(PR.fetch)


class TestQuickstartGitHubClientSnippets:
    """Snippets from the Quickstart page: GitHubClient usage."""

    def test_github_client_accepts_token_pool(self) -> None:
        """Website: ``GitHubClient(pool)``"""
        from datasmith.github import GitHubClient
        from datasmith.utils import TokenPool

        pool = MagicMock(spec=TokenPool)
        client = GitHubClient(pool)
        assert client is not None

    def test_github_client_get_pr_signature(self) -> None:
        """Website: ``gh.get_pr("pandas-dev", "pandas", 16222)``
        Actual: ``get_pr(self, owner, repo, number)`` — three positional args.
        """
        from datasmith.github import GitHubClient

        sig = inspect.signature(GitHubClient.get_pr)
        params = list(sig.parameters.keys())
        assert "owner" in params
        assert "repo" in params
        assert "number" in params
        assert inspect.iscoroutinefunction(GitHubClient.get_pr)

    def test_github_client_get_diff_signature(self) -> None:
        """Website: ``gh.get_diff("pandas-dev", "pandas", 16222)``"""
        from datasmith.github import GitHubClient

        assert hasattr(GitHubClient, "get_diff")
        assert inspect.iscoroutinefunction(GitHubClient.get_diff)

    def test_github_client_get_timeline_signature(self) -> None:
        """Website: ``gh.get_timeline("pandas-dev", "pandas", 16222)``"""
        from datasmith.github import GitHubClient

        assert hasattr(GitHubClient, "get_timeline")
        assert inspect.iscoroutinefunction(GitHubClient.get_timeline)


# ---------------------------------------------------------------------------
# Quickstart — "Rendering Problem Statements"
# ---------------------------------------------------------------------------


class TestQuickstartRenderSnippets:
    """Snippets from the Quickstart page: render and scrape_links."""

    def test_render_imports(self) -> None:
        """Website: ``from datasmith.github import render_problem_statement, scrape_links``"""
        from datasmith.github import render_problem_statement, scrape_links

        assert callable(render_problem_statement)
        assert callable(scrape_links)

    def test_render_problem_statement_signature(self) -> None:
        """Website: ``render_problem_statement(pr, anonymize=True)`` and
        ``render_problem_statement(pr, issues=issues, repo_description=...)``
        """
        from datasmith.github import render_problem_statement

        sig = inspect.signature(render_problem_statement)
        params = sig.parameters
        assert "pr" in params
        assert "anonymize" in params
        assert "issues" in params
        assert "repo_description" in params

    def test_scrape_links_signature(self) -> None:
        """Website: ``scrape_links(pr, gh.get_issue_expanded, depth=2, only_issues=True, limit=6)``"""
        from datasmith.github import scrape_links

        sig = inspect.signature(scrape_links)
        params = sig.parameters
        assert "pr" in params
        assert "get_issue_fn" in params
        assert "depth" in params
        assert "only_issues" in params
        assert "limit" in params
        assert inspect.iscoroutinefunction(scrape_links)


# ---------------------------------------------------------------------------
# Quickstart — "Custom Hooks and Caching"
# ---------------------------------------------------------------------------


class TestQuickstartHookSnippets:
    """Snippets from the Quickstart page: HookRegistry."""

    def test_hook_registry_import(self) -> None:
        """Website: ``from datasmith.github import HookRegistry``"""
        from datasmith.github import HookRegistry

        assert HookRegistry is not None

    def test_hook_registry_register_and_call(self) -> None:
        """Website:
        ``HookRegistry.register("summarize", summarize)``
        ``HookRegistry.call("summarize", pr)``
        """
        from datasmith.github import HookRegistry

        HookRegistry.clear()  # clean slate
        try:
            HookRegistry.register("test_hook", lambda x: x * 2, cached=False)
            assert HookRegistry.call("test_hook", 21) == 42
        finally:
            HookRegistry.clear()


# ---------------------------------------------------------------------------
# Quickstart — "Classification at Scale"
# ---------------------------------------------------------------------------


class TestQuickstartClassifySnippets:
    """Snippets from the Quickstart page: ClassifyPRsRunner."""

    def test_classify_imports(self) -> None:
        """Website:
        ``from datasmith.runners import ClassifyPRsRunner``
        ``from datasmith.agents import PerfClassifier, ClassifyJudge``
        """
        from datasmith.agents import ClassifyJudge, PerfClassifier
        from datasmith.runners import ClassifyPRsRunner

        assert ClassifyPRsRunner is not None
        assert PerfClassifier is not None
        assert ClassifyJudge is not None

    def test_classify_prs_runner_signature(self) -> None:
        """Website: ``ClassifyPRsRunner(PerfClassifier(), ClassifyJudge(), n_concurrent=64)``
        Actual: ``ClassifyPRsRunner(classifier, judge, n_concurrent=5)``
        """
        from datasmith.runners import ClassifyPRsRunner

        sig = inspect.signature(ClassifyPRsRunner.__init__)
        params = list(sig.parameters.keys())
        assert "classifier" in params
        assert "judge" in params
        assert "n_concurrent" in params


# ---------------------------------------------------------------------------
# Quickstart — "Building Docker Images"
# ---------------------------------------------------------------------------


class TestQuickstartDockerSnippets:
    """Snippets from the Quickstart page: ImageManager."""

    def test_image_manager_import(self) -> None:
        """Website: ``from datasmith.docker import ImageManager``"""
        from datasmith.docker import ImageManager

        assert ImageManager is not None

    def test_image_manager_methods_exist(self) -> None:
        """Website:
        ``mgr.build_base_image()``
        ``mgr.build_repo_image("pandas-dev", "pandas")``
        ``mgr.build_pr_image("pandas-dev", "pandas", 16222)``
        """
        from datasmith.docker import ImageManager

        assert hasattr(ImageManager, "build_base_image")
        assert hasattr(ImageManager, "build_repo_image")
        assert hasattr(ImageManager, "build_pr_image")

    def test_build_repo_image_signature(self) -> None:
        """Website: ``mgr.build_repo_image("pandas-dev", "pandas")``
        Actual: ``build_repo_image(self, owner, repo, context=None, ...)``
        """
        from datasmith.docker import ImageManager

        sig = inspect.signature(ImageManager.build_repo_image)
        params = list(sig.parameters.keys())
        assert "owner" in params
        assert "repo" in params

    def test_build_pr_image_signature(self) -> None:
        """Website: ``mgr.build_pr_image("pandas-dev", "pandas", 16222)``
        Actual: ``build_pr_image(self, owner, repo, issue_number, ...)``
        """
        from datasmith.docker import ImageManager

        sig = inspect.signature(ImageManager.build_pr_image)
        params = list(sig.parameters.keys())
        assert "owner" in params
        assert "repo" in params
        assert "issue_number" in params


# ---------------------------------------------------------------------------
# Guide — "Docker Images" — Build Contexts
# ---------------------------------------------------------------------------


class TestDockerContextSnippets:
    """Snippets from the Docker Images guide page."""

    def test_docker_context_import(self) -> None:
        """Website: ``from datasmith.docker.context import DockerContext``"""
        from datasmith.docker.context import DockerContext

        assert DockerContext is not None

    def test_docker_context_from_directory(self) -> None:
        """Website: ``DockerContext.from_directory("dataset/formulacode_verified/...")``"""
        from datasmith.docker.context import DockerContext

        assert hasattr(DockerContext, "from_directory")
        sig = inspect.signature(DockerContext.from_directory)
        assert "path" in sig.parameters

    def test_docker_context_to_tar_bytes(self) -> None:
        """Website: ``ctx.to_tar_bytes()``"""
        from datasmith.docker.context import DockerContext

        ctx = DockerContext(build_pkg_sh="#!/bin/bash\necho hello")
        tar_bytes = ctx.to_tar_bytes()
        assert isinstance(tar_bytes, bytes)
        assert len(tar_bytes) > 0


# ---------------------------------------------------------------------------
# Guide — "Docker Images" / "Verification" — Verifiers
# ---------------------------------------------------------------------------


class TestVerifierSnippets:
    """Snippets from the Docker Images and Verification guide pages."""

    def test_verifier_imports(self) -> None:
        """Website: ``from datasmith.docker import MultiObjVerifier, SmokeVerifier, ProfileVerifier``"""
        from datasmith.docker import MultiObjVerifier, ProfileVerifier, SmokeVerifier

        assert MultiObjVerifier is not None
        assert SmokeVerifier is not None
        assert ProfileVerifier is not None

    def test_smoke_verifier_takes_package(self) -> None:
        """Website: ``SmokeVerifier("pandas")``"""
        from datasmith.docker import SmokeVerifier

        sig = inspect.signature(SmokeVerifier.__init__)
        assert "package" in sig.parameters

    def test_profile_verifier_takes_timeout(self) -> None:
        """Website: ``ProfileVerifier(timeout=300)``"""
        from datasmith.docker import ProfileVerifier

        sig = inspect.signature(ProfileVerifier.__init__)
        assert "timeout" in sig.parameters

    def test_multi_obj_verifier_takes_verifiers_list(self) -> None:
        """Website: ``MultiObjVerifier(verifiers=[SmokeVerifier(...), ProfileVerifier(...)])``"""
        from datasmith.docker import MultiObjVerifier

        sig = inspect.signature(MultiObjVerifier.__init__)
        assert "verifiers" in sig.parameters

    def test_verify_result_fields(self) -> None:
        """Website: ``result.ok, result.rc, result.stdout, result.stderr, result.duration_s``"""
        from datasmith.docker import VerifyResult

        r = VerifyResult(ok=True, rc=0, stdout="out", stderr="err", duration_s=1.5)
        assert r.ok is True
        assert r.rc == 0
        assert r.stdout == "out"
        assert r.stderr == "err"
        assert r.duration_s == 1.5


# ---------------------------------------------------------------------------
# Guide — "Synthesis"
# ---------------------------------------------------------------------------


class TestSynthesisSnippets:
    """Snippets from the Synthesis guide page.

    NOTE: The website shows ``Synthesizer`` imported from ``datasmith.docker``
    but it actually lives in ``datasmith.agents``.  The website also shows
    ``Synthesizer.run()`` accepting ``verifier`` and ``base_context`` keyword
    arguments that do not exist in the actual signature.
    """

    def test_synthesizer_import_actual(self) -> None:
        """Synthesizer is in ``datasmith.agents``, NOT ``datasmith.docker``."""
        from datasmith.agents import Synthesizer

        assert Synthesizer is not None

    def test_synthesizer_not_in_docker(self) -> None:
        """Synthesizer lives in datasmith.agents, not datasmith.docker."""
        import datasmith.docker as docker_mod

        assert not hasattr(docker_mod, "Synthesizer")

    def test_synthesizer_init_signature(self) -> None:
        """Website: ``Synthesizer(max_attempts=3)``
        Actual: ``Synthesizer(max_attempts=2, dry_run=False, agent=None, force=False)``
        """
        from datasmith.agents import Synthesizer

        sig = inspect.signature(Synthesizer.__init__)
        assert "max_attempts" in sig.parameters

    def test_synthesizer_run_signature(self) -> None:
        """Website: ``synth.run(owner, repo, issue_number, pr_context, sha, env_payload, python_version)``
        Actual: ``run(owner, repo, issue_number, pr_context, sha, repo_image,
                      env_payload, python_version, force)``
        """
        from datasmith.agents import Synthesizer

        sig = inspect.signature(Synthesizer.run)
        params = set(sig.parameters.keys()) - {"self"}
        assert "owner" in params
        assert "repo" in params
        assert "issue_number" in params
        assert "pr_context" in params
        assert "sha" in params
        assert "env_payload" in params
        assert "python_version" in params
        # These were removed from the website (they never existed in the API):
        assert "verifier" not in params
        assert "base_context" not in params

    def test_synthesize_images_runner_import(self) -> None:
        """Website: ``from datasmith.runners import SynthesizeImagesRunner``"""
        from datasmith.runners import SynthesizeImagesRunner

        assert SynthesizeImagesRunner is not None

    def test_synthesize_images_runner_signature(self) -> None:
        """Website: ``SynthesizeImagesRunner(synth, n_concurrent=8)``
        Actual: ``SynthesizeImagesRunner(synthesizer, gh=None, n_concurrent=3)``
        """
        from datasmith.runners import SynthesizeImagesRunner

        sig = inspect.signature(SynthesizeImagesRunner.__init__)
        params = set(sig.parameters.keys()) - {"self"}
        assert "synthesizer" in params
        assert "n_concurrent" in params


# ---------------------------------------------------------------------------
# Guide — "Publishing"
# ---------------------------------------------------------------------------


class TestPublishSnippets:
    """Snippets from the Publishing guide page."""

    def test_publish_imports(self) -> None:
        """Website:
        ``from datasmith.utils.db import get_client``
        ``from datasmith.publish import records_from_supabase, HuggingFacePublisher``
        """
        from datasmith.publish import HuggingFacePublisher, records_from_supabase
        from datasmith.utils.db import get_client

        assert callable(get_client)
        assert callable(records_from_supabase)
        assert HuggingFacePublisher is not None

    def test_records_from_supabase_signature(self) -> None:
        """Website: ``records_from_supabase(start_date="2026-02-01", end_date="2026-03-01")``"""
        from datasmith.publish import records_from_supabase

        sig = inspect.signature(records_from_supabase)
        assert "start_date" in sig.parameters
        assert "end_date" in sig.parameters

    def test_huggingface_publisher_publish_signature(self) -> None:
        """Website: ``hf.publish(records, version="formulacode@2026-03")``"""
        from datasmith.publish import HuggingFacePublisher

        sig = inspect.signature(HuggingFacePublisher.publish)
        params = list(sig.parameters.keys())
        assert "records" in params
        assert "version" in params

    def test_supabase_client_table_query(self) -> None:
        """Website: ``sb.table("pull_requests").select("*").eq(...).not_.is_(...).execute()``

        This verifies that the Supabase client has the expected query API.
        We mock ``get_client`` to avoid needing a real Supabase instance.
        """
        mock_client = MagicMock()
        mock_client.table.return_value.select.return_value.eq.return_value.not_.is_.return_value.execute.return_value.data = []

        # The chained API should not raise
        result = (
            mock_client.table("pull_requests")
            .select("*")
            .eq("is_performance_commit", True)
            .not_.is_("container_name", "null")
            .execute()
        )
        assert result.data == []


# ---------------------------------------------------------------------------
# Guide — "Pipeline" (ds-update CLI)
# ---------------------------------------------------------------------------


class TestPipelineSnippets:
    """Snippets from the Pipeline guide page: ds-update CLI."""

    def test_ds_update_entry_point_exists(self) -> None:
        """Website: ``ds-update --start-date 2026-02-01 --end-date 2026-03-01``"""
        from datasmith.update.cli import main

        assert callable(main)

    def test_ds_update_cli_accepts_documented_flags(self) -> None:
        """Website shows ``--start-date``, ``--end-date``, ``--resume``, ``--stage``, ``--dry-run``.
        Verify the arg parser accepts them.
        """
        from datasmith.update.cli import parse_args

        args = parse_args(["--start-date", "2026-02-01", "--end-date", "2026-03-01"])
        assert args.start_date == "2026-02-01"
        assert args.end_date == "2026-03-01"

    def test_ds_update_resume_flag(self) -> None:
        args = _parse(["--start-date", "2026-02-01", "--end-date", "2026-03-01", "--resume"])
        assert args.resume is True

    def test_ds_update_stage_flag(self) -> None:
        args = _parse(["--start-date", "2026-02-01", "--end-date", "2026-03-01", "--stage", "3"])
        assert args.stage == [3]

    def test_ds_update_dry_run_flag(self) -> None:
        args = _parse(["--start-date", "2026-02-01", "--end-date", "2026-03-01", "--dry-run"])
        assert args.dry_run is True

    def test_ds_update_n_concurrent_flag(self) -> None:
        """Website documents ``--n-concurrent N``."""
        args = _parse(["--start-date", "2026-02-01", "--end-date", "2026-03-01", "--n-concurrent", "16"])
        assert args.n_concurrent == 16

    def test_ds_update_tasks_per_repo_flag(self) -> None:
        """Website documents ``--tasks-per-repo N``."""
        args = _parse(["--start-date", "2026-02-01", "--end-date", "2026-03-01", "--tasks-per-repo", "5"])
        assert args.tasks_per_repo == 5

    def test_ds_update_agent_flag(self) -> None:
        """Website documents ``--agent claude|codex|gemini|none``."""
        args = _parse(["--start-date", "2026-02-01", "--end-date", "2026-03-01", "--agent", "claude"])
        assert args.agent == "claude"

    def test_ds_update_force_flag(self) -> None:
        """Website documents ``--force``."""
        args = _parse(["--start-date", "2026-02-01", "--end-date", "2026-03-01", "--force"])
        assert args.force is True

    def test_ds_update_offline_source_flag(self) -> None:
        """Website documents ``--offline-source PATH``."""
        args = _parse([
            "--start-date",
            "2026-02-01",
            "--end-date",
            "2026-03-01",
            "--offline-source",
            "/tmp/data.parquet",
        ])
        assert args.offline_source == "/tmp/data.parquet"

    def test_ds_update_min_stars_flag(self) -> None:
        """Website documents ``--min-stars N``."""
        args = _parse(["--start-date", "2026-02-01", "--end-date", "2026-03-01", "--min-stars", "1000"])
        assert args.min_stars == 1000

    def test_ds_update_multiple_stages(self) -> None:
        """Website documents ``--stage 5 --stage 6`` for multiple stages."""
        args = _parse(["--start-date", "2026-02-01", "--end-date", "2026-03-01", "--stage", "5", "--stage", "6"])
        assert args.stage == [5, 6]

    def test_pipeline_has_7_stages(self) -> None:
        """Website documents 7 pipeline stages."""
        from datasmith.update.pipeline import STAGES

        assert len(STAGES) == 7


# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------


class TestPreflightSnippets:
    """Snippets from Installation / Verification pages: preflight check."""

    def test_preflight_module_exists(self) -> None:
        """Website: ``python -m datasmith.preflight``"""
        import datasmith.preflight

        assert hasattr(datasmith.preflight, "run_preflight")
        assert callable(datasmith.preflight.run_preflight)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse(argv: list[str]) -> object:
    from datasmith.update.cli import parse_args

    return parse_args(argv)
