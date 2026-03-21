"""Tests for datasmith.agents.sandbox."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from datasmith.agents.sandbox import (
    SandboxConfig,
    SandboxResult,
    SandboxRunner,
    _generate_task_txt,
    _render_agents_md,
)
from datasmith.docker.context import DockerContext


def _make_base_context() -> DockerContext:
    return DockerContext(
        dockerfile="FROM ubuntu:22.04",
        build_base_sh="#!/bin/bash\necho base",
        build_env_sh="#!/bin/bash\necho env",
        build_pkg_sh="#!/bin/bash\necho pkg",
        build_run_sh="#!/bin/bash\necho run",
        build_final_sh="#!/bin/bash\necho final",
        profile_sh="#!/bin/bash\necho profile",
        run_tests_sh="#!/bin/bash\necho tests",
        entrypoint_sh="#!/bin/bash\necho entry",
    )


class TestSandboxConfig:
    def test_defaults(self) -> None:
        cfg = SandboxConfig()
        assert cfg.timeout_s == 1800
        assert cfg.codex_timeout_s == 1800
        assert cfg.skip_tests is False

    def test_custom(self) -> None:
        cfg = SandboxConfig(timeout_s=600, codex_timeout_s=300, skip_tests=True)
        assert cfg.timeout_s == 600
        assert cfg.codex_timeout_s == 300
        assert cfg.skip_tests is True


class TestSandboxResult:
    def test_defaults(self) -> None:
        r = SandboxResult(success=False)
        assert r.success is False
        assert r.docker_context is None
        assert r.failure_json is None
        assert r.duration_s == 0.0
        assert r.agent_output == ""


class TestGenerateTaskTxt:
    def test_basic(self) -> None:
        txt = _generate_task_txt("pandas-dev", "pandas", "abc123", '{"deps": []}', "3.10")
        assert "owner='pandas-dev'" in txt
        assert "repo='pandas'" in txt
        assert "sha='abc123'" in txt
        assert "python_version='3.10'" in txt
        assert '{"deps": []}' in txt

    def test_special_chars_in_env_payload(self) -> None:
        payload = '{"dependencies": ["numpy>=1.21", "scipy"]}'
        txt = _generate_task_txt("owner", "repo", "sha", payload, "3.11")
        assert "numpy>=1.21" in txt


class TestRenderAgentsMd:
    def test_renders_template(self) -> None:
        md = _render_agents_md(
            owner="pandas-dev",
            repo="pandas",
            sha="abc123def456",
            python_version="3.10",
            pr_context="This PR optimizes groupby performance.",
        )
        assert "pandas-dev/pandas" in md
        assert "abc123def456" in md
        assert "3.10" in md
        assert "This PR optimizes groupby performance." in md
        assert "sandbox_verify.py" in md
        assert "docker_build_pkg.sh" in md

    def test_pr_context_multiline(self) -> None:
        pr_context = "Line 1\nLine 2\n\nLine 4"
        md = _render_agents_md("o", "r", "s", "3.10", pr_context)
        assert "Line 1\nLine 2" in md


class TestPrepareWorkspace:
    def test_workspace_structure(self, tmp_path: Path) -> None:
        runner = SandboxRunner()
        ctx = _make_base_context()

        runner._prepare_workspace(
            workspace=tmp_path,
            owner="pandas-dev",
            repo="pandas",
            sha="abc123",
            base_context=ctx,
            env_payload='{"deps": []}',
            python_version="3.10",
            pr_context="fix groupby",
        )

        # Check task directory has all 9 context files + task.txt
        task_dir = tmp_path / "task"
        assert task_dir.is_dir()
        assert (task_dir / "Dockerfile").exists()
        assert (task_dir / "docker_build_base.sh").exists()
        assert (task_dir / "docker_build_env.sh").exists()
        assert (task_dir / "docker_build_pkg.sh").exists()
        assert (task_dir / "docker_build_run.sh").exists()
        assert (task_dir / "docker_build_final.sh").exists()
        assert (task_dir / "profile.sh").exists()
        assert (task_dir / "run-tests.sh").exists()
        assert (task_dir / "entrypoint.sh").exists()
        assert (task_dir / "task.txt").exists()

        # Check task.txt content
        task_txt = (task_dir / "task.txt").read_text()
        assert "pandas-dev" in task_txt
        assert "abc123" in task_txt

        # Check AGENTS.md at workspace root
        agents_md = tmp_path / "AGENTS.md"
        assert agents_md.exists()
        assert "pandas-dev/pandas" in agents_md.read_text()

        # Check sandbox_verify.py at workspace root
        assert (tmp_path / "sandbox_verify.py").exists()
        verify_content = (tmp_path / "sandbox_verify.py").read_text()
        assert "def verify(" in verify_content

    def test_context_file_contents(self, tmp_path: Path) -> None:
        ctx = _make_base_context()
        runner = SandboxRunner()

        runner._prepare_workspace(
            workspace=tmp_path,
            owner="o",
            repo="r",
            sha="s",
            base_context=ctx,
            env_payload="",
            python_version="3.10",
            pr_context="",
        )

        assert (tmp_path / "task" / "Dockerfile").read_text() == "FROM ubuntu:22.04"
        assert (tmp_path / "task" / "docker_build_pkg.sh").read_text() == "#!/bin/bash\necho pkg"

        # profile.sh, run-tests.sh, entrypoint.sh are overridden with latest templates
        profile_content = (tmp_path / "task" / "profile.sh").read_text()
        assert profile_content != "#!/bin/bash\necho profile"  # not the base context value
        assert "ASV baseline" in profile_content  # from the real template


class TestInitGit:
    def test_creates_git_repo(self, tmp_path: Path) -> None:
        # Create a file so git has something to commit
        (tmp_path / "test.txt").write_text("hello")

        runner = SandboxRunner()
        runner._init_git(tmp_path)

        assert (tmp_path / ".git").is_dir()


class TestExtractResults:
    def test_success(self, tmp_path: Path) -> None:
        task_dir = tmp_path / "task"
        task_dir.mkdir()
        (task_dir / "verification_success.json").write_text('{"local_image": "test:latest"}')
        (task_dir / "docker_build_pkg.sh").write_text("#!/bin/bash\necho fixed")

        runner = SandboxRunner()
        codex_result = MagicMock()
        codex_result.output = "agent output"

        result = runner._extract_results(tmp_path, codex_result)

        assert result.success is True
        assert result.docker_context is not None
        assert result.docker_context.build_pkg_sh == "#!/bin/bash\necho fixed"
        assert result.agent_output == "agent output"

    def test_failure(self, tmp_path: Path) -> None:
        task_dir = tmp_path / "task"
        task_dir.mkdir()
        failure = {"stage": "build", "return_code": 1, "error_message": "missing dep"}
        (task_dir / "failure.json").write_text(json.dumps(failure))
        (task_dir / "docker_build_pkg.sh").write_text("#!/bin/bash\necho broken")

        runner = SandboxRunner()
        codex_result = MagicMock()
        codex_result.output = "agent output"

        result = runner._extract_results(tmp_path, codex_result)

        assert result.success is False
        assert result.docker_context is None
        assert result.failure_json is not None
        assert result.failure_json["stage"] == "build"

    def test_no_result_files(self, tmp_path: Path) -> None:
        task_dir = tmp_path / "task"
        task_dir.mkdir()

        runner = SandboxRunner()
        codex_result = MagicMock()
        codex_result.output = ""

        result = runner._extract_results(tmp_path, codex_result)

        assert result.success is False
        assert result.failure_json is None


class TestSandboxRunnerRun:
    @patch("datasmith.agents.sandbox.SandboxRunner._launch_agent")
    def test_dry_run(self, mock_launch: MagicMock) -> None:
        runner = SandboxRunner()
        ctx = _make_base_context()

        result = runner.run(
            owner="o",
            repo="r",
            sha="abc123",
            base_context=ctx,
            env_payload="{}",
            python_version="3.10",
            pr_context="test",
            dry_run=True,
        )

        assert result.success is True
        assert result.docker_context is ctx
        assert "[dry run" in result.agent_output
        mock_launch.assert_not_called()

    @patch("datasmith.agents.sandbox.SandboxRunner._launch_agent")
    @patch("datasmith.agents.sandbox.SandboxRunner._init_git")
    def test_full_run_success(self, mock_git: MagicMock, mock_launch: MagicMock, tmp_path: Path) -> None:
        """Mock a successful sandbox run end-to-end."""

        def fake_launch(workspace: Path) -> MagicMock:
            # Simulate the agent creating a success file
            task_dir = workspace / "task"
            (task_dir / "verification_success.json").write_text('{"local_image": "test:latest"}')
            result = MagicMock()
            result.output = "done"
            result.returncode = 0
            result.success = True
            return result

        mock_launch.side_effect = fake_launch

        runner = SandboxRunner()
        ctx = _make_base_context()

        result = runner.run(
            owner="o",
            repo="r",
            sha="abc123",
            base_context=ctx,
            env_payload="{}",
            python_version="3.10",
            pr_context="test",
        )

        assert result.success is True
        assert result.docker_context is not None
        assert result.duration_s > 0

    @patch("datasmith.agents.sandbox.SandboxRunner._launch_agent")
    @patch("datasmith.agents.sandbox.SandboxRunner._init_git")
    def test_full_run_failure(self, mock_git: MagicMock, mock_launch: MagicMock) -> None:
        """Mock a failed sandbox run end-to-end."""

        def fake_launch(workspace: Path) -> MagicMock:
            # Simulate the agent failing
            task_dir = workspace / "task"
            failure = {"stage": "build", "return_code": 1, "error_message": "error"}
            (task_dir / "failure.json").write_text(json.dumps(failure))
            result = MagicMock()
            result.output = "failed"
            result.success = False
            return result

        mock_launch.side_effect = fake_launch

        runner = SandboxRunner()
        ctx = _make_base_context()

        result = runner.run(
            owner="o",
            repo="r",
            sha="abc123",
            base_context=ctx,
            env_payload="{}",
            python_version="3.10",
            pr_context="test",
        )

        assert result.success is False
        assert result.failure_json is not None
        assert result.failure_json["stage"] == "build"
