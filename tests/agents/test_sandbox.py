"""Tests for datasmith.agents.sandbox."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from datasmith.agents.sandbox import (
    SandboxConfig,
    SandboxResult,
    SandboxRunner,
    _compute_immutable_hashes,
    _generate_task_txt,
    _render_agents_md,
)


class TestSandboxConfig:
    def test_defaults(self) -> None:
        cfg = SandboxConfig()
        assert cfg.timeout_s == 3600
        assert cfg.codex_timeout_s == 3600

    def test_custom(self) -> None:
        cfg = SandboxConfig(timeout_s=600, codex_timeout_s=300)
        assert cfg.timeout_s == 600
        assert cfg.codex_timeout_s == 300


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
        txt = _generate_task_txt(
            "pandas-dev", "pandas", "abc123", '{"deps": []}', "3.10", "formulacode/pandas-dev-pandas:latest"
        )
        assert "owner='pandas-dev'" in txt
        assert "repo='pandas'" in txt
        assert "sha='abc123'" in txt
        assert "python_version='3.10'" in txt
        assert '{"deps": []}' in txt
        assert "repo_image='formulacode/pandas-dev-pandas:latest'" in txt

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

    def test_no_skip_tests_mentioned(self) -> None:
        md = _render_agents_md("o", "r", "s", "3.10", "context")
        assert "--skip-tests" not in md

    def test_pr_context_multiline(self) -> None:
        pr_context = "Line 1\nLine 2\n\nLine 4"
        md = _render_agents_md("o", "r", "s", "3.10", pr_context)
        assert "Line 1\nLine 2" in md


class TestPrepareWorkspace:
    def test_workspace_structure(self, tmp_path: Path) -> None:
        runner = SandboxRunner()

        runner._prepare_workspace(
            workspace=tmp_path,
            owner="pandas-dev",
            repo="pandas",
            sha="abc123",
            repo_image="formulacode/pandas-dev-pandas:latest",
            env_payload='{"deps": []}',
            python_version="3.10",
            pr_context="fix groupby",
        )

        # Check task directory has template files + task.txt
        task_dir = tmp_path / "task"
        assert task_dir.is_dir()
        assert (task_dir / "Dockerfile.pr").exists()
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
        assert "repo_image='formulacode/pandas-dev-pandas:latest'" in task_txt

        # Check AGENTS.md at workspace root
        agents_md = tmp_path / "AGENTS.md"
        assert agents_md.exists()
        assert "pandas-dev/pandas" in agents_md.read_text()

        # Check sandbox_verify.py at workspace root
        assert (tmp_path / "sandbox_verify.py").exists()
        verify_content = (tmp_path / "sandbox_verify.py").read_text()
        assert "def verify(" in verify_content

    def test_immutable_hashes_written(self, tmp_path: Path) -> None:
        runner = SandboxRunner()

        runner._prepare_workspace(
            workspace=tmp_path,
            owner="o",
            repo="r",
            sha="s",
            repo_image="formulacode/o-r:latest",
            env_payload="",
            python_version="3.10",
            pr_context="",
        )

        # .immutable_hashes.json should be at workspace root
        hashes_file = tmp_path / ".immutable_hashes.json"
        assert hashes_file.exists()

        hashes = json.loads(hashes_file.read_text())
        task_dir = tmp_path / "task"

        # All immutable files should have hashes
        for fname in (
            "Dockerfile.pr",
            "docker_build_base.sh",
            "docker_build_env.sh",
            "docker_build_final.sh",
            "profile.sh",
            "run-tests.sh",
            "entrypoint.sh",
            "task.txt",
        ):
            assert fname in hashes, f"Missing hash for {fname}"
            expected = hashlib.md5((task_dir / fname).read_bytes()).hexdigest()  # noqa: S324
            assert hashes[fname] == expected

        # Editable files should NOT be in hashes
        assert "docker_build_pkg.sh" not in hashes
        assert "docker_build_run.sh" not in hashes

    def test_all_files_from_templates(self, tmp_path: Path) -> None:
        runner = SandboxRunner()

        runner._prepare_workspace(
            workspace=tmp_path,
            owner="o",
            repo="r",
            sha="s",
            repo_image="formulacode/o-r:latest",
            env_payload="",
            python_version="3.10",
            pr_context="",
        )

        # All files come from templates, so they should have real template content
        profile_content = (tmp_path / "task" / "profile.sh").read_text()
        assert "ASV baseline" in profile_content  # from the real template

        # Dockerfile.pr should exist (not Dockerfile)
        assert (tmp_path / "task" / "Dockerfile.pr").exists()
        assert not (tmp_path / "task" / "Dockerfile").exists()


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
        (task_dir / "docker_build_run.sh").write_text("#!/bin/bash\necho run")

        runner = SandboxRunner()
        codex_result = MagicMock()
        codex_result.output = "agent output"

        result = runner._extract_results(tmp_path, codex_result)

        assert result.success is True
        assert result.docker_context is not None
        assert result.docker_context.build_pkg_sh == "#!/bin/bash\necho fixed"
        assert result.docker_context.build_run_sh == "#!/bin/bash\necho run"
        # Only pkg and run scripts are extracted — others should be empty
        assert result.docker_context.dockerfile == ""
        assert result.docker_context.build_base_sh == ""
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
        codex_result.error = ""

        result = runner._extract_results(tmp_path, codex_result)

        assert result.success is False
        assert result.failure_json is None

    def test_integrity_violation_blocks_success(self, tmp_path: Path) -> None:
        """Even with verification_success.json, tampered files cause failure."""
        task_dir = tmp_path / "task"
        task_dir.mkdir()

        # Write an immutable file and record its hash
        (task_dir / "Dockerfile.pr").write_text("FROM base")
        hashes = _compute_immutable_hashes(task_dir)
        (tmp_path / ".immutable_hashes.json").write_text(json.dumps(hashes))

        # Agent writes success file but also tampers with Dockerfile
        (task_dir / "verification_success.json").write_text('{"local_image": "test:latest"}')
        (task_dir / "Dockerfile.pr").write_text("FROM hacked")
        (task_dir / "docker_build_pkg.sh").write_text("#!/bin/bash\necho pkg")

        runner = SandboxRunner()
        codex_result = MagicMock()
        codex_result.output = "agent output"

        result = runner._extract_results(tmp_path, codex_result)

        assert result.success is False
        assert result.failure_json is not None
        assert result.failure_json["stage"] == "integrity"
        assert "Dockerfile.pr" in result.failure_json["error_message"]

    def test_integrity_ok_with_editable_changes(self, tmp_path: Path) -> None:
        """Editing docker_build_pkg.sh and docker_build_run.sh is allowed."""
        task_dir = tmp_path / "task"
        task_dir.mkdir()

        # Write immutable files and record hashes
        (task_dir / "Dockerfile.pr").write_text("FROM base")
        (task_dir / "docker_build_pkg.sh").write_text("#!/bin/bash\necho original")
        hashes = _compute_immutable_hashes(task_dir)
        (tmp_path / ".immutable_hashes.json").write_text(json.dumps(hashes))

        # Agent edits only allowed files and succeeds
        (task_dir / "docker_build_pkg.sh").write_text("#!/bin/bash\necho fixed")
        (task_dir / "verification_success.json").write_text('{"local_image": "test:latest"}')

        runner = SandboxRunner()
        codex_result = MagicMock()
        codex_result.output = "done"

        result = runner._extract_results(tmp_path, codex_result)

        assert result.success is True
        assert result.docker_context is not None


class TestSandboxRunnerRun:
    @patch("datasmith.agents.sandbox.SandboxRunner._launch_agent")
    def test_dry_run(self, mock_launch: MagicMock) -> None:
        runner = SandboxRunner()

        result = runner.run(
            owner="o",
            repo="r",
            sha="abc123",
            repo_image="formulacode/o-r:latest",
            env_payload="{}",
            python_version="3.10",
            pr_context="test",
            dry_run=True,
        )

        assert result.success is True
        assert result.docker_context is not None
        assert "[dry run" in result.agent_output
        mock_launch.assert_not_called()

    @patch("datasmith.agents.sandbox.SandboxRunner._launch_agent")
    @patch("datasmith.agents.sandbox.SandboxRunner._init_git")
    def test_full_run_success(self, mock_git: MagicMock, mock_launch: MagicMock, tmp_path: Path) -> None:
        """Mock a successful sandbox run end-to-end."""

        def fake_launch(workspace: Path) -> tuple[str, MagicMock]:
            # Simulate the agent creating a success file
            task_dir = workspace / "task"
            (task_dir / "verification_success.json").write_text('{"local_image": "test:latest"}')
            result = MagicMock()
            result.output = "done"
            result.raw_output = ""
            result.returncode = 0
            result.success = True
            result.files_changed = []
            return "claude", result

        mock_launch.side_effect = fake_launch

        runner = SandboxRunner()

        result = runner.run(
            owner="o",
            repo="r",
            sha="abc123",
            repo_image="formulacode/o-r:latest",
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

        def fake_launch(workspace: Path) -> tuple[str, MagicMock]:
            # Simulate the agent failing
            task_dir = workspace / "task"
            failure = {"stage": "build", "return_code": 1, "error_message": "error"}
            (task_dir / "failure.json").write_text(json.dumps(failure))
            result = MagicMock()
            result.output = "failed"
            result.raw_output = ""
            result.success = False
            result.files_changed = []
            return "claude", result

        mock_launch.side_effect = fake_launch

        runner = SandboxRunner()

        result = runner.run(
            owner="o",
            repo="r",
            sha="abc123",
            repo_image="formulacode/o-r:latest",
            env_payload="{}",
            python_version="3.10",
            pr_context="test",
        )

        assert result.success is False
        assert result.failure_json is not None
        assert result.failure_json["stage"] == "build"
