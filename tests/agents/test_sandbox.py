"""Tests for datasmith.agents.sandbox."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from datasmith.agents.sandbox import (
    SandboxConfig,
    SandboxResult,
    SandboxRunner,
    _compute_immutable_hashes,
    _extract_build_manifest,
    _extract_resource_metrics,
    _generate_task_txt,
    _render_agents_md,
)


class TestSandboxConfig:
    def test_defaults(self) -> None:
        cfg = SandboxConfig()
        assert cfg.timeout_s == 14400
        assert cfg.codex_timeout_s == 14400

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
        assert r.resource_metrics == {}
        assert r.build_manifest is None
        assert r.env_payload_override is None

    def test_with_env_payload_override(self) -> None:
        override = '["numpy==1.21.0"]'
        r = SandboxResult(success=True, env_payload_override=override)
        assert r.env_payload_override == override


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
        assert "local_ci.py" in md
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
        assert (task_dir / "emit_manifest.py").exists()
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

        # Check local_ci.py at workspace root
        assert (tmp_path / "local_ci.py").exists()
        verify_content = (tmp_path / "local_ci.py").read_text()
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
            "docker_build_final.sh",
            "emit_manifest.py",
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
        assert "docker_build_env.sh" not in hashes

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


class TestExtractResourceMetrics:
    def test_from_success_file(self, tmp_path: Path) -> None:
        success = tmp_path / "verification_success.json"
        failure = tmp_path / "failure.json"
        metrics = {"build_duration_s": 12.5, "image_size_bytes": 500_000_000}
        success.write_text(json.dumps({"local_image": "t", "resource_metrics": metrics}))

        assert _extract_resource_metrics(success, failure, None) == metrics

    def test_from_failure_json(self, tmp_path: Path) -> None:
        success = tmp_path / "verification_success.json"
        failure = tmp_path / "failure.json"
        metrics = {"build_duration_s": 5.0}
        failure_data = {"stage": "build", "resource_metrics": metrics}

        assert _extract_resource_metrics(success, failure, failure_data) == metrics

    def test_empty_when_no_metrics(self, tmp_path: Path) -> None:
        success = tmp_path / "verification_success.json"
        failure = tmp_path / "failure.json"

        assert _extract_resource_metrics(success, failure, None) == {}

    def test_extract_results_includes_metrics(self, tmp_path: Path) -> None:
        """_extract_results populates resource_metrics from success JSON."""
        task_dir = tmp_path / "task"
        task_dir.mkdir()
        metrics = {"build_duration_s": 10.0, "peak_memory_bytes": 1_000_000}
        (task_dir / "verification_success.json").write_text(
            json.dumps({"local_image": "test:latest", "resource_metrics": metrics})
        )
        (task_dir / "docker_build_pkg.sh").write_text("#!/bin/bash")
        (task_dir / "docker_build_run.sh").write_text("#!/bin/bash")

        runner = SandboxRunner()
        codex_result = MagicMock()
        codex_result.output = "ok"
        result = runner._extract_results(tmp_path, codex_result)

        assert result.success is True
        assert result.resource_metrics == metrics

    def test_extract_results_metrics_from_failure(self, tmp_path: Path) -> None:
        """_extract_results populates resource_metrics from failure JSON."""
        task_dir = tmp_path / "task"
        task_dir.mkdir()
        metrics = {"build_duration_s": 3.0}
        failure = {"stage": "build", "return_code": 1, "error_message": "err", "resource_metrics": metrics}
        (task_dir / "failure.json").write_text(json.dumps(failure))

        runner = SandboxRunner()
        codex_result = MagicMock()
        codex_result.output = ""
        codex_result.error = ""
        result = runner._extract_results(tmp_path, codex_result)

        assert result.success is False
        assert result.resource_metrics == metrics


class TestExtractBuildManifest:
    """build_manifest rides inside resource_metrics — see local_ci.py's verify()."""

    def test_from_success_file(self, tmp_path: Path) -> None:
        success = tmp_path / "verification_success.json"
        failure = tmp_path / "failure.json"
        manifest = {"schema_version": 1, "build": {"discovered_n": 3}, "verify": {}}
        metrics = {"build_duration_s": 12.5, "build_manifest": manifest}
        success.write_text(json.dumps({"local_image": "t", "resource_metrics": metrics}))

        assert _extract_build_manifest(success, failure, None) == manifest

    def test_from_failure_json(self, tmp_path: Path) -> None:
        success = tmp_path / "verification_success.json"
        failure = tmp_path / "failure.json"
        manifest = {"schema_version": 1, "build": {}, "verify": {}}
        failure_data = {"stage": "tests", "resource_metrics": {"build_manifest": manifest}}

        assert _extract_build_manifest(success, failure, failure_data) == manifest

    def test_none_when_absent(self, tmp_path: Path) -> None:
        success = tmp_path / "verification_success.json"
        failure = tmp_path / "failure.json"
        success.write_text(json.dumps({"local_image": "t", "resource_metrics": {"build_duration_s": 1.0}}))

        assert _extract_build_manifest(success, failure, None) is None

    def test_none_when_not_a_dict(self, tmp_path: Path) -> None:
        """A malformed build_manifest (e.g. a string) is ignored rather than propagated."""
        success = tmp_path / "verification_success.json"
        failure = tmp_path / "failure.json"
        success.write_text(json.dumps({"local_image": "t", "resource_metrics": {"build_manifest": "not-a-dict"}}))

        assert _extract_build_manifest(success, failure, None) is None

    def test_extract_results_includes_build_manifest(self, tmp_path: Path) -> None:
        """_extract_results populates SandboxResult.build_manifest from success JSON."""
        task_dir = tmp_path / "task"
        task_dir.mkdir()
        manifest = {"schema_version": 1, "build": {"discovered_n": 5}, "verify": {"test_timed_out": False}}
        metrics = {"build_duration_s": 10.0, "build_manifest": manifest}
        (task_dir / "verification_success.json").write_text(
            json.dumps({"local_image": "test:latest", "resource_metrics": metrics})
        )
        (task_dir / "docker_build_pkg.sh").write_text("#!/bin/bash")
        (task_dir / "docker_build_run.sh").write_text("#!/bin/bash")

        runner = SandboxRunner()
        codex_result = MagicMock()
        codex_result.output = "ok"
        result = runner._extract_results(tmp_path, codex_result)

        assert result.success is True
        assert result.build_manifest == manifest


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


# ── docker_build_env.sh editability ────────────────────────────────


class TestEnvShEditability:
    """docker_build_env.sh should be editable by the agent, not immutable."""

    def test_env_sh_not_in_immutable_files(self) -> None:
        """docker_build_env.sh must NOT appear in _IMMUTABLE_FILES."""
        from datasmith.agents.sandbox import _IMMUTABLE_FILES

        assert "docker_build_env.sh" not in _IMMUTABLE_FILES

    def test_env_sh_not_in_verify_immutable_files(self) -> None:
        """docker_build_env.sh must NOT appear in local_ci.py's _IMMUTABLE_FILES."""
        verify_src = Path(__file__).parents[1] / ".." / "src" / "datasmith" / "agents" / "templates" / "local_ci.py"
        content = verify_src.resolve().read_text()
        # Parse the _IMMUTABLE_FILES tuple from the source
        import ast

        tree = ast.parse(content)
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "_IMMUTABLE_FILES":
                        immutable = ast.literal_eval(node.value)
                        assert "docker_build_env.sh" not in immutable, (
                            "docker_build_env.sh should not be in local_ci.py _IMMUTABLE_FILES"
                        )
                        return
        raise AssertionError("Could not find _IMMUTABLE_FILES in local_ci.py")

    def test_env_sh_not_in_immutable_hashes(self, tmp_path: Path) -> None:
        """_prepare_workspace should NOT hash docker_build_env.sh (it's editable)."""
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

        hashes = json.loads((tmp_path / ".immutable_hashes.json").read_text())
        assert "docker_build_env.sh" not in hashes
        # Editable files should not be hashed
        assert "docker_build_pkg.sh" not in hashes
        assert "docker_build_run.sh" not in hashes

    def test_integrity_allows_env_sh_edits(self, tmp_path: Path) -> None:
        """Editing docker_build_env.sh must NOT trigger an integrity violation."""
        task_dir = tmp_path / "task"
        task_dir.mkdir()

        # Write immutable files and the editable env script
        (task_dir / "Dockerfile.pr").write_text("FROM base")
        (task_dir / "docker_build_env.sh").write_text("#!/bin/bash\noriginal env")
        hashes = _compute_immutable_hashes(task_dir)
        (tmp_path / ".immutable_hashes.json").write_text(json.dumps(hashes))

        # Agent edits env script and succeeds
        (task_dir / "docker_build_env.sh").write_text("#!/bin/bash\npip install Cython\noriginal env")
        (task_dir / "docker_build_pkg.sh").write_text("#!/bin/bash\necho pkg")
        (task_dir / "docker_build_run.sh").write_text("#!/bin/bash\necho run")
        (task_dir / "verification_success.json").write_text('{"local_image": "test:latest"}')

        runner = SandboxRunner()
        codex_result = MagicMock()
        codex_result.output = "done"
        codex_result.raw_output = ""
        codex_result.files_changed = ["task/docker_build_env.sh"]

        result = runner._extract_results(tmp_path, codex_result)

        assert result.success is True
        assert result.docker_context is not None

    def test_extract_results_reads_build_env_sh(self, tmp_path: Path) -> None:
        """_extract_results must read back docker_build_env.sh into DockerContext.build_env_sh."""
        task_dir = tmp_path / "task"
        task_dir.mkdir()

        env_content = "#!/bin/bash\npip install Cython\n# modified env"
        (task_dir / "docker_build_env.sh").write_text(env_content)
        (task_dir / "docker_build_pkg.sh").write_text("#!/bin/bash\necho pkg")
        (task_dir / "docker_build_run.sh").write_text("#!/bin/bash\necho run")
        (task_dir / "verification_success.json").write_text('{"local_image": "test:latest"}')

        runner = SandboxRunner()
        codex_result = MagicMock()
        codex_result.output = "done"
        codex_result.raw_output = ""
        codex_result.files_changed = []

        result = runner._extract_results(tmp_path, codex_result)

        assert result.success is True
        assert result.docker_context is not None
        assert result.docker_context.build_env_sh == env_content

    def test_extract_results_env_sh_empty_when_missing(self, tmp_path: Path) -> None:
        """If docker_build_env.sh doesn't exist, build_env_sh should be empty."""
        task_dir = tmp_path / "task"
        task_dir.mkdir()

        (task_dir / "docker_build_pkg.sh").write_text("#!/bin/bash\necho pkg")
        (task_dir / "docker_build_run.sh").write_text("#!/bin/bash\necho run")
        (task_dir / "verification_success.json").write_text('{"local_image": "test:latest"}')

        runner = SandboxRunner()
        codex_result = MagicMock()
        codex_result.output = "done"
        codex_result.raw_output = ""
        codex_result.files_changed = []

        result = runner._extract_results(tmp_path, codex_result)

        assert result.success is True
        assert result.docker_context is not None
        assert result.docker_context.build_env_sh == ""

    def test_failed_result_does_not_return_context(self, tmp_path: Path) -> None:
        """On failure, docker_context should still be None even with env edits."""
        task_dir = tmp_path / "task"
        task_dir.mkdir()

        (task_dir / "docker_build_env.sh").write_text("#!/bin/bash\nmodified")
        (task_dir / "docker_build_pkg.sh").write_text("#!/bin/bash\necho pkg")
        failure = {"stage": "build", "return_code": 1, "error_message": "err"}
        (task_dir / "failure.json").write_text(json.dumps(failure))

        runner = SandboxRunner()
        codex_result = MagicMock()
        codex_result.output = ""
        codex_result.error = ""
        codex_result.raw_output = ""
        codex_result.files_changed = []

        result = runner._extract_results(tmp_path, codex_result)

        assert result.success is False
        assert result.docker_context is None


# ── env_payload override ───────────────────────────────────────────


class TestEnvPayloadOverride:
    """Agent can write env_payload_override.json to modify the package list."""

    def test_extract_results_reads_payload_override(self, tmp_path: Path) -> None:
        """_extract_results reads env_payload_override.json when present."""
        task_dir = tmp_path / "task"
        task_dir.mkdir()

        override = '["numpy==1.21.0", "scipy==1.7.0"]'
        (task_dir / "env_payload_override.json").write_text(override)
        (task_dir / "docker_build_pkg.sh").write_text("#!/bin/bash")
        (task_dir / "docker_build_run.sh").write_text("#!/bin/bash")
        (task_dir / "verification_success.json").write_text('{"local_image": "test:latest"}')

        runner = SandboxRunner()
        codex_result = MagicMock()
        codex_result.output = "done"
        codex_result.raw_output = ""
        codex_result.files_changed = []

        result = runner._extract_results(tmp_path, codex_result)

        assert result.success is True
        assert result.env_payload_override == override

    def test_extract_results_no_override_returns_none(self, tmp_path: Path) -> None:
        """When no override file exists, env_payload_override should be None."""
        task_dir = tmp_path / "task"
        task_dir.mkdir()

        (task_dir / "docker_build_pkg.sh").write_text("#!/bin/bash")
        (task_dir / "docker_build_run.sh").write_text("#!/bin/bash")
        (task_dir / "verification_success.json").write_text('{"local_image": "test:latest"}')

        runner = SandboxRunner()
        codex_result = MagicMock()
        codex_result.output = "done"
        codex_result.raw_output = ""
        codex_result.files_changed = []

        result = runner._extract_results(tmp_path, codex_result)

        assert result.success is True
        assert result.env_payload_override is None

    def test_override_not_returned_on_failure(self, tmp_path: Path) -> None:
        """On failure, env_payload_override should be None."""
        task_dir = tmp_path / "task"
        task_dir.mkdir()

        (task_dir / "env_payload_override.json").write_text('["numpy==1.21.0"]')
        (task_dir / "docker_build_pkg.sh").write_text("#!/bin/bash")
        failure = {"stage": "build", "return_code": 1, "error_message": "err"}
        (task_dir / "failure.json").write_text(json.dumps(failure))

        runner = SandboxRunner()
        codex_result = MagicMock()
        codex_result.output = ""
        codex_result.error = ""
        codex_result.raw_output = ""
        codex_result.files_changed = []

        result = runner._extract_results(tmp_path, codex_result)

        assert result.success is False
        assert result.env_payload_override is None

    def test_override_must_be_valid_json_list(self, tmp_path: Path) -> None:
        """Malformed env_payload_override.json should be treated as no override."""
        task_dir = tmp_path / "task"
        task_dir.mkdir()

        (task_dir / "env_payload_override.json").write_text("not valid json")
        (task_dir / "docker_build_pkg.sh").write_text("#!/bin/bash")
        (task_dir / "docker_build_run.sh").write_text("#!/bin/bash")
        (task_dir / "verification_success.json").write_text('{"local_image": "test:latest"}')

        runner = SandboxRunner()
        codex_result = MagicMock()
        codex_result.output = "done"
        codex_result.raw_output = ""
        codex_result.files_changed = []

        result = runner._extract_results(tmp_path, codex_result)

        assert result.success is True
        assert result.env_payload_override is None

    def test_override_rejects_non_list(self, tmp_path: Path) -> None:
        """env_payload_override.json must be a JSON list, not an object or string."""
        task_dir = tmp_path / "task"
        task_dir.mkdir()

        (task_dir / "env_payload_override.json").write_text('{"numpy": "1.21.0"}')
        (task_dir / "docker_build_pkg.sh").write_text("#!/bin/bash")
        (task_dir / "docker_build_run.sh").write_text("#!/bin/bash")
        (task_dir / "verification_success.json").write_text('{"local_image": "test:latest"}')

        runner = SandboxRunner()
        codex_result = MagicMock()
        codex_result.output = "done"
        codex_result.raw_output = ""
        codex_result.files_changed = []

        result = runner._extract_results(tmp_path, codex_result)

        assert result.success is True
        assert result.env_payload_override is None


# ── Build stage detection ──────────────────────────────────────────


class TestBuildStageDetection:
    """local_ci.py should identify which Dockerfile stage (env/pkg/run) failed."""

    def test_parse_failed_stage_env(self) -> None:
        """Detect failure in the 'env' stage from Docker build logs."""
        from datasmith.agents.templates.local_ci import _parse_failed_stage

        # Typical buildkit log: the last stage marker before the error
        logs = (
            "#5 [env 1/2] RUN git checkout abc123\n"
            "#5 DONE 0.5s\n"
            "#6 [env 2/2] RUN chmod +x ... && /workspace/repo/docker_build_env.sh\n"
            "#6 ERROR: process returned non-zero exit code: 1\n"
        )
        assert _parse_failed_stage(logs) == "env"

    def test_parse_failed_stage_pkg(self) -> None:
        """Detect failure in the 'pkg' stage from Docker build logs."""
        from datasmith.agents.templates.local_ci import _parse_failed_stage

        logs = (
            "#5 [env 2/2] RUN chmod +x ... && /workspace/repo/docker_build_env.sh\n"
            "#5 DONE 30.0s\n"
            "#6 [pkg 1/2] COPY docker_build_pkg.sh /workspace/repo/docker_build_pkg.sh\n"
            "#6 DONE 0.1s\n"
            "#7 [pkg 2/2] RUN chmod +x ... && /workspace/repo/docker_build_pkg.sh\n"
            "#7 ERROR: process returned non-zero exit code: 1\n"
        )
        assert _parse_failed_stage(logs) == "pkg"

    def test_parse_failed_stage_run(self) -> None:
        """Detect failure in the 'run' stage from Docker build logs."""
        from datasmith.agents.templates.local_ci import _parse_failed_stage

        logs = (
            "#8 [pkg 2/2] RUN chmod +x ... && /workspace/repo/docker_build_pkg.sh\n"
            "#8 DONE 15.0s\n"
            "#9 [run 1/2] COPY docker_build_run.sh /docker_build_run.sh\n"
            "#9 DONE 0.1s\n"
            "#10 [run 2/2] RUN chmod +x /docker_build_run.sh && /docker_build_run.sh\n"
            "#10 ERROR: process returned non-zero exit code: 2\n"
        )
        assert _parse_failed_stage(logs) == "run"

    def test_parse_failed_stage_no_markers(self) -> None:
        """When no stage markers are found, return 'build' as fallback."""
        from datasmith.agents.templates.local_ci import _parse_failed_stage

        logs = "Some random error output\nwithout any stage markers\n"
        assert _parse_failed_stage(logs) == "build"

    def test_parse_failed_stage_empty(self) -> None:
        """Empty log returns 'build' fallback."""
        from datasmith.agents.templates.local_ci import _parse_failed_stage

        assert _parse_failed_stage("") == "build"

    def test_parse_failed_stage_legacy_docker(self) -> None:
        """Legacy (non-buildkit) Docker output with 'Step N/M : FROM x AS stage'."""
        from datasmith.agents.templates.local_ci import _parse_failed_stage

        logs = (
            "Step 3/10 : FROM env AS pkg\n"
            "Step 4/10 : COPY docker_build_pkg.sh /workspace/repo/docker_build_pkg.sh\n"
            "Step 5/10 : RUN chmod +x ... && /workspace/repo/docker_build_pkg.sh\n"
            "The command '/bin/sh -c chmod +x ...' returned a non-zero code: 1\n"
        )
        assert _parse_failed_stage(logs) == "pkg"

    def test_write_failure_uses_detected_stage(self, tmp_path: Path) -> None:
        """_write_failure should write the specific stage name, not generic 'build'."""
        from datasmith.agents.templates.local_ci import _write_failure

        _write_failure(tmp_path, "env", stdout="log output", stderr="error", rc=1)

        failure = json.loads((tmp_path / "failure.json").read_text())
        assert failure["stage"] == "env"
        assert failure["return_code"] == 1

    def test_build_error_carries_stage_through_verify(self, tmp_path: Path) -> None:
        """When build_image raises BuildError, verify() should detect and write the correct stage."""
        from datasmith.agents.templates.local_ci import _parse_failed_stage, _write_failure

        # Simulate: BuildError with env-stage logs
        build_stdout = (
            "#5 [env 2/2] RUN chmod +x ... && /workspace/repo/docker_build_env.sh\n"
            "#5 ERROR: process returned non-zero exit code: 1\n"
        )
        stage = _parse_failed_stage(build_stdout)
        assert stage == "env"

        # Write failure with detected stage
        _write_failure(tmp_path, stage, stdout=build_stdout, stderr="", rc=1)
        failure = json.loads((tmp_path / "failure.json").read_text())
        assert failure["stage"] == "env"


class TestSolutionPatchPlumbing:
    def _prepare(self, tmp_path, **kw):
        from datasmith.agents.sandbox import SandboxRunner

        runner = SandboxRunner()
        runner._prepare_workspace(
            workspace=tmp_path,
            owner="o",
            repo="r",
            sha="s" * 40,
            repo_image="img",
            env_payload="[]",
            python_version="3.11",
            pr_context="ctx",
            base_sha="b" * 40,
            **kw,
        )

    def test_prepare_workspace_writes_solution_patch(self, tmp_path):
        self._prepare(tmp_path, solution_patch="diff --git a/x b/x\n")
        assert (tmp_path / "task" / "solution.patch").read_text() == "diff --git a/x b/x\n"

    def test_absent_patch_writes_an_empty_file_not_nothing(self, tmp_path):
        """local_ci.py mounts this path unconditionally; a missing file makes
        `docker run -v` create a DIRECTORY at the mount point, which the
        applier then cannot read."""
        self._prepare(tmp_path)
        p = tmp_path / "task" / "solution.patch"
        assert p.exists()
        assert p.read_text() == ""

    def test_solution_patch_is_immutable(self):
        from datasmith.agents.sandbox import _IMMUTABLE_FILES

        assert "solution.patch" in _IMMUTABLE_FILES

    def test_solution_patch_is_hashed_for_integrity(self, tmp_path):
        import json

        self._prepare(tmp_path, solution_patch="diff --git a/x b/x\n")
        hashes = json.loads((tmp_path / ".immutable_hashes.json").read_text())
        assert "solution.patch" in hashes

    def test_measure_scripts_are_copied_into_the_task_dir(self, tmp_path):
        self._prepare(tmp_path)
        for name in (
            "measure.sh",
            "apply_oracle_patch.py",
            "emit_measure.py",
            "lsv_init.py",
            "lsv_measure.py",
            "parser.py",
        ):
            assert (tmp_path / "task" / name).exists(), name


class TestEveryCopySourceReachesTheTaskDir:
    """A COPY source missing from the task dir breaks the build before the
    container can measure anything.

    Driven off Dockerfile.pr's actual COPY directives rather than a
    hand-written list: adding a COPY without updating the copy lists fails
    here immediately. There are THREE independent producers of a task
    directory (SandboxRunner._prepare_workspace, verify_context, and
    _fill_missing_scripts) and they each carry their own list, so each is
    checked.
    """

    def _copy_sources(self, task_dir) -> list[str]:
        sources: list[str] = []
        for line in (task_dir / "Dockerfile.pr").read_text().splitlines():
            s = line.strip()
            if s.upper().startswith("COPY "):
                parts = s.split()[1:]
                sources.extend(p for p in parts[:-1] if not p.startswith("--"))
        assert sources, "no COPY directives parsed — this guard would pass vacuously"
        return sources

    def test_prepare_workspace_supplies_every_copy_source(self, tmp_path):
        from datasmith.agents.sandbox import SandboxRunner

        SandboxRunner()._prepare_workspace(
            workspace=tmp_path,
            owner="o",
            repo="r",
            sha="a" * 40,
            repo_image="img",
            env_payload="[]",
            python_version="3.11",
            pr_context="ctx",
            base_sha="b" * 40,
        )
        task = tmp_path / "task"
        have = {p.name for p in task.iterdir()}
        missing = [n for n in self._copy_sources(task) if n not in have]
        assert not missing, f"Dockerfile.pr COPYs files _prepare_workspace never writes: {missing}"

    def test_fill_missing_scripts_supplies_every_copy_source(self, tmp_path):
        from datasmith.runners.synthesize_images import _fill_missing_scripts

        _fill_missing_scripts(str(tmp_path), base_commit="deadbeef")
        have = {p.name for p in tmp_path.iterdir()}
        missing = [n for n in self._copy_sources(tmp_path) if n not in have]
        assert not missing, f"Dockerfile.pr COPYs files _fill_missing_scripts never writes: {missing}"

    def test_verify_contexts_copy_list_matches_prepare_workspaces(self):
        """verify_context duplicates _prepare_workspace's copy list. Nothing
        else keeps the two in sync, and TRY_SIMILAR goes through
        verify_context — so drift silently breaks that path only."""
        import re
        from pathlib import Path

        src = (Path(__file__).parents[2] / "src" / "datasmith" / "agents" / "sandbox.py").read_text()
        tuples = re.findall(r"for fname in \(\n(.*?)\n        \):", src, re.DOTALL)
        assert len(tuples) == 2, f"expected 2 template copy lists in sandbox.py, found {len(tuples)}"
        parsed = [sorted(re.findall(r'"([^"]+)"', block)) for block in tuples]
        assert parsed[0] == parsed[1], f"copy lists have drifted: {parsed[0]} vs {parsed[1]}"

        lsv_tuples = re.findall(r'for fname in \("lsv_init\.py".*?\):', src)
        assert len(lsv_tuples) == 2, f"expected 2 LSV copy loops, found {len(lsv_tuples)}"
        assert lsv_tuples[0] == lsv_tuples[1]


class TestAVerificationTimeoutKillsItsContainers:
    """Killing local_ci.py does not stop the containers it started.

    `verify_context` runs local_ci.py under `subprocess.run(timeout=...)`.
    When that fires, Python kills local_ci while its container keeps running
    in the daemon -- and local_ci deliberately omits `--rm` so it can collect
    metrics after exit, so its own cleanup is the only thing that would have
    removed the container, and it never runs.

    Measured 2026-08-25/26: containers surviving 90+ minutes against a 3600 s
    timeout, with the host at load 372 on 128 cores doing work nothing was
    waiting for. The container name is a uuid the parent never sees, so the
    label is what makes cleanup possible.
    """

    def test_labelled_containers_are_force_removed(self, monkeypatch) -> None:
        import datasmith.agents.sandbox as sb

        calls: list[list[str]] = []

        class _P:
            stdout = "abc123 def456"
            returncode = 0

        def fake_run(cmd, **kwargs):
            calls.append(list(cmd))
            return _P()

        monkeypatch.setattr(sb.subprocess, "run", fake_run)
        removed = sb._kill_labelled_containers("verify-deadbeef")
        assert removed == 2
        assert calls[0][:3] == ["docker", "ps", "-q"]
        assert "label=fc.run=verify-deadbeef" in calls[0]
        assert [c for c in calls if c[:3] == ["docker", "rm", "-f"]], "must force-remove what it listed"

    def test_it_never_raises_on_a_docker_failure(self, monkeypatch) -> None:
        """This runs on a path that is already failing; an exception here would
        replace a timeout result with a crash."""
        import datasmith.agents.sandbox as sb

        def boom(cmd, **kwargs):
            raise OSError("docker is gone")

        monkeypatch.setattr(sb.subprocess, "run", boom)
        assert sb._kill_labelled_containers("verify-x") == 0

    def test_nothing_to_remove_is_not_an_error(self, monkeypatch) -> None:
        import datasmith.agents.sandbox as sb

        class _P:
            stdout = ""
            returncode = 0

        monkeypatch.setattr(sb.subprocess, "run", lambda cmd, **kw: _P())
        assert sb._kill_labelled_containers("verify-y") == 0

    def test_local_ci_labels_the_containers_it_starts(self) -> None:
        """The parent can only clean up what the child labelled."""
        from pathlib import Path

        src = Path(sb_path()).read_text()
        assert 'os.environ.get("FC_RUN_LABEL"' in src, "local_ci must read the label"
        assert '"--label", f"fc.run={run_label}"' in src, "and apply it to docker run"


def sb_path() -> str:
    from pathlib import Path

    import datasmith

    return str(Path(datasmith.__file__).parent / "agents" / "templates" / "local_ci.py")


class TestTheVerificationTimeoutCoversItsOwnSteps:
    """An outer wrapper must not be smaller than the steps it wraps.

    `verify_context` bounds build + tests + measurement together. It was 3600 s
    — the same budget `local_ci.py` gives to tests alone and to measurement
    alone. Work still inside its own allowance was killed, and the result came
    back as `Timed out after 3600s`, indistinguishable from a hang.

    Measured on the verified corpus 2026-08-26 (tests + measurement, excluding
    build): bottleneck#305 3351 s, bottleneck#298 2615 s, uxarray#1118 2039 s.
    The first cleared the old budget by 249 s and only because its build was
    cached. 103 rounds across the grind burned on this timeout.
    """

    def test_the_outer_budget_exceeds_the_inner_ones(self) -> None:
        import datasmith.agents.sandbox as sb

        test_s = int(os.environ.get("DATASMITH_VERIFY_TEST_TIMEOUT_S", "3600"))
        measure_s = int(os.environ.get("DATASMITH_VERIFY_MEASURE_TIMEOUT_S", "3600"))
        longest_step = max(test_s, measure_s)
        assert longest_step < sb.DATASMITH_VERIFY_TIMEOUT_S, "the wrapper must outlive any single step it wraps"

    def test_it_covers_the_largest_observed_real_run(self) -> None:
        """bottleneck#305: 68 s of tests plus 3283 s of measurement."""
        import datasmith.agents.sandbox as sb

        assert sb.DATASMITH_VERIFY_TIMEOUT_S >= 3351, "the corpus already contains a run this long"

    def test_it_is_overridable(self, monkeypatch) -> None:
        """A hung-task problem is fixed by lowering this, not by editing code."""
        import importlib

        monkeypatch.setenv("DATASMITH_VERIFY_TIMEOUT_S", "1234")
        import datasmith.agents.sandbox as sb

        try:
            importlib.reload(sb)
            assert sb.DATASMITH_VERIFY_TIMEOUT_S == 1234
        finally:
            monkeypatch.delenv("DATASMITH_VERIFY_TIMEOUT_S", raising=False)
            importlib.reload(sb)

    def test_verify_context_defaults_to_the_knob(self) -> None:
        import inspect

        import datasmith.agents.sandbox as sb

        sig = inspect.signature(sb.verify_context)
        assert sig.parameters["timeout_s"].default == sb.DATASMITH_VERIFY_TIMEOUT_S


class TestTheTimeoutHandlerActuallyCallsTheCleanup:
    """Asserted over the AST, not the source text.

    The isolated tests for `_kill_labelled_containers` all passed with the call
    removed from `verify_context`'s timeout handler — they proved the helper
    works, not that anything invokes it. A string search would be little
    better: the helper's name appears in comments and in its own definition.

    This walks the `except subprocess.TimeoutExpired` handler inside
    `verify_context` and requires a call to the helper in its body, which is
    the thing that actually stops containers outliving the run.
    """

    @staticmethod
    def _timeout_handlers():
        import ast
        import inspect
        import textwrap

        import datasmith.agents.sandbox as sb

        tree = ast.parse(textwrap.dedent(inspect.getsource(sb.verify_context)))
        found = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.ExceptHandler):
                continue
            names = [n.attr for n in ast.walk(node.type) if isinstance(n, ast.Attribute)] if node.type else []
            if "TimeoutExpired" in names:
                found.append(node)
        return found

    def test_a_timeout_handler_exists(self) -> None:
        assert self._timeout_handlers(), "verify_context must handle its own timeout"

    def test_every_timeout_handler_cleans_up_its_containers(self) -> None:
        import ast

        for handler in self._timeout_handlers():
            called = {n.func.id for n in ast.walk(handler) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
            assert "_kill_labelled_containers" in called, (
                "a timeout that leaves its containers running is how the host reached "
                "load 372 with work nothing was waiting for"
            )

    def test_the_run_label_is_passed_to_the_child(self) -> None:
        """The child can only label what the parent told it to label."""
        import ast
        import inspect
        import textwrap

        import datasmith.agents.sandbox as sb

        tree = ast.parse(textwrap.dedent(inspect.getsource(sb.verify_context)))
        keys = [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and n.value == "FC_RUN_LABEL"]
        assert keys, "verify_context must export FC_RUN_LABEL to local_ci.py"
