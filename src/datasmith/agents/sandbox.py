"""Sandboxed synthesis via an installed CLI agent.

Prepares a temporary workspace with Docker build context files, an AGENTS.md
guide, and a simplified verify script, then launches the first available
installed agent (Claude Code, Codex, or Gemini CLI). The agent iterates
internally — reading failure.json, editing build scripts, re-running
verification — until it succeeds or the session times out.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from datasmith.agents.installed import AgentResult, get_agent
from datasmith.docker.context import DockerContext
from datasmith.utils import get_logger

logger = get_logger("agents.sandbox")

_TEMPLATES_DIR = Path(__file__).parent / "templates"


@dataclass
class SandboxConfig:
    """Configuration for the Codex sandbox runner."""

    timeout_s: int = 1800
    """Total wall-clock timeout for the codex session (seconds)."""

    codex_timeout_s: int = 1800
    """Timeout passed to subprocess.run for the codex process (seconds)."""

    skip_tests: bool = False
    """Pass --skip-tests to sandbox_verify.py."""


@dataclass
class SandboxResult:
    """Outcome of a sandbox synthesis run."""

    success: bool
    docker_context: DockerContext | None = None
    failure_json: dict | None = None
    duration_s: float = 0.0
    agent_output: str = ""


class SandboxRunner:
    """Launch an installed CLI agent in a sandboxed workspace to iteratively fix Docker builds."""

    def __init__(self, config: SandboxConfig | None = None) -> None:
        self._config = config or SandboxConfig()

    def run(
        self,
        owner: str,
        repo: str,
        sha: str,
        base_context: DockerContext,
        env_payload: str,
        python_version: str,
        pr_context: str,
        dry_run: bool = False,
    ) -> SandboxResult:
        """Prepare workspace, launch agent, extract results.

        Returns a ``SandboxResult`` indicating success/failure and the
        (potentially modified) ``DockerContext``.
        """
        start = time.time()

        with tempfile.TemporaryDirectory(prefix="synthesis-") as tmpdir:
            workspace = Path(tmpdir)

            # 1. Prepare workspace
            self._prepare_workspace(
                workspace=workspace,
                owner=owner,
                repo=repo,
                sha=sha,
                base_context=base_context,
                env_payload=env_payload,
                python_version=python_version,
                pr_context=pr_context,
            )

            # 2. Init git repo (Codex requirement)
            self._init_git(workspace)

            if dry_run:
                logger.info(
                    "[DRY RUN] Would launch agent sandbox for %s/%s@%s in %s",
                    owner,
                    repo,
                    sha[:12],
                    workspace,
                )
                return SandboxResult(
                    success=True,
                    docker_context=base_context,
                    duration_s=time.time() - start,
                    agent_output="[dry run — no execution]",
                )

            # 3. Launch agent
            agent_result = self._launch_agent(workspace)

            # 4. Extract results
            result = self._extract_results(workspace, agent_result)
            result.duration_s = time.time() - start
            return result

    def _prepare_workspace(
        self,
        workspace: Path,
        owner: str,
        repo: str,
        sha: str,
        base_context: DockerContext,
        env_payload: str,
        python_version: str,
        pr_context: str,
    ) -> None:
        """Create the workspace directory structure."""
        task_dir = workspace / "task"

        # Write all 9 context files
        base_context.to_directory(str(task_dir))

        # Overwrite profile.sh, entrypoint.sh with latest templates (static files)
        docker_templates = Path(__file__).parents[1] / "docker" / "templates"
        for fname in ("profile.sh", "entrypoint.sh"):
            src = docker_templates / fname
            if src.exists():
                shutil.copy2(str(src), str(task_dir / fname))

        # Render run-tests.sh from Jinja2 template with embedded scripts
        run_tests_sh = _render_run_tests_sh(docker_templates, base_commit=sha)
        (task_dir / "run-tests.sh").write_text(run_tests_sh)

        # Generate task.txt
        task_txt = _generate_task_txt(owner, repo, sha, env_payload, python_version)
        (task_dir / "task.txt").write_text(task_txt)

        # Render AGENTS.md from Jinja2 template
        agents_md = _render_agents_md(
            owner=owner,
            repo=repo,
            sha=sha,
            python_version=python_version,
            pr_context=pr_context,
        )
        (workspace / "AGENTS.md").write_text(agents_md)

        # Copy sandbox_verify.py
        src_verify = _TEMPLATES_DIR / "sandbox_verify.py"
        shutil.copy2(str(src_verify), str(workspace / "sandbox_verify.py"))

    def _init_git(self, workspace: Path) -> None:
        """Initialize a git repo in the workspace (required by Codex)."""
        subprocess.run(
            ["git", "init"],  # noqa: S607
            cwd=str(workspace),
            capture_output=True,
            check=True,
        )
        subprocess.run(
            ["git", "add", "-A"],  # noqa: S607
            cwd=str(workspace),
            capture_output=True,
            check=True,
        )
        subprocess.run(
            [  # noqa: S607
                "git",
                "-c",
                "user.name=sandbox",
                "-c",
                "user.email=sandbox@local",
                "commit",
                "-m",
                "init",
            ],
            cwd=str(workspace),
            capture_output=True,
            check=True,
        )

    def _launch_agent(self, workspace: Path) -> AgentResult:
        """Launch the first available installed CLI agent in the workspace."""
        agent = get_agent()
        logger.info("Launching %s agent sandbox in %s", agent.name(), workspace)
        return agent.exec(
            prompt="Read AGENTS.md and follow its instructions to fix the Docker build.",
            timeout=self._config.codex_timeout_s,
            workdir=str(workspace),
        )

    def _extract_results(self, workspace: Path, codex_result: AgentResult) -> SandboxResult:
        """Read workspace state after Codex exits to build the result."""
        task_dir = workspace / "task"

        # Check for success
        success_file = task_dir / "verification_success.json"
        failure_file = task_dir / "failure.json"

        success = success_file.exists()

        # Read modified context back
        docker_context: DockerContext | None = None
        try:
            docker_context = DockerContext.from_directory(str(task_dir))
        except Exception:
            logger.warning("Failed to read Docker context from workspace")

        # Read failure.json if present
        failure_json: dict | None = None
        if failure_file.exists():
            try:
                failure_json = json.loads(failure_file.read_text())
            except Exception:
                logger.warning("Failed to parse failure.json")

        if success:
            logger.info("Sandbox synthesis succeeded")
        else:
            stage = failure_json.get("stage", "unknown") if failure_json else "unknown"
            logger.warning("Sandbox synthesis failed at stage: %s", stage)

        return SandboxResult(
            success=success,
            docker_context=docker_context if success else None,
            failure_json=failure_json,
            agent_output=codex_result.output,
        )


def _generate_task_txt(
    owner: str,
    repo: str,
    sha: str,
    env_payload: str,
    python_version: str,
) -> str:
    """Generate a task.txt file content."""
    # Escape env_payload for repr
    return (
        f"Task(\n"
        f"    owner={owner!r},\n"
        f"    repo={repo!r},\n"
        f"    sha={sha!r},\n"
        f"    commit_date=0.0,\n"
        f"    env_payload={env_payload!r},\n"
        f"    python_version={python_version!r},\n"
        f"    tag='pkg',\n"
        f"    benchmarks=''\n"
        f")\n"
    )


def _render_agents_md(
    owner: str,
    repo: str,
    sha: str,
    python_version: str,
    pr_context: str,
) -> str:
    """Render the AGENTS.md template with task-specific variables."""
    env = Environment(
        loader=FileSystemLoader(str(_TEMPLATES_DIR)),
        keep_trailing_newline=True,
        autoescape=False,
    )
    template = env.get_template("AGENTS.md.j2")
    return template.render(
        owner=owner,
        repo=repo,
        sha=sha,
        python_version=python_version,
        pr_context=pr_context,
    )


def _render_run_tests_sh(docker_templates: Path, base_commit: str) -> str:
    """Render the run-tests.sh Jinja2 template with embedded scripts."""
    env = Environment(
        loader=FileSystemLoader(str(docker_templates)),
        keep_trailing_newline=True,
        autoescape=False,
    )
    template = env.get_template("run-tests.sh")

    # Read the embedded scripts
    pytest_runner = (docker_templates / "pytest_runner.py").read_text()
    parser = (docker_templates / "parser.py").read_text()

    return template.render(
        base_commit=base_commit,
        pytest_runner=pytest_runner,
        parser=parser,
        run_pytest=True,
    )
