"""Sandboxed synthesis via an installed CLI agent.

Prepares a temporary workspace with Docker build context files, an AGENTS.md
guide, and a simplified verify script, then launches the first available
installed agent (Claude Code, Codex, or Gemini CLI). The agent iterates
internally — reading failure.json, editing build scripts, re-running
verification — until it succeeds or the session times out.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from datasmith.agents.installed import AgentResult, get_agent
from datasmith.docker.context import DockerContext
from datasmith.utils import get_logger

logger = get_logger("agents.sandbox")

# Wall-clock for ONE verification: build + tests + measurement, together.
#
# This used to be 3600 s -- the same budget `local_ci.py` gives to tests ALONE
# and to measurement ALONE (`DATASMITH_VERIFY_TEST_TIMEOUT_S`,
# `DATASMITH_VERIFY_MEASURE_TIMEOUT_S`, both 3600). An outer wrapper no larger
# than one of its own steps kills work that is still inside its allowance, and
# it does so silently: the run comes back as `Timed out after 3600s`, which is
# indistinguishable from a hang.
#
# Measured on the verified corpus 2026-08-26, tests + measurement alone:
# bottleneck#305 3351 s (93% of the old budget, and it only fit because its
# build was cached), bottleneck#298 2615 s, uxarray#1118 2039 s, networkx#8138
# 1693 s, tiled#1283 1555 s. Add a build and the largest repos cannot finish
# inside an hour at all -- which is why every repo with a big test suite or a
# large benchmark suite had never produced a verified container, while 103
# rounds burned on `Timed out after 3600s`.
#
# 5400 covers the observed maximum with room for a cold build. It is a
# trade-off, not a free win: a genuinely hung task now costs more before the
# stall detector ends it, and only 3 of 38 tasks that hit a timeout ever went
# on to be accepted. Lower it if hung tasks start dominating again.
DATASMITH_VERIFY_TIMEOUT_S: int = int(os.environ.get("DATASMITH_VERIFY_TIMEOUT_S", "5400"))


_TEMPLATES_DIR = Path(__file__).parent / "templates"

# Files the agent must NOT modify.  Hashes are recorded at workspace setup
# and verified both by local_ci.py (so the agent gets feedback) and by
# _extract_results (hard server-side check the agent cannot bypass).
_IMMUTABLE_FILES = (
    "Dockerfile.pr",
    "docker_build_base.sh",
    "docker_build_final.sh",
    "emit_manifest.py",
    "measure.sh",
    "apply_oracle_patch.py",
    "emit_measure.py",
    "profile.sh",
    "run-tests.sh",
    "entrypoint.sh",
    "task.txt",
    "solution.patch",
)


def _compute_immutable_hashes(task_dir: Path) -> dict[str, str]:
    """Compute MD5 hashes of all immutable files in *task_dir*."""
    hashes: dict[str, str] = {}
    for fname in _IMMUTABLE_FILES:
        fp = task_dir / fname
        if fp.exists():
            hashes[fname] = hashlib.md5(fp.read_bytes()).hexdigest()  # noqa: S324
    return hashes


def _read_env_payload_override(task_dir: Path) -> str | None:
    """Read and validate ``env_payload_override.json`` from *task_dir*.

    Returns the raw JSON string if the file exists and contains a valid
    JSON list, otherwise ``None``.
    """
    override_file = task_dir / "env_payload_override.json"
    if not override_file.exists():
        return None
    try:
        raw = override_file.read_text()
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return raw
        logger.warning("env_payload_override.json is not a JSON list, ignoring")
    except (json.JSONDecodeError, Exception):
        logger.warning("Failed to parse env_payload_override.json, ignoring")
    return None


@dataclass
class SandboxConfig:
    """Configuration for the Codex sandbox runner."""

    timeout_s: int = int(os.environ.get("SYNTHESIS_TIMEOUT_S", "14400"))
    """Total wall-clock timeout for the codex session (seconds)."""

    codex_timeout_s: int = int(os.environ.get("SYNTHESIS_TIMEOUT_S", "14400"))
    """Timeout passed to subprocess.run for the codex process (seconds)."""


@dataclass
class SandboxResult:
    """Outcome of a sandbox synthesis run."""

    success: bool
    docker_context: DockerContext | None = None
    failure_json: dict | None = None
    duration_s: float = 0.0
    agent_output: str = ""
    raw_agent_output: str = ""
    agent_name: str = ""
    files_changed: list[str] = field(default_factory=list)
    resource_metrics: dict = field(default_factory=dict)
    build_manifest: dict | None = None
    env_payload_override: str | None = None
    aborted: bool = False
    """True when the agent exited without ever producing failure.json or
    verification_success.json — i.e. it never ran (or never finished running)
    local_ci.py. Distinct from a real verifier failure, and should not
    consume the synthesizer's per-PR attempt budget."""

    # The tag the build actually produced. Callers must never reconstruct it:
    # verify_context serves TRY_SIMILAR and does not necessarily tag what
    # another caller would guess.
    image_tag: str = ""


class SandboxRunner:
    """Launch an installed CLI agent in a sandboxed workspace to iteratively fix Docker builds."""

    def __init__(self, config: SandboxConfig | None = None, agent: str | None = None) -> None:
        self._config = config or SandboxConfig()
        self._agent = agent

    def run(
        self,
        owner: str,
        repo: str,
        sha: str,
        repo_image: str,
        env_payload: str,
        python_version: str,
        pr_context: str,
        prior_attempts: str = "",
        dry_run: bool = False,
        base_sha: str = "",
        solution_patch: str = "",
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
                repo_image=repo_image,
                env_payload=env_payload,
                python_version=python_version,
                pr_context=pr_context,
                prior_attempts=prior_attempts,
                base_sha=base_sha,
                solution_patch=solution_patch,
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
                    docker_context=DockerContext(),
                    duration_s=time.time() - start,
                    agent_output="[dry run — no execution]",
                )

            # 3. Launch agent
            agent_name, agent_result = self._launch_agent(workspace)

            # 4. Extract results
            result = self._extract_results(workspace, agent_result, agent_name)
            result.duration_s = time.time() - start
            return result

    def _prepare_workspace(
        self,
        workspace: Path,
        owner: str,
        repo: str,
        sha: str,
        repo_image: str,
        env_payload: str,
        python_version: str,
        pr_context: str,
        prior_attempts: str = "",
        base_sha: str = "",
        solution_patch: str = "",
    ) -> None:
        """Create the workspace directory structure."""
        task_dir = workspace / "task"
        task_dir.mkdir(parents=True, exist_ok=True)

        # Use base_sha for Docker checkout so the repo is at the
        # pre-optimization state; fall back to merge_commit_sha for compat.
        checkout_sha = base_sha or sha

        # Copy ALL template files from docker/templates/ into task/
        docker_templates = Path(__file__).parents[1] / "docker" / "templates"
        for fname in (
            "Dockerfile.pr",
            "docker_build_base.sh",
            "docker_build_env.sh",
            "docker_build_pkg.sh",
            "docker_build_run.sh",
            "docker_build_final.sh",
            "emit_manifest.py",
            "measure.sh",
            "apply_oracle_patch.py",
            "emit_measure.py",
            "profile.sh",
            "entrypoint.sh",
        ):
            src = docker_templates / fname
            if src.exists():
                shutil.copy2(str(src), str(task_dir / fname))

        # lsv_init.py / lsv_measure.py / parser.py are shared with the harbor
        # trial path — copied, never forked, so a change to LSV selection
        # affects stage 6 and stage 7 identically.
        lsv_templates = Path(__file__).parents[1] / "harbor_adapter" / "template"
        for fname in ("lsv_init.py", "lsv_measure.py", "parser.py"):
            src = lsv_templates / fname
            if src.exists():
                shutil.copy2(str(src), str(task_dir / fname))

        # Render run-tests.sh from Jinja2 template with embedded scripts
        run_tests_sh = _render_run_tests_sh(docker_templates, base_commit=checkout_sha)
        (task_dir / "run-tests.sh").write_text(run_tests_sh)

        # Generate task.txt — use checkout_sha so Dockerfile.pr checks out
        # the base commit, not the merge commit.
        task_txt = _generate_task_txt(owner, repo, checkout_sha, env_payload, python_version, repo_image)
        (task_dir / "task.txt").write_text(task_txt)

        # The oracle patch, mounted read-only into the measure container by
        # local_ci.py.  Always written — even empty — because `docker run -v`
        # creates a DIRECTORY at a mount source that does not exist, which
        # apply_oracle_patch.py then cannot read.  It is deliberately NOT a
        # Dockerfile.pr COPY target: a published image carrying the oracle
        # solution would be readable by the agent under evaluation.
        (task_dir / "solution.patch").write_text(solution_patch or "")

        # Render AGENTS.md from Jinja2 template
        agents_md = _render_agents_md(
            owner=owner,
            repo=repo,
            sha=sha,
            python_version=python_version,
            pr_context=pr_context,
        )
        (workspace / "AGENTS.md").write_text(agents_md)

        # Copy local_ci.py
        src_verify = _TEMPLATES_DIR / "local_ci.py"
        shutil.copy2(str(src_verify), str(workspace / "local_ci.py"))

        # Write prior attempts context (from failed TRY_SIMILAR stage)
        if prior_attempts:
            (workspace / "prior_attempts.md").write_text(prior_attempts)

        # Record immutable file hashes so local_ci.py and
        # _extract_results can detect unauthorised modifications.
        hashes = _compute_immutable_hashes(task_dir)
        (workspace / ".immutable_hashes.json").write_text(json.dumps(hashes))

    def _init_git(self, workspace: Path) -> None:
        """Initialize a git repo in the workspace (required by Codex)."""
        subprocess.run(
            ["git", "init"],
            cwd=str(workspace),
            capture_output=True,
            check=True,
        )
        subprocess.run(
            ["git", "add", "-A"],
            cwd=str(workspace),
            capture_output=True,
            check=True,
        )
        subprocess.run(
            [
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

    def _launch_agent(self, workspace: Path) -> tuple[str, AgentResult]:
        """Launch the first available installed CLI agent in the workspace.

        Returns ``(agent_name, AgentResult)``.
        """
        preference = [self._agent] if self._agent else None
        agent = get_agent(preference=preference)
        logger.info("Launching %s agent sandbox in %s", agent.name(), workspace)
        result = agent.exec(
            prompt=(
                "Read AGENTS.md and follow its instructions to fix the Docker build.\n"
                "\n"
                "HARD REQUIREMENTS — your work will be discarded otherwise:\n"
                "1. You MUST execute `python3 local_ci.py` from the workspace root "
                "at least once. This is the ONLY accepted verifier — do not validate by "
                "running pip, docker, or build commands outside this script.\n"
                "2. The ONLY accepted success state is `task/verification_success.json` "
                "existing on disk when you exit. A `bash -n` syntax check, a 'local' pip "
                "install, or a manual docker build does NOT count and will be ignored.\n"
                "3. Do NOT touch processes outside the workspace. Never run `pkill`, "
                "`kill`, `killall`, or `pgrep` against `local_ci`, `docker`, "
                "`asv`, `pytest`, or any other process — peer worker processes you may "
                "see in `ps` belong to other tasks and must be left alone.\n"
                "4. A full docker build inside `local_ci.py` typically takes "
                "15-40 minutes. Budget your turns accordingly and wait for it to "
                "finish; do not exit while a build is still running.\n"
            ),
            timeout=self._config.codex_timeout_s,
            workdir=str(workspace),
        )
        logger.info(
            "Agent %s exited (success=%s, duration=%.1fs, output_len=%d, error=%s)",
            agent.name(),
            result.success,
            result.duration_s,
            len(result.output),
            result.error[:200] if result.error else "",
        )
        return agent.name(), result

    def _extract_results(self, workspace: Path, codex_result: AgentResult, agent_name: str = "") -> SandboxResult:
        """Read workspace state after the agent exits to build the result."""
        task_dir = workspace / "task"

        # Hard integrity check — the agent cannot bypass this even if it
        # modifies local_ci.py or writes a fake success file.
        hashes_file = workspace / ".immutable_hashes.json"
        if hashes_file.exists():
            expected = json.loads(hashes_file.read_text())
            current = _compute_immutable_hashes(task_dir)
            modified = [f for f in expected if expected[f] != current.get(f, "")]
            if modified:
                logger.warning("File integrity violation: %s", ", ".join(modified))
                return SandboxResult(
                    success=False,
                    failure_json={
                        "stage": "integrity",
                        "return_code": 1,
                        "error_message": f"Agent modified immutable files: {', '.join(modified)}",
                    },
                    agent_output=codex_result.output,
                    raw_agent_output=codex_result.raw_output,
                    agent_name=agent_name,
                    files_changed=codex_result.files_changed,
                )

        # Check for success
        success_file = task_dir / "verification_success.json"
        failure_file = task_dir / "failure.json"

        success = success_file.exists()

        # Read back the agent-editable scripts (the rest are templates)
        docker_context: DockerContext | None = None
        try:
            pkg_sh = (
                (task_dir / "docker_build_pkg.sh").read_text() if (task_dir / "docker_build_pkg.sh").exists() else ""
            )
            run_sh = (
                (task_dir / "docker_build_run.sh").read_text() if (task_dir / "docker_build_run.sh").exists() else ""
            )
            env_sh = (
                (task_dir / "docker_build_env.sh").read_text() if (task_dir / "docker_build_env.sh").exists() else ""
            )
            docker_context = DockerContext(build_pkg_sh=pkg_sh, build_run_sh=run_sh, build_env_sh=env_sh)
        except Exception:
            logger.warning("Failed to read Docker context from workspace")

        # Read env_payload override if the agent wrote one
        env_payload_override = _read_env_payload_override(task_dir)

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
            if stage == "unknown":
                # No failure.json means local_ci.py was never run or crashed.
                # Log agent details to help diagnose why.
                logger.warning(
                    "No failure.json found — agent likely never ran local_ci.py. Agent error: %s",
                    codex_result.error[:500] if codex_result.error else "(none)",
                )
                if codex_result.output:
                    logger.info(
                        "Agent output (last 1000 chars): %s",
                        codex_result.output[-1000:],
                    )

        # Extract resource_metrics from whichever JSON file was written
        resource_metrics = _extract_resource_metrics(success_file, failure_file, failure_json)
        build_manifest = _extract_build_manifest(success_file, failure_file, failure_json)

        # An "aborted" attempt is one where the agent exited without producing
        # either result file. Distinct from a real verifier failure: the
        # synthesizer should retry these without consuming the attempt budget.
        aborted = (not success) and (not failure_file.exists())

        return SandboxResult(
            success=success,
            docker_context=docker_context if success else None,
            failure_json=failure_json,
            agent_output=codex_result.output,
            raw_agent_output=codex_result.raw_output,
            agent_name=agent_name,
            files_changed=codex_result.files_changed,
            resource_metrics=resource_metrics,
            build_manifest=build_manifest,
            env_payload_override=env_payload_override if success else None,
            aborted=aborted,
        )


def _extract_resource_metrics(
    success_file: Path,
    failure_file: Path,
    failure_json: dict | None,
) -> dict:
    """Read ``resource_metrics`` from the verification JSON files.

    ``local_ci.py`` writes metrics into both ``verification_success.json``
    and ``failure.json``.  We check the success file first (authoritative on
    success), then fall back to the failure JSON dict (already parsed by caller).
    """
    if success_file.exists():
        try:
            data = json.loads(success_file.read_text())
            rm = data.get("resource_metrics")
            if isinstance(rm, dict):
                return dict(rm)
        except Exception:
            logger.debug("Failed to read resource_metrics from success file")
    if isinstance(failure_json, dict):
        metrics = failure_json.get("resource_metrics")
        if isinstance(metrics, dict):
            return metrics
    return {}


def _extract_image_tag(success_file: Path) -> str:
    """Read the tag local_ci.py actually built, or "" if it never got that far.

    ``local_ci.py`` records it as ``local_image`` in
    ``verification_success.json``. Read rather than reconstructed: the tag
    is local_ci.py's to name, and a caller that rebuilt the string would
    drift the moment that naming changes.
    """
    if not success_file.exists():
        return ""
    try:
        data = json.loads(success_file.read_text())
    except Exception:
        logger.debug("Failed to read local_image from success file")
        return ""
    tag = data.get("local_image")
    return tag if isinstance(tag, str) else ""


def _extract_build_manifest(success_file: Path, failure_file: Path, failure_json: dict | None) -> dict | None:
    """Pull ``build_manifest`` out of the verification JSON files.

    It travels inside ``resource_metrics`` because local_ci.py writes it
    there, reusing the existing plumbing rather than adding a channel.
    """
    metrics = _extract_resource_metrics(success_file, failure_file, failure_json)
    manifest = (metrics or {}).get("build_manifest")
    return manifest if isinstance(manifest, dict) else None


def _generate_task_txt(
    owner: str,
    repo: str,
    sha: str,
    env_payload: str,
    python_version: str,
    repo_image: str = "",
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
        f"    benchmarks='',\n"
        f"    repo_image={repo_image!r}\n"
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


def _kill_labelled_containers(run_label: str) -> int:
    """Force-remove every container carrying ``fc.run=<run_label>``.

    Best-effort by design: this runs on a path that is already failing, and a
    docker hiccup here must not replace a timeout result with an exception.
    Returns how many it removed, for the log line.
    """
    try:
        listed = subprocess.run(
            ["docker", "ps", "-q", "--filter", f"label=fc.run={run_label}"],
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        ).stdout.split()
    except Exception:
        logger.warning("could not list containers for %s", run_label)
        return 0
    removed = 0
    for cid in listed:
        try:
            if (
                subprocess.run(["docker", "rm", "-f", cid], capture_output=True, check=False, timeout=120).returncode
                == 0
            ):
                removed += 1
        except Exception:
            logger.warning("could not remove container %s for %s", cid, run_label)
    if removed:
        logger.warning("verification timed out; force-removed %d container(s) for %s", removed, run_label)
    return removed


def verify_context(
    owner: str,
    repo: str,
    sha: str,
    repo_image: str,
    env_payload: str,
    python_version: str,
    context: DockerContext,
    timeout_s: int = DATASMITH_VERIFY_TIMEOUT_S,
    base_sha: str = "",
    solution_patch: str = "",
    run_tests_gate: bool = True,
) -> SandboxResult:
    """Build and verify a :class:`DockerContext` without launching an agent.

    Used by ``Synthesizer.TRY_SIMILAR`` to test whether a previously
    successful build context works for a new commit in the same repository.

    ``run_tests_gate=False`` builds the image and seals the manifest but does
    NOT fail on pytest's exit code. PRODUCE_VERIFY needs that: in its path
    pytest runs only in the verifier's battery and severity.py grades the
    verdict. Leaving the gate on there would reject a container before the
    verifier could weigh it, which is the contradiction the design settled.
    Defaults to True so TRY_SIMILAR and TRY_DEFAULT are unchanged.
    """
    start = time.time()
    docker_templates = Path(__file__).parents[1] / "docker" / "templates"

    # Use base_sha for Docker checkout so the repo is at the
    # pre-optimization state; fall back to merge_commit_sha for compat.
    checkout_sha = base_sha or sha

    with tempfile.TemporaryDirectory(prefix="verify-ctx-") as tmpdir:
        workspace = Path(tmpdir)
        task_dir = workspace / "task"
        task_dir.mkdir(parents=True, exist_ok=True)

        # Copy template files
        for fname in (
            "Dockerfile.pr",
            "docker_build_base.sh",
            "docker_build_env.sh",
            "docker_build_pkg.sh",
            "docker_build_run.sh",
            "docker_build_final.sh",
            "emit_manifest.py",
            "measure.sh",
            "apply_oracle_patch.py",
            "emit_measure.py",
            "profile.sh",
            "entrypoint.sh",
        ):
            src = docker_templates / fname
            if src.exists():
                shutil.copy2(str(src), str(task_dir / fname))

        # lsv_init.py / lsv_measure.py / parser.py are shared with the harbor
        # trial path — copied, never forked, so a change to LSV selection
        # affects stage 6 and stage 7 identically.
        lsv_templates = Path(__file__).parents[1] / "harbor_adapter" / "template"
        for fname in ("lsv_init.py", "lsv_measure.py", "parser.py"):
            src = lsv_templates / fname
            if src.exists():
                shutil.copy2(str(src), str(task_dir / fname))

        # Render run-tests.sh from Jinja2 template
        run_tests_sh = _render_run_tests_sh(docker_templates, base_commit=checkout_sha)
        (task_dir / "run-tests.sh").write_text(run_tests_sh)

        # Write task.txt — use checkout_sha so Dockerfile.pr checks out
        # the base commit, not the merge commit.
        task_txt = _generate_task_txt(owner, repo, checkout_sha, env_payload, python_version, repo_image)
        (task_dir / "task.txt").write_text(task_txt)

        # The oracle patch, mounted read-only into the measure container by
        # local_ci.py.  Always written — even empty — because `docker run -v`
        # creates a DIRECTORY at a mount source that does not exist, which
        # apply_oracle_patch.py then cannot read.  It is deliberately NOT a
        # Dockerfile.pr COPY target: a published image carrying the oracle
        # solution would be readable by the agent under evaluation.
        (task_dir / "solution.patch").write_text(solution_patch or "")

        # Override with the candidate context's editable scripts
        if context.build_pkg_sh:
            (task_dir / "docker_build_pkg.sh").write_text(context.build_pkg_sh)
        if context.build_run_sh:
            (task_dir / "docker_build_run.sh").write_text(context.build_run_sh)

        # Copy local_ci.py
        src_verify = _TEMPLATES_DIR / "local_ci.py"
        shutil.copy2(str(src_verify), str(workspace / "local_ci.py"))

        # Run local_ci.py directly (no agent)
        skip_gate = [] if run_tests_gate else ["--skip-test-gate"]
        local_ci_argv = [sys.executable, str(workspace / "local_ci.py"), "--task", str(task_dir), *skip_gate]
        # Tag every container this verification starts, so a timeout can kill
        # them. Killing local_ci.py does NOT stop its containers -- they run in
        # the daemon, and local_ci deliberately omits `--rm` so it can collect
        # metrics after exit. Its own cleanup never runs when it is killed from
        # here, which is how containers survived 90+ minutes against a 3600 s
        # timeout on 2026-08-25/26 and put the host at load 372 on 128 cores.
        run_label = f"verify-{uuid.uuid4().hex[:12]}"
        env = {**os.environ, "FC_RUN_LABEL": run_label}
        try:
            proc = subprocess.run(
                local_ci_argv,
                capture_output=True,
                text=True,
                timeout=timeout_s,
                env=env,
            )
            output = proc.stdout
        except subprocess.TimeoutExpired:
            _kill_labelled_containers(run_label)
            return SandboxResult(
                success=False,
                failure_json={
                    "stage": "timeout",
                    "return_code": 124,
                    "error_message": f"Verification timed out after {timeout_s}s",
                },
                duration_s=time.time() - start,
                agent_output=f"Timed out after {timeout_s}s",
            )

        # Read results
        success_file = task_dir / "verification_success.json"
        failure_file = task_dir / "failure.json"

        success = success_file.exists()

        failure_json: dict | None = None
        if failure_file.exists():
            try:
                failure_json = json.loads(failure_file.read_text())
            except Exception:
                logger.debug("Failed to parse failure.json in verify_context")

        resource_metrics = _extract_resource_metrics(success_file, failure_file, failure_json)
        build_manifest = _extract_build_manifest(success_file, failure_file, failure_json)
        tag = _extract_image_tag(success_file)

        return SandboxResult(
            success=success,
            docker_context=context if success else None,
            failure_json=failure_json,
            duration_s=time.time() - start,
            agent_output=output,
            resource_metrics=resource_metrics,
            build_manifest=build_manifest,
            image_tag=tag,
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
