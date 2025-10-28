"""Agent-based build script synthesis and validation.

This module contains the AI agent logic for synthesizing build scripts and iteratively
validating containers. It uses DSPy for LLM-powered script generation.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import pickle
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import docker
import dspy
from docker.errors import ImageNotFound, NotFound

from datasmith.agents.tools.container import ContainerToolExecutor
from datasmith.core.models import BuildResult, Task
from datasmith.docker.cleanup import fast_cleanup_run_artifacts, remove_containers_by_label
from datasmith.docker.context import ContextRegistry, DockerContext
from datasmith.docker.orchestrator import gen_run_labels
from datasmith.docker.validation import DockerValidator, ValidationConfig
from datasmith.execution.resolution.task_utils import resolve_task

logger = logging.getLogger(__name__)
RE_PY_EXTRACT = re.compile(r"python[=<>!~]*([\d\.]+)")


@dataclass
class AttemptRecord:
    """Record of a single build attempt with its result."""

    attempt_idx: int
    building_data: str
    build_result: BuildResult | None = None


def _ts_to_iso(ts: float | int | None) -> str:
    """Convert Unix timestamp to ISO format string."""
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return str(ts)


def _merge_tail(stderr_tail: str, stdout_tail: str, max_len: int = 8000) -> str:
    """Merge stderr and stdout tails into a single log."""
    text = (stderr_tail or "") + "\n" + (stdout_tail or "")
    return text[-max_len:]


def _save_pickle(ctx: DockerContext, path: Path) -> None:
    """Save a DockerContext to a pickle file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(ctx, f, protocol=pickle.HIGHEST_PROTOCOL)
    logger.info("Saved DockerContext pickle: %s", path.name)


def _image_exists(client: docker.DockerClient, name: str, retries: int = 3, delay: float = 0.5) -> bool:
    """Check if a Docker image exists with retries."""
    for i in range(retries):
        try:
            client.images.get(name)
            return True  # noqa: TRY300
        except ImageNotFound:
            return False
        except Exception:
            if i == retries - 1:
                raise
            time.sleep(delay * (2**i))
    return False


class BuildScriptAgentStep(dspy.Signature):
    """
    An interactive planner for producing a bash script (docker_build.sh) to build and install a Python repo inside micromamba envs. It can either:
      (A) request a TOOL call (probe_repo, list_tree, read_file, try_import) with JSON args, or
      (B) output the final script.
    If you need a tool, set next_action to one of: 'probe_repo' | 'list_tree' | 'read_file' | 'try_import' | 'none'.
    For read_file, provide JSON like {"path": "...", "max_bytes": 65536}.
    For list_tree, provide JSON like {"depth": 2}.
    For try_import, provide JSON like {"candidates": ["foo", "bar"]}.
    Return docker_build_script ONLY when you're satisfied.

    Respect these constraints:
        - The script MUST be idempotent and safe to run in Docker.
        - No user prompts, all non-interactive.
        - Do not surround with Markdown tags like ```bash ... ```.
        - CRITICAL: Scripts MUST use actual newline characters, NOT escaped \\n sequences.
        - CRITICAL: Scripts MUST be valid executable bash without syntax errors.
        - CRITICAL: Do NOT output literal backslash-n (\\n) in the script - use actual line breaks.
        - Critically: After editable install, the environment must be READY for quick verification:
          - A lightweight profiling sanity check should be able to start (not error immediately) with the project importable.
          - A lightweight pytest sanity check should be able to start (not error immediately), even for projects that require running from a subdirectory (e.g., SciPy).
          - Install test/benchmark extras as needed (e.g., pyproject optional-dependencies, dev/test requirements) so that import and minimal pytest discovery succeed.
    """

    # Inputs (context)
    owner_repo = dspy.InputField(desc="The repository this commit belongs to. E.g. 'scikit-learn/scikit-learn'.")
    sha = dspy.InputField(desc="The commit SHA that is currently checked out.")
    commit_date = dspy.InputField(desc="The commit date in ISO format, e.g. '2023-10-05T12:34:56Z'.")
    stderr_logs = dspy.InputField(
        desc="The most recent stderr logs from the last build attempt. Upto ~8k tail-end chars."
    )
    stdout_logs = dspy.InputField(
        desc="The most recent stdout logs from the last build attempt. Upto ~8k tail-end chars."
    )
    failure_more = dspy.InputField(
        desc="Describes where the failure occured. E.g. 'N/A', 'build failed', 'asv run failed'."
    )
    last_docker_build_script = dspy.InputField(desc="Previous docker_build.sh script.")
    repo_facts_json = dspy.InputField(desc="Some inferred repo facts (A JSON object with paths, candidates, versions).")
    toolbelt = dspy.InputField(desc="Human-readable summary of available tools.")
    messages_log = dspy.InputField(desc="Transcript of prior tool actions & observations.")

    # Outputs
    thought = dspy.OutputField(desc="Brief rationale.")
    next_action = dspy.OutputField(desc="One of probe_repo|list_tree|read_file|try_import|none|finish.")
    action_input = dspy.OutputField(desc="JSON args for the tool (or empty).")
    error_summary = dspy.OutputField(desc="A brief summary of the last build failure, and possible causes.")
    resolution_steps = dspy.OutputField(desc="Concrete steps to resolve the failure.")
    docker_build_script = dspy.OutputField(
        desc="Final executable bash script that successfully builds the project from source."
    )


class BuildScriptProgram(dspy.Module):
    """DSPy program for synthesizing build scripts using an agent-based approach."""

    def __init__(self) -> None:
        super().__init__()
        self.step = dspy.Predict(BuildScriptAgentStep)

    def _toolbelt_text(self) -> str:
        return (
            "Tools you can use:\n"
            "- probe_repo(): recompute repo facts (asv dir, pyproject, setup, pkg candidates, python versions).\n"
            "- list_tree(depth=2): show a trimmed top-level tree for orientation.\n"
            "- read_file(path, max_bytes=65536): read a file at this commit.\n"
            "- try_import(candidates=[...]): (post-build) quick python import check inside the built image.\n"
            "- exec_arbitrary(command): run arbitrary shell command in the checked-out repo (careful!).\n"
            "- none/finish + docker_build_script: when you are satisfied, return the final build script in the docker_build_script field.\n"
            "Hints:\n"
            "- facts_json contains installed_packages (by Python version) exported by build_env; use it to avoid redundant installs, to spot missing test deps, to downgrade or upgrade packages, etc."
        )

    def forward(
        self,
        owner_repo: str,
        sha: str,
        commit_date: str,
        stderr_logs: str,
        stdout_logs: str,
        failure_more: str,
        last_docker_build_script: str,
        repo_facts_json: str,
        tool_executor: ContainerToolExecutor,
        max_steps: int = 4,
    ) -> tuple[str, str]:
        logger.info(
            "DSPy: synthesizing build script for %s@%s (stderr_len=%d, stdout_len=%d, has_last=%s, failure=%s)",
            owner_repo,
            sha,
            len(stderr_logs or ""),
            len(stdout_logs or ""),
            bool(last_docker_build_script),
            failure_more,
        )
        messages_log = ""
        toolbelt = self._toolbelt_text()
        iter_script: str | None = None
        for step_idx in range(max_steps):
            out = self.step(
                owner_repo=owner_repo,
                sha=sha,
                commit_date=commit_date,
                stderr_logs=stderr_logs or "",
                stdout_logs=stdout_logs or "",
                failure_more=failure_more or "N/A",
                last_docker_build_script=last_docker_build_script or "",
                repo_facts_json=repo_facts_json or "{}",
                toolbelt=toolbelt,
                messages_log=messages_log,
            )

            action = (out.next_action or "").strip().lower()  # pyright: ignore[reportAttributeAccessIssue]
            action_input = (out.action_input or "").strip()  # pyright: ignore[reportAttributeAccessIssue]
            if action in ("none", "finish") and (out.docker_build_script or "").strip():  # pyright: ignore[reportAttributeAccessIssue]
                iter_script = out.docker_build_script.strip()  # pyright: ignore[reportAttributeAccessIssue]
                break

            # Tool dispatch
            observation = tool_executor.choose_action(
                action=action,
                action_input=action_input,
            )
            if action == "probe_repo":
                repo_facts_json = tool_executor.facts_json()

            messages_log += f"\n\n# Step [{step_idx + 1}/{max_steps}]\n# Action: {action}\n# Input: {action_input}\n# Observation:\n{observation[:4000]}"

            if action in ("none", "finish"):
                # Model is done but didn't provide a script. Stop.
                break

        script = (iter_script or "").strip()
        logger.debug("DSPy: candidate script preview: %s", script[-240:] if script else "")

        must_haves = ["/etc/profile.d/asv_utils.sh", "/etc/profile.d/asv_build_vars.sh"]
        ok_template = all(m in script for m in must_haves)
        must_not_haves = ["```bash", "```", "import IPython", "from IPython"]
        no_bad = all(m not in script for m in must_not_haves)
        if not ok_template:
            raise RuntimeError(f"Generated script is missing required template anchors: {must_haves}")
        if not no_bad:
            raise RuntimeError(f"Generated script contains disallowed fragments: {must_not_haves}")
        logger.info("DSPy: finalized script length=%d", len(script))
        assert isinstance(script, str), "type mismatch"  # noqa: S101
        return script, messages_log


def synthesize_script(
    program: BuildScriptProgram,
    task: Task,
    last_script: str,
    stderr_tail: str,
    stdout_tail: str,
    failure_more: str,
    tool_exec: ContainerToolExecutor,
    max_steps: int = 4,
) -> str:
    """Synthesize a build script using the agent program.

    Args:
        program: BuildScriptProgram instance
        task: Task to build
        last_script: Previous build script
        stderr_tail: Stderr from last build
        stdout_tail: Stdout from last build
        failure_more: Description of failure location
        tool_exec: Tool executor for repo operations
        max_steps: Maximum agent steps

    Returns:
        Tuple of (script)
    """
    logger.info(
        "synthesize_script: task=%s/%s@%s, last_script=%s",
        task.owner,
        task.repo,
        task.sha,
        "present" if last_script else "none",
    )
    merged_log = _merge_tail(stderr_tail, stdout_tail)
    logger.debug("synthesize_script: merged_log_len=%d", len(merged_log))

    try:
        result = program(
            owner_repo=f"{task.owner}/{task.repo}",
            sha=task.sha,
            commit_date=_ts_to_iso(getattr(task, "commit_date", None)),
            stderr_logs=stderr_tail or "",
            stdout_logs=stdout_tail or "",
            failure_more=failure_more or "N/A",
            last_docker_build_script=last_script or "",
            repo_facts_json=tool_exec.facts_json(),
            tool_executor=tool_exec,
            max_steps=max_steps,
        )
        script = cast(str, result)
        script = str(script)
        logger.info("synthesize_script: raw script length=%d", len(script))

    except Exception:
        logger.exception("synthesize_script: error")
        return ""

    return script


def build_once_with_context(
    client: docker.DockerClient,
    task: Task,
    context: DockerContext,
    repo_url: str,
    sha: str,
    *,
    timeout_s: int,
    tail_chars: int,
    run_labels: dict[str, str],
    probe: bool = False,
    pull: bool = False,
    force: bool = False,
) -> BuildResult:
    """Build a Docker image once with the given context.

    Args:
        client: Docker client
        task: Task to build
        context: DockerContext with build configuration
        repo_url: Repository URL
        sha: Commit SHA
        timeout_s: Build timeout in seconds
        tail_chars: Number of tail characters to capture
        run_labels: Labels for the build
        probe: Whether this is a probe build
        pull: Whether to pull base images
        force: Whether to force rebuild

    Returns:
        BuildResult with build outcome
    """
    logger.debug(
        "build_once_with_context: build args: REPO_URL=%s, COMMIT_SHA=%s, timeout_s=%s, tail_chars=%s, pull=%s",
        repo_url,
        sha,
        timeout_s,
        tail_chars,
        pull,
    )

    res = context.build_container_streaming(
        client=client,
        image_name=task.get_image_name(),
        build_args={
            "REPO_URL": repo_url,
            "COMMIT_SHA": sha,
            "ENV_PAYLOAD": task.env_payload if len(task.env_payload) else "{}",
            "PY_VERSION": task.python_version or "",
        },
        probe=probe,
        force=force,
        timeout_s=timeout_s,
        tail_chars=tail_chars,
        pull=pull,
        run_labels=run_labels,
    )
    logger.info(
        "build_once_with_context: result ok=%s rc=%s duration=%.1fs (stderr_tail_len=%d, stdout_tail_len=%d)",
        res.ok,
        res.rc,
        res.duration_s,
        len(res.stderr_tail or ""),
        len(res.stdout_tail or ""),
    )
    logger.debug("build_once_with_context: stderr_tail preview: %s", (res.stderr_tail or "")[-240:])
    return res


# def build_and_validate(
#     client: docker.DockerClient,
#     task: Task,
#     context: DockerContext,
#     repo_url: str,
#     sha: str,
#     run_labels: dict[str, str],
#     args: argparse.Namespace,
# ) -> BuildResult:
#     """Build and validate a container (wrapper that injects build_once_with_context).

#     This is a thin wrapper around the validation module's build_and_validate
#     that injects the build_once_with_context function to avoid circular imports.

#     Args:
#         client: Docker client
#         task: Task to build and validate
#         context: DockerContext with build configuration
#         repo_url: Repository URL
#         sha: Commit SHA
#         run_labels: Labels for the container run
#         args: Command-line arguments

#     Returns:
#         BuildResult with validation outcome
#     """
#     return build_and_validate_impl(
#         client=client,
#         task=task,
#         context=context,
#         repo_url=repo_url,
#         sha=sha,
#         run_labels=run_labels,
#         args=args,
#         build_once_fn=build_once_with_context,
#     )


def agent_build_and_validate(  # noqa: C901
    task: Task,
    args: argparse.Namespace,
    client: docker.DockerClient,
    machine_defaults: dict,
    context_registry: ContextRegistry,
    max_attempts: int = 3,
) -> dict:
    """
    Main entry: iteratively try similar-context build scripts; if all fail,
    synthesize a script with the agent and iterate. Saves attempt pickles and
    final pickle on success.

    Args:
        task: Task to build
        args: Command-line arguments
        client: Docker client
        machine_defaults: Default machine configuration
        context_registry: Registry of build contexts
        max_attempts: Maximum number of synthesis attempts

    Returns:
        Dictionary with build results and attempt history
    """
    assert task.sha is not None, "task.sha must be set"  # noqa: S101
    run_labels = gen_run_labels(task, runid=uuid.uuid4().hex)
    task_analysis, task = resolve_task(task)
    if not task_analysis or not task_analysis.get("can_install", False):
        logger.warning("agent_build_and_validate: task cannot be installed")
        return {
            "ok": False,
            "rc": 1,
            "stage": "analysis",
            "owner": task.owner,
            "repo": task.repo,
            "sha": task.sha,
            "image_name": task.with_tag("pkg").get_image_name(),
            "duration_s": 0.0,
            "stderr_tail": json.dumps(task_analysis) if task_analysis else "No analysis",
            "stdout_tail": task_analysis.get("dry_run_log", "") if task_analysis else "",
            "attempts": [],
            "context_pickle": None,
        }

    # Gather defaults + similar contexts
    default_building_template = context_registry.get_default(tag="env")[1].building_data
    similar_contexts = context_registry.get_similar(task.with_tag("env"))

    # Choose an ENV context for the probe build (most similar if possible)
    if similar_contexts:
        t, probe_context = similar_contexts[0]
        logger.info(
            "build_once_with_context: found %d similar contexts; using most similar for probe with key=%s",
            len(similar_contexts),
            str(t),
        )
    else:
        _, probe_context = context_registry.get_default(tag="env")
        logger.info(
            "build_once_with_context: no similar context found; using default for probe with key=%s", str(probe_context)
        )

    # Build-package script candidates: try every similar context's building_data first.
    script_candidates = [ctx.building_data for _, ctx in (similar_contexts or [])]
    if not script_candidates:
        script_candidates = [default_building_template]

    # keep the first max_similar_candidates entries
    if hasattr(args, "max_similar_candidates") and args.max_similar_candidates > 0:
        script_candidates = script_candidates[: args.max_similar_candidates]

    logger.info(
        "agent_build_and_validate: start for %s/%s@%s (max_attempts=%d, candidates=%d)",
        task.owner,
        task.repo,
        task.sha,
        max_attempts,
        len(script_candidates),
    )

    program = BuildScriptProgram()
    repo_url = f"https://www.github.com/{task.owner}/{task.repo}"
    logger.debug("agent_build_and_validate: task=%s repo_url=%s", task, repo_url)

    # Ensure probe ENV image exists
    if not _image_exists(client, task.with_tag("env").get_image_name()):
        logger.info("agent_build_and_validate: probe image not found, building probe image")
        env_res = build_once_with_context(
            client=client,
            task=task.with_tag("env"),
            context=probe_context,
            repo_url=repo_url,
            sha=cast(str, task.sha),
            timeout_s=args.build_timeout,
            tail_chars=args.tail_chars,
            probe=True,
            pull=False,
            force=False,  # don't rebuild if already present
            run_labels=run_labels,
        )
        if not env_res.ok:
            logger.warning("agent_build_and_validate: probe build failed; something is wrong with Dockerfile")
            return {
                "ok": False,
                "rc": env_res.rc,
                "stage": "probe",
                "owner": task.owner,
                "repo": task.repo,
                "sha": task.sha,
                "image_name": task.with_tag("pkg").get_image_name(),
                "duration_s": env_res.duration_s,
                "stderr_tail": env_res.stderr_tail,
                "stdout_tail": env_res.stdout_tail,
                "attempts": [],
                "context_pickle": None,
            }

    tool_exec = ContainerToolExecutor(
        docker_client=client,
        image_name=task.with_tag("env").get_image_name(),
        container_name=task.with_tag("env").get_container_name() + f"-{run_labels.get('datasmith.run', 'run')[:8]}",
        workdir="/workspace/repo/",
        run_labels=run_labels,
    )

    validation_config = ValidationConfig(
        output_dir=args.output_dir,
        build_timeout=args.build_timeout,
        run_timeout=args.run_timeout,
        tail_chars=args.tail_chars,
    )

    validator = DockerValidator(
        client=client,
        context_registry=context_registry,
        machine_defaults=machine_defaults,
        config=validation_config,
    )

    try:
        attempts: list[AttemptRecord] = []
        attempt_idx = 0

        # Phase 1: try all similar-context scripts (no agent yet)
        for cand_idx, script in enumerate(script_candidates, start=1):
            logger.info(
                "agent_build_and_validate: trying similar-context candidate %d/%d",
                cand_idx,
                len(script_candidates),
            )

            ctx = DockerContext(building_data=script)

            # Save attempt pickle (skip index 0 for parity with old behavior)
            if attempt_idx >= 1:
                attempt_pickle = args.output_dir / f"{task.owner}-{task.repo}-{task.sha}-attempt-{attempt_idx}.pkl"
                _save_pickle(ctx, attempt_pickle)

            # Build and validate package image
            # build_res = build_and_validate(
            #     client=client,
            #     task=task,
            #     context=ctx,
            #     repo_url=repo_url,
            #     sha=sha,
            #     run_labels=run_labels,
            #     args=args,
            # )
            build_res = validator.build_and_validate(
                task=task.with_tag("run"),
                context=ctx,
                # repo_url=repo_url,
                # sha=sha,
                run_labels=run_labels,
                build_once_fn=build_once_with_context,
            )

            attempts.append(AttemptRecord(attempt_idx=attempt_idx, building_data=script, build_result=build_res))
            attempt_idx += 1

            if build_res.ok:
                with context_registry.get_lock():
                    context_registry.register(task.with_tag("pkg"), ctx)
                context_registry.save_to_file(path=args.context_registry)

                # If requested, publish the successfully built image to ECR
                if getattr(args, "push_to_ecr", False):
                    logger.info(
                        "agent_build_and_validate: pushed %s to ECR",
                        task.with_tag("run").get_image_name(),
                    )
                    ctx.build_and_publish_to_ecr(
                        client=client,
                        task=task.with_tag("run"),
                        region=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
                    )

                final_pickle = args.output_dir / f"{task.owner}-{task.repo}-{task.sha}-final.pkl"
                _save_pickle(ctx, final_pickle)

                result = attempts[-1].build_result
                if result is None:
                    raise RuntimeError("Unexpected: result is None after successful build")

                result_dict: dict[str, Any] = {
                    "owner": task.owner,
                    "repo": task.repo,
                    "sha": task.sha,
                    "image_name": task.with_tag("pkg").get_image_name(),
                    "ok": result.ok,
                    "rc": result.rc,
                    "duration_s": result.duration_s,
                    "stderr_tail": result.stderr_tail,
                    "stdout_tail": result.stdout_tail,
                    "stage": "build",
                }
                logger.info(
                    "agent_build_and_validate: validation stage=%s ok=%s rc=%s",
                    result_dict.get("stage"),
                    result_dict.get("ok"),
                    result_dict.get("rc"),
                )

                result_dict["attempts"] = [
                    {
                        "attempt": a.attempt_idx,
                        "ok": (a.build_result.ok if a.build_result else False),
                        "rc": (a.build_result.rc if a.build_result else None),
                        "stderr_tail": (a.build_result.stderr_tail if a.build_result else ""),
                        "stdout_tail": (a.build_result.stdout_tail if a.build_result else ""),
                        "building_data": a.building_data,
                    }
                    for a in attempts
                ]
                result_dict["context_pickle"] = str(final_pickle)
                return result_dict

            # Detect malformed scripts early (syntax errors, escaped newlines)
            if not build_res.ok and build_res.stderr_tail:
                stderr_lower = build_res.stderr_tail.lower()
                # Check for common script syntax errors
                if "docker_build_pkg.sh" in build_res.stderr_tail and any(
                    indicator in stderr_lower
                    for indicator in [
                        "syntax error",
                        ": not found",
                        "unexpected token",
                        "line 1:",  # Often indicates malformed script
                    ]
                ):
                    logger.error(
                        "agent_build_and_validate: detected malformed docker_build_pkg.sh script (syntax error) in candidate %d; "
                        "script likely has invalid bash syntax",
                        cand_idx,
                    )

            # Early exit if failure is unrelated to docker_build_pkg.sh or validation
            # Check failure_stage to determine if this was a validation failure (profile/tests)
            # or a build infrastructure failure
            is_verification_failure = build_res.failure_stage in ("profile", "tests")
            if (
                (not build_res.ok)
                and (build_res.stderr_tail)
                and ("docker_build_pkg.sh" not in build_res.stderr_tail)
                and (not is_verification_failure)
            ):
                logger.error(
                    "agent_build_and_validate: build failed without mentioning docker_build_pkg.sh or validation; not worth iterating"
                )
                return {
                    "ok": False,
                    "rc": build_res.rc,
                    "stage": "build",
                    "owner": task.owner,
                    "repo": task.repo,
                    "sha": task.sha,
                    "image_name": task.with_tag("pkg").get_image_name(),
                    "duration_s": build_res.duration_s,
                    "stderr_tail": build_res.stderr_tail,
                    "stdout_tail": build_res.stdout_tail,
                    "attempts": [
                        {
                            "attempt": a.attempt_idx,
                            "ok": (a.build_result.ok if a.build_result else False),
                            "rc": (a.build_result.rc if a.build_result else None),
                            "stderr_tail": (a.build_result.stderr_tail if a.build_result else ""),
                            "stdout_tail": (a.build_result.stdout_tail if a.build_result else ""),
                            "building_data": a.building_data,
                        }
                        for a in attempts
                    ],
                    "context_pickle": None,
                }

            logger.warning(
                "agent_build_and_validate: candidate %d failed (rc=%s); trying next similar context if any.",
                cand_idx,
                (build_res.rc if build_res else "unknown"),
            )

        # Phase 2: all similar-context candidates failed  synthesize and iterate
        for j in range(max_attempts):
            logger.info("agent_build_and_validate: agent attempt %d/%d", j + 1, max_attempts)

            last = attempts[-1].build_result if attempts else None
            stderr_tail = (last.stderr_tail if last else "") or ""
            stdout_tail = (last.stdout_tail if last else "") or ""

            if "[profile_ok=" in stderr_tail:
                location = "profiler"
            elif "[tests_ok=" in stderr_tail:
                location = "pytest"
            else:
                location = "build"

            if last and last.rc == 124:
                failure_more = f"{location} timeout"
            else:
                failure_more = f"{location} failed rc={last.rc}" if last else f"{location} failed"

            logger.debug(
                "agent_build_and_validate: re-synthesis with last tails (stderr_len=%d, stdout_len=%d, failure=%s, location=%s)",
                len(stderr_tail),
                len(stdout_tail),
                failure_more,
                location,
            )

            try:
                script = synthesize_script(
                    program,
                    task,
                    attempts[-1].building_data if attempts else default_building_template,
                    stderr_tail=stderr_tail,
                    stdout_tail=stdout_tail,
                    failure_more=failure_more,
                    tool_exec=tool_exec,
                    max_steps=args.max_steps,
                )
            except Exception as e:
                logger.error("agent_build_and_validate: synthesis error: %s", e, exc_info=True)
                build_res = BuildResult(
                    ok=False,
                    image_id=None,
                    image_name=task.with_tag("pkg").get_image_name(),
                    rc=1,
                    duration_s=0.0,
                    stderr_tail=str(e),
                    stdout_tail="",
                )
                attempts.append(AttemptRecord(attempt_idx=attempt_idx, building_data="", build_result=build_res))
                break  # exit agent loop

            ctx = DockerContext(building_data=script)

            if attempt_idx >= 1:
                attempt_pickle = args.output_dir / f"{task.owner}-{task.repo}-{task.sha}-attempt-{attempt_idx}.pkl"
                _save_pickle(ctx, attempt_pickle)

            # Build and validate package image
            # build_res = build_and_validate(
            #     client=client,
            #     task=task,
            #     context=ctx,
            #     repo_url=repo_url,
            #     sha=sha,
            #     run_labels=run_labels,
            #     args=args,
            # )
            build_res = validator.build_and_validate(
                task=task.with_tag("run"),
                context=ctx,
                # repo_url=repo_url,
                # sha=sha,
                run_labels=run_labels,
                build_once_fn=build_once_with_context,
            )

            attempts.append(AttemptRecord(attempt_idx=attempt_idx, building_data=script, build_result=build_res))
            attempt_idx += 1

            if build_res.ok:
                with context_registry.get_lock():
                    context_registry.register(task.with_tag("pkg"), ctx)

                if args.push_to_ecr:
                    logger.info("agent_build_and_validate: pushed %s to ECR", task.with_tag("run").get_image_name())
                    ctx.build_and_publish_to_ecr(
                        client=client,
                        task=task.with_tag("run"),
                        region=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
                    )

                final_pickle = args.output_dir / f"{task.owner}-{task.repo}-{task.sha}-final.pkl"
                _save_pickle(ctx, final_pickle)

                result = attempts[-1].build_result
                if result is None:
                    raise RuntimeError("Unexpected: result is None after successful build")

                result_dict = {
                    "owner": task.owner,
                    "repo": task.repo,
                    "sha": task.sha,
                    "image_name": task.with_tag("pkg").get_image_name(),
                    "ok": result.ok,
                    "rc": result.rc,
                    "duration_s": result.duration_s,
                    "stderr_tail": result.stderr_tail,
                    "stdout_tail": result.stdout_tail,
                    "stage": "build",
                }
                logger.info(
                    "agent_build_and_validate: validation stage=%s ok=%s rc=%s",
                    result_dict.get("stage"),
                    result_dict.get("ok"),
                    result_dict.get("rc"),
                )
                result_dict["attempts"] = [
                    {
                        "attempt": a.attempt_idx,
                        "ok": (a.build_result.ok if a.build_result else False),
                        "rc": (a.build_result.rc if a.build_result else None),
                        "stderr_tail": (a.build_result.stderr_tail if a.build_result else ""),
                        "stdout_tail": (a.build_result.stdout_tail if a.build_result else ""),
                        "building_data": a.building_data,
                    }
                    for a in attempts
                ]
                result_dict["context_pickle"] = str(final_pickle)
                return result_dict

            # Early exit if failure is unrelated to docker_build_pkg.sh or validation
            # Check failure_stage to determine if this was a validation failure (profile/tests)
            # or a build infrastructure failure
            is_verification_failure = build_res.failure_stage in ("profile", "tests")
            if (
                (not build_res.ok)
                and (build_res.stderr_tail)
                and ("docker_build_pkg.sh" not in build_res.stderr_tail)
                and (not is_verification_failure)
            ):
                logger.error(
                    "agent_build_and_validate: build failed without mentioning docker_build_pkg.sh or validation; not worth iterating"
                )
                return {
                    "ok": False,
                    "rc": build_res.rc,
                    "stage": "build",
                    "owner": task.owner,
                    "repo": task.repo,
                    "sha": task.sha,
                    "image_name": task.with_tag("pkg").get_image_name(),
                    "duration_s": build_res.duration_s,
                    "stderr_tail": build_res.stderr_tail,
                    "stdout_tail": build_res.stdout_tail,
                    "attempts": [
                        {
                            "attempt": a.attempt_idx,
                            "ok": (a.build_result.ok if a.build_result else False),
                            "rc": (a.build_result.rc if a.build_result else None),
                            "stderr_tail": (a.build_result.stderr_tail if a.build_result else ""),
                            "stdout_tail": (a.build_result.stdout_tail if a.build_result else ""),
                            "building_data": a.building_data,
                        }
                        for a in attempts
                    ],
                    "context_pickle": None,
                }

            logger.warning(
                "agent_build_and_validate: agent attempt %d failed (rc=%s). Iterating if attempts remain.",
                attempt_idx - 1,
                (build_res.rc if build_res else "unknown"),
            )

        # All attempts failed (similar-context candidates + agent tries)
        last = attempts[-1].build_result if attempts else None
        logger.error("agent_build_and_validate: all attempts failed for %s", task.with_tag("pkg").get_image_name())
        return {
            "owner": task.owner,
            "repo": task.repo,
            "sha": task.sha,
            "image_name": task.with_tag("pkg").get_image_name(),
            "stage": "build",
            "ok": False,
            "rc": (last.rc if last else 1),
            "duration_s": (last.duration_s if last else None),
            "stderr_tail": (last.stderr_tail if last else ""),
            "stdout_tail": (last.stdout_tail if last else ""),
            "attempts": [
                {
                    "attempt": a.attempt_idx,
                    "ok": (a.build_result.ok if a.build_result else False),
                    "rc": (a.build_result.rc if a.build_result else None),
                    "stderr_tail": (a.build_result.stderr_tail if a.build_result else ""),
                    "stdout_tail": (a.build_result.stdout_tail if a.build_result else ""),
                    "building_data": a.building_data,
                }
                for a in attempts
            ],
            "files": [],
        }
    finally:
        with contextlib.suppress(Exception):
            tool_exec.shutdown()

        try:
            run_id = run_labels.get("datasmith.run", "unknown")

            # 1) Containers first
            remove_containers_by_label(client, run_id)
            # Also try removing containers by their exact names once (cheap best-effort).
            for name in [
                task.with_tag("env").get_container_name(),
                task.with_tag("pkg").get_container_name(),
                task.with_tag("run").get_container_name(),
                f"{task.with_tag('env').get_container_name()}-{run_id[:8]}",
                f"{task.with_tag('pkg').get_container_name()}-{run_id[:8]}",
                f"{task.with_tag('run').get_container_name()}-{run_id[:8]}",
            ]:
                with contextlib.suppress(Exception, NotFound):
                    c = client.containers.get(name)
                    c.remove(force=True)

            # 2) Images & build cache (fast path)
            fast_cleanup_run_artifacts(
                client,
                run_id,
                extra_image_refs=[
                    task.with_tag("env").get_image_name(),
                    task.with_tag("pkg").get_image_name(),
                    task.with_tag("run").get_image_name(),
                ],
            )

        except Exception:
            logger.exception("agent_build_and_validate: cleanup error")
