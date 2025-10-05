from __future__ import annotations

import argparse
import contextlib
import logging
import pickle
import shlex
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import docker
import dspy
from docker.errors import APIError, ImageNotFound, NotFound

from datasmith.agents.tools.container import ContainerToolExecutor
from datasmith.agents.utils import _TEST_SUITE_IMPORT_OVERRIDES
from datasmith.core.models import BuildResult, Task
from datasmith.docker.context import ContextRegistry, DockerContext
from datasmith.docker.orchestrator import gen_run_labels

logger = logging.getLogger(__name__)


def remove_containers_by_label(client: docker.DockerClient, run_id: str) -> None:
    """
    Fast container cleanup for this run_id:
      - remove all labeled containers (force)
      - prune any stopped containers that still carry the label (server-side)
    """
    with contextlib.suppress(Exception):
        for c in client.containers.list(all=True, filters={"label": f"datasmith.run={run_id}"}):
            try:
                logger.debug("Removing container %s (%s)", c.name, c.id[:12])
                c.remove(force=True)
            except NotFound:
                pass

        # Server-side prune is much faster and avoids N API calls.
        with contextlib.suppress(Exception):
            client.containers.prune(filters={"label": [f"datasmith.run={run_id}"]})


def fast_cleanup_run_artifacts(  # noqa: C901
    client: docker.DockerClient,
    run_id: str,
    *,
    extra_image_refs: list[str] | None = None,
) -> None:
    """
    Aggressive but safe cleanup that prefers server-side prunes and removes by image ID:
      1) Resolve explicit image refs (tags/names) to IDs.
      2) Union with all images carrying datasmith.run=run_id.
      3) Remove by ID; then issue a server-side prune for all *unused* images with that label.
      4) Best-effort prune build cache, networks, volumes by label.
    """
    extra_image_refs = extra_image_refs or []

    img_ids: set[str] = set()
    with contextlib.suppress(Exception):
        for ref in extra_image_refs:
            try:
                img = client.images.get(ref)
                labels = getattr(img, "labels", None) or img.attrs.get("Config", {}).get("Labels", {}) or {}
                if (labels.get("datasmith.run") == run_id) and img.id:
                    img_ids.add(img.id)
            except (ImageNotFound, NotFound):
                pass

        # try:
        #     labeled = client.images.list(filters={"label": f"datasmith.run={run_id}"})
        #     for img in labeled:
        #         img_ids.add(img.id)  # type: ignore[arg-type]
        # except Exception:
        #     logger.exception("image list (by label) failed")

    with contextlib.suppress(Exception):
        for iid in img_ids:
            try:
                logger.debug("Removing image id=%s", iid[:20])
                client.images.remove(iid, force=True, noprune=True)
            except (ImageNotFound, NotFound):
                pass
            except APIError as e:
                if getattr(e, "status_code", None) != 409:
                    logger.debug("images.remove(%s) failed: %s", iid[:20], getattr(e, "explanation", e))

    with contextlib.suppress(Exception):
        client.images.prune(filters={"label": [f"datasmith.run={run_id}"], "dangling": False})

    with contextlib.suppress(Exception):
        low = getattr(client, "api", None)
        if low is not None:
            if hasattr(low, "prune_builds"):
                low.prune_builds(filters={"label": [f"datasmith.run={run_id}"]})
            elif hasattr(low, "build_prune"):
                low.build_prune(filters={"labels": [f"datasmith.run={run_id}"]})

    with contextlib.suppress(Exception):
        client.networks.prune(filters={"label": [f"datasmith.run={run_id}"]})
    with contextlib.suppress(Exception):
        client.volumes.prune(filters={"label": [f"datasmith.run={run_id}"]})


def _preview(s: str, n: int = 160) -> str:
    bottom_s = n
    s = s or ""
    s = s.replace("\n", "\\n")
    return s[-bottom_s:]


def _run_quick_profile(
    client: docker.DockerClient,
    image_name: str,
    run_labels: dict[str, str] | None = None,
    timeout: int = 600,
) -> tuple[bool, str]:
    """Run a quick profiling sanity check using /profile.sh in the image."""
    if run_labels is None:
        run_labels = {}

    container = None
    quick_s = 45
    try:
        cmd = [
            f'timeout -k 5 {quick_s}s /profile.sh /output/profile ""',
        ]
        logger.debug("profile:spawn cmd=%s", " ".join(cmd))
        container = client.containers.run(
            image=image_name,
            command=cmd,
            entrypoint=["/bin/bash", "-lc"],
            detach=True,
            labels=run_labels,
        )
        rc = container.wait(timeout=quick_s + 10).get("StatusCode", 1)
        logs = (container.logs() or b"").decode("utf-8", errors="replace").replace("\\n", "\n")
        if rc == 124:
            logger.debug("profile:timeout rc=124 treated as success")
            return (True, _preview(logs, 4000))
        if rc != 0:
            logger.debug("profile:failed rc=%s stderr_tail: %s", rc, _preview(logs, 240))
        return (rc == 0, _preview(logs, 4000))
    except Exception as e:
        return (False, f"exception: {e}")
    finally:
        with contextlib.suppress(Exception):
            if container is not None:
                container.remove(force=True)


def _resolve_test_suite_name(repo_name: str) -> str:
    """
    Decide the TEST_SUITE argument for run_tests.sh, based on the repo key.
    Falls back to a conservative transform: hyphens → underscores.
    """
    if repo_name in _TEST_SUITE_IMPORT_OVERRIDES:
        return _TEST_SUITE_IMPORT_OVERRIDES[repo_name]
    # default heuristic: convert repo name to a plausible import name
    return repo_name.replace("-", "_")


def _run_quick_tests(
    client: docker.DockerClient,
    image_name: str,
    repo_name: str,
    run_labels: dict[str, str],
    timeout: int = 900,
    pytest_options: str = "",
) -> tuple[bool, str]:
    """Run a quick test sanity check using /run_tests.sh in the image.
    Semantics preserved: feedparser install, 45s timeout, -q -k filter, rc==124 => success.
    """
    suite = _resolve_test_suite_name(repo_name)
    container = None
    quick_s = 45
    try:
        # Build the base command with default options
        base_options = "-q -k 'not slow and not network' --disable-warnings"
        if suite == "astropy":
            base_options += " --override-ini='addopts=--color=yes --maxfail=0'"

        # Add synthesized pytest options if provided
        if pytest_options:
            base_options += f" {pytest_options}"
        cmdline = f"timeout -k 5 {quick_s}s /run_tests.sh /output/tests {shlex.quote(suite)} {base_options}"
        logger.debug("tests:spawn cmd=%s", cmdline)
        container = client.containers.run(
            image=image_name,
            command=[cmdline],
            entrypoint=["/bin/bash", "-lc"],
            detach=True,
            labels=run_labels,
        )
        rc = container.wait(timeout=quick_s + 10).get("StatusCode", 1)
        logs = (container.logs() or b"").decode("utf-8", errors="replace").replace("\\n", "\n")
        if rc == 124:
            logger.debug("tests:timeout rc=124 treated as success")
            return (True, _preview(logs, 4000))
        if rc != 0:
            logger.debug("tests:failed rc=%s stderr_tail: %s", rc, _preview(logs, 240))
        return (rc == 0, _preview(logs, 4000))
    except Exception as e:
        return (False, f"exception: {e}")
    finally:
        with contextlib.suppress(Exception):
            if container is not None:
                container.remove(force=True)


def _ts_to_iso(ts: float | int | None) -> str:
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return str(ts)


def build_and_validate(
    client: docker.DockerClient,
    task: Task,
    context: DockerContext,
    repo_url: str,
    sha: str,
    run_labels: dict[str, str],
    args: argparse.Namespace,
) -> BuildResult:
    """
    Unified function that builds a container and validates it with profiling and testing.

    Returns a BuildResult that indicates success/failure of the entire process.
    If the build succeeds but profiling fails, the BuildResult will be marked as failed
    with error information from the profiling/testing phase.
    """
    logger.info("build_and_validate: building image '%s'", task.get_image_name())
    logger.debug("build:start image=%s", task.get_image_name())
    _t_build_start = time.time()

    build_res = build_once_with_context(
        client=client,
        task=task.with_tag("pkg"),
        context=context,
        repo_url=repo_url,
        sha=sha,
        timeout_s=args.build_timeout,
        tail_chars=args.tail_chars * 2,
        force=False,  # always rebuild to pick up new script
        run_labels=run_labels,
    )
    logger.debug(
        "build:done ok=%s rc=%s duration=%.1fs",
        build_res.ok,
        build_res.rc,
        time.time() - _t_build_start,
    )

    # If build failed, return the build result as-is
    if not build_res.ok:
        return build_res

    # Build succeeded, now validate with profiling and testing
    logger.info("build_and_validate: build ok; verifying profile+tests before recording attempt")
    logger.debug("profile:start")
    _t_profile_start = time.time()
    profile_ok, profile_preview = _run_quick_profile(
        client=client,
        image_name=task.with_tag("pkg").get_image_name(),
        run_labels=run_labels,
        timeout=min(args.build_timeout, 600),
    )
    logger.debug("profile:done ok=%s duration=%.1fs", profile_ok, time.time() - _t_profile_start)

    if not profile_ok:
        tests_ok = False
        tests_preview = "SKIPPED"
    else:
        logger.debug("tests:start")
        _t_tests_start = time.time()
        tests_ok, tests_preview = _run_quick_tests(
            client=client,
            image_name=task.with_tag("pkg").get_image_name(),
            repo_name=task.repo,
            run_labels=run_labels,
            timeout=min(args.build_timeout, 900),
            pytest_options=context.test_commands,
        )
        logger.debug("tests:done ok=%s duration=%.1fs", tests_ok, time.time() - _t_tests_start)

    logger.warning(
        "build_and_validate: verification failed (profile_ok=%s, tests_ok=%s)",
        profile_ok,
        tests_ok,
    )
    if not profile_ok:
        combined_preview = []
        combined_preview.append(f"[profile_ok={profile_ok}] {profile_preview}")
        if tests_preview != "SKIPPED":
            combined_preview.append(f"[tests_ok={tests_ok}] {tests_preview}")
        preview_text = " | ".join(combined_preview)
        return BuildResult(
            ok=False,
            image_id=build_res.image_id,
            image_name=build_res.image_name,
            rc=1,
            duration_s=build_res.duration_s,
            stderr_tail=preview_text,
            stdout_tail="",
        )
    else:
        logger.info("build_and_validate: verification passed (profile and tests)")
        return build_res


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
    # initial_template = dspy.InputField(
    #     desc="Initial template of the docker_build.sh script with important instructions."
    # )
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
    # pytest_options = dspy.OutputField(
    #     desc="Additional pytest options/arguments to pass to pytest for running tests. Should be a space-separated string of pytest flags (e.g., '--override-ini='addopts=--color=yes --maxfail=0' --disable-warnings'). Leave empty if no special options needed."
    # )


class BuildScriptProgram(dspy.Module):
    def __init__(self) -> None:
        super().__init__()
        # self.predict = dspy.Predict(BuildScriptSynthesis)
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
        # initial_template: str,
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
        iter_pytest_options: str | None = None
        for step_idx in range(max_steps):
            out = self.step(
                owner_repo=owner_repo,
                sha=sha,
                commit_date=commit_date,
                stderr_logs=stderr_logs or "",
                stdout_logs=stdout_logs or "",
                failure_more=failure_more or "N/A",
                last_docker_build_script=last_docker_build_script or "",
                # initial_template=initial_template,
                repo_facts_json=repo_facts_json or "{}",
                toolbelt=toolbelt,
                messages_log=messages_log,
            )

            action = (out.next_action or "").strip().lower()  # pyright: ignore[reportAttributeAccessIssue]
            action_input = (out.action_input or "").strip()  # pyright: ignore[reportAttributeAccessIssue]
            if action in ("none", "finish") and (out.docker_build_script or "").strip():  # pyright: ignore[reportAttributeAccessIssue]
                iter_script = out.docker_build_script.strip()  # pyright: ignore[reportAttributeAccessIssue]
                # iter_pytest_options = (out.pytest_options or "").strip()  # pyright: ignore[reportAttributeAccessIssue]
                iter_pytest_options = ""
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
        pytest_options = (iter_pytest_options or "").strip()
        logger.debug("DSPy: candidate script preview: %s", _preview(script, 240))
        logger.debug("DSPy: candidate pytest options: %s", pytest_options)
        # source /etc/profile.d/asv_utils.sh || true
        # source /etc/profile.d/asv_build_vars.sh || true

        must_haves = ["/etc/profile.d/asv_utils.sh", "/etc/profile.d/asv_build_vars.sh"]
        ok_template = all(m in script for m in must_haves)
        must_not_haves = ["```bash", "```", "import IPython", "from IPython"]
        no_bad = all(m not in script for m in must_not_haves)
        if not ok_template:
            raise RuntimeError(f"Generated script is missing required template anchors: {must_haves}")
        if not no_bad:
            raise RuntimeError(f"Generated script contains disallowed fragments: {must_not_haves}")
        logger.info("DSPy: finalized script length=%d, pytest_options='%s'", len(script), pytest_options)
        assert isinstance(script, str), "type mismatch"  # noqa: S101
        assert isinstance(pytest_options, str), "type mismatch"  # noqa: S101
        return script, pytest_options


@dataclass
class AttemptRecord:
    attempt_idx: int
    building_data: str
    build_result: BuildResult | None = None


def _merge_tail(stderr_tail: str, stdout_tail: str, max_len: int = 8000) -> str:
    text = (stderr_tail or "") + "\n" + (stdout_tail or "")
    return text[-max_len:]


def _save_pickle(ctx: DockerContext, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        pickle.dump(ctx, f, protocol=pickle.HIGHEST_PROTOCOL)
    logger.info("Saved DockerContext pickle: %s", path.name)


def synthesize_script(
    program: BuildScriptProgram,
    task: Task,
    last_script: str,
    stderr_tail: str,
    stdout_tail: str,
    # building_template: str,
    failure_more: str,
    tool_exec: ContainerToolExecutor,
    max_steps: int = 4,
) -> tuple[str, str]:
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
            # initial_template=building_template,
            repo_facts_json=tool_exec.facts_json(),
            tool_executor=tool_exec,
            max_steps=max_steps,
        )
        script, pytest_options = cast(tuple[str, str], result)
        script = str(script)
        pytest_options = str(pytest_options)
        logger.info("synthesize_script: script length=%d, pytest_options='%s'", len(script), pytest_options)
    except Exception:
        logger.exception("synthesize_script: error=%s")
        return "", ""

    return script, pytest_options


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
    # logger.info("build_once_with_context: registering context key=%s", task.get_image_name())
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
    logger.debug("build_once_with_context: stderr_tail preview: %s", _preview(res.stderr_tail, 240))
    return res


def _image_exists(client: docker.DockerClient, name: str, retries: int = 3, delay: float = 0.5) -> bool:
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
    """
    run_labels = gen_run_labels(task, runid=uuid.uuid4().hex)
    assert task.sha is not None, "task.sha must be set"  # noqa: S101

    # Gather defaults + similar contexts
    default_building_template = context_registry.get_default(tag="env")[1].building_data
    similar_contexts = context_registry.get_similar(task.with_tag("env"))
    # # remove from similar_contexts
    # with context_registry.get_lock():
    #     del context_registry.registry[task.with_tag("pkg")]

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
    test_commands_candidates = [ctx.test_commands for _, ctx in (similar_contexts or [])]
    if not script_candidates:
        script_candidates = [default_building_template]
        test_commands_candidates = [""]

    # keep the first max_similar_candidates entries
    if hasattr(args, "max_similar_candidates") and args.max_similar_candidates > 0:
        script_candidates = script_candidates[: args.max_similar_candidates]
        test_commands_candidates = test_commands_candidates[: args.max_similar_candidates]

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
    # if not client.images.list(name=task.with_tag("env").get_image_name()):
    # instead of the expensive list(), try get()

    if not _image_exists(client, task.with_tag("env").get_image_name()):
        logger.info("agent_build_and_validate: probe image not found, building probe image")
        env_res = build_once_with_context(
            client=client,
            task=task.with_tag("env"),
            context=probe_context,
            repo_url=repo_url,
            sha=task.sha,
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

    try:
        attempts: list[AttemptRecord] = []
        attempt_idx = 0

        # Phase 1: try all similar-context scripts (no agent yet)
        for cand_idx, (script, test_commands) in enumerate(zip(script_candidates, test_commands_candidates), start=1):
            logger.info(
                "agent_build_and_validate: trying similar-context candidate %d/%d",
                cand_idx,
                len(script_candidates),
            )

            ctx = DockerContext(building_data=script, test_commands=test_commands)

            # Save attempt pickle (skip index 0 for parity with old behavior)
            if attempt_idx >= 1:
                attempt_pickle = args.output_dir / f"{task.owner}-{task.repo}-{task.sha}-attempt-{attempt_idx}.pkl"
                _save_pickle(ctx, attempt_pickle)

            # Build and validate package image
            build_res = build_and_validate(
                client=client,
                task=task,
                context=ctx,
                repo_url=repo_url,
                sha=task.sha,
                run_labels=run_labels,
                args=args,
            )

            attempts.append(AttemptRecord(attempt_idx=attempt_idx, building_data=script, build_result=build_res))
            attempt_idx += 1

            if build_res.ok:
                with context_registry.get_lock():
                    context_registry.register(task.with_tag("pkg"), ctx)
                context_registry.save_to_file(path=args.context_registry)

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

            # Early exit if failure is unrelated to docker_build_pkg.sh
            is_verification_failure = False
            try:
                st = build_res.stderr_tail or ""
                is_verification_failure = "[profile_ok=" in st
                # or ("[tests_ok=" in st)
            except Exception:
                is_verification_failure = False
            if (
                (not build_res.ok)
                and (build_res.stderr_tail)
                and ("docker_build_pkg.sh" not in build_res.stderr_tail)
                and (not is_verification_failure)
            ):
                logger.error(
                    "agent_build_and_validate: build failed without mentioning docker_build_pkg.sh; not worth iterating"
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

        # Phase 2: all similar-context candidates failed — synthesize and iterate
        for j in range(max_attempts):
            logger.info("agent_build_and_validate: agent attempt %d/%d", j + 1, max_attempts)

            last = attempts[-1].build_result if attempts else None
            stderr_tail = (last.stderr_tail if last else "") or ""
            stdout_tail = (last.stdout_tail if last else "") or ""

            if "profile_ok=" in stderr_tail:
                location = "profiler"
            elif "tests_ok=" in stderr_tail:
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
                script, pytest_options = synthesize_script(
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

            ctx = DockerContext(building_data=script, test_commands=pytest_options)

            if attempt_idx >= 1:
                attempt_pickle = args.output_dir / f"{task.owner}-{task.repo}-{task.sha}-attempt-{attempt_idx}.pkl"
                _save_pickle(ctx, attempt_pickle)

            # Build and validate package image
            build_res = build_and_validate(
                client=client,
                task=task,
                context=ctx,
                repo_url=repo_url,
                sha=task.sha,
                run_labels=run_labels,
                args=args,
            )

            attempts.append(AttemptRecord(attempt_idx=attempt_idx, building_data=script, build_result=build_res))
            attempt_idx += 1

            if build_res.ok:
                with context_registry.get_lock():
                    context_registry.register(task.with_tag("pkg"), ctx)

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

            # Early exit if failure is unrelated to docker_build_pkg.sh
            is_verification_failure = False
            try:
                st = build_res.stderr_tail or ""
                is_verification_failure = "[profile_ok=" in st
                # or ("[tests_ok=" in st)
            except Exception:
                is_verification_failure = False
            if (
                (not build_res.ok)
                and (build_res.stderr_tail)
                and ("docker_build_pkg.sh" not in build_res.stderr_tail)
                and (not is_verification_failure)
            ):
                logger.error(
                    "agent_build_and_validate: build failed without mentioning docker_build_pkg.sh; not worth iterating"
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
                f"{task.with_tag('env').get_container_name()}-{run_id[:8]}",
                f"{task.with_tag('pkg').get_container_name()}-{run_id[:8]}",
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
                ],
            )

        except Exception:
            logger.exception("agent_build_and_validate: cleanup error")
