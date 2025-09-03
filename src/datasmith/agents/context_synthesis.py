from __future__ import annotations

import argparse
import logging
import pickle
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import docker
import dspy
from docker.errors import NotFound

from datasmith.agents.tool_executor import ContainerToolExecutor
from datasmith.docker.context import BuildResult, ContextRegistry, DockerContext
from datasmith.docker.validation import Task

logger = logging.getLogger(__name__)


def _preview(s: str, n: int = 160) -> str:
    s = s or ""
    s = s.replace("\n", "\\n")
    return s[:n] + ("..." if len(s) > n else "")


def _ts_to_iso(ts: float | int | None) -> str:
    if ts is None:
        return ""
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return str(ts)


# class BuildScriptSynthesis(dspy.Signature):
#     """
#     Draft a bash script (docker_build.sh) to build & install a Python repo inside micromamba envs
#     discovered via asv.*.json. The script MUST be idempotent and safe to run in Docker.
#     Respect this template:
#       - discover and cd into the dir containing asv.*.json
#       - for each python version listed there:
#           * create micromamba env "asv_${version}"
#           * ensure asv + build tooling
#           * then perform project install (editable or wheel) with best-guess flags
#       - no user prompts, all non-interactive
#       - Do not surround with ```bash ... ```. Return raw bash script.
#     """

#     # Inputs
#     owner_repo = dspy.InputField(desc="The repository this commit belongs to. E.g. 'scikit-learn/scikit-learn'.")
#     sha = dspy.InputField(desc="The commit SHA that is currently checked out.")
#     commit_date = dspy.InputField(desc="The commit date in ISO format, e.g. '2023-10-05T12:34:56Z'.")
#     stderr_logs = dspy.InputField(
#         desc="The most recent stderr logs from the last build attempt. Upto ~8k tail-end chars."
#     )
#     stdout_logs = dspy.InputField(
#         desc="The most recent stdout logs from the last build attempt. Upto ~8k tail-end chars."
#     )
#     failure_more = dspy.InputField(
#         desc="Describes where the failure occured. E.g. 'N/A', 'build failed', 'asv run failed'."
#     )
#     last_docker_build_script = dspy.InputField(desc="Previous docker_build.sh script.")
#     initial_template = dspy.InputField(desc="Stable outer template..")

#     # Output
#     error_summary = dspy.OutputField(desc="A brief summary of the last build failure, and possible causes.")
#     resolution_steps = dspy.OutputField(desc="Concrete steps to resolve the failure.")
#     docker_build_script = dspy.OutputField(
#         desc="Final executable bash script that successfully builds the project from source."
#     )
# Draft a bash script (docker_build.sh) to build & install a Python repo inside micromamba envs
# discovered via asv.*.json. The script MUST be idempotent and safe to run in Docker.
# Respect this template:
#   - discover and cd into the dir containing asv.*.json
#   - for each python version listed there:
#       * create micromamba env "asv_${version}"
#       * ensure asv + build tooling
#       * then perform project install (editable or wheel) with best-guess flags
#   - no user prompts, all non-interactive
#   - Do not surround with ```bash ... ```. Return raw bash script.


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
    initial_template = dspy.InputField(
        desc="Initial template of the docker_build.sh script with important instructions."
    )
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
        initial_template: str,
        repo_facts_json: str,
        tool_executor: ContainerToolExecutor,
        max_steps: int = 4,
    ) -> str:
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
                initial_template=initial_template,
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

            # Don't prefer build_script until model is completely done with it.
            # # If model already emitted a script, prefer it
            # if (out.docker_build_script or "").strip():  # pyright: ignore[reportAttributeAccessIssue]
            #     iter_script = out.docker_build_script.strip()  # pyright: ignore[reportAttributeAccessIssue]
            #     break

        # out = self.predict(
        #     owner_repo=owner_repo,
        #     sha=sha,
        #     commit_date=commit_date,
        #     stderr_logs=stderr_logs or "",
        #     stdout_logs=stdout_logs or "",
        #     failure_more=failure_more or "N/A",
        #     last_docker_build_script=last_docker_build_script or "",
        #     initial_template=initial_template,
        # )
        # Safety belt: ensure the required fixed template anchors are present.
        # script = out.docker_build_script.strip()  # pyright: ignore[reportAttributeAccessIssue]
        script = (iter_script or "").strip()
        logger.debug("DSPy: candidate script preview: %s", _preview(script, 240))
        must_haves = ["###### SETUP CODE (NOT TO BE MODIFIED) ######"]
        ok_template = all(m in script for m in must_haves)
        must_not_haves = ["```bash", "```", "import IPython", "from IPython"]
        no_bad = all(m not in script for m in must_not_haves)
        if not ok_template:
            raise RuntimeError(f"Generated script is missing required template anchors: {must_haves}")
        if not no_bad:
            raise RuntimeError(f"Generated script contains disallowed fragments: {must_not_haves}")
        logger.info("DSPy: finalized script length=%d", len(script))
        assert isinstance(script, str), "type mismatch"  # noqa: S101
        return script


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
    building_template: str,
    failure_more: str,
    tool_exec: ContainerToolExecutor,
    max_steps: int = 4,
) -> str:
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
        script = program(
            owner_repo=f"{task.owner}/{task.repo}",
            sha=task.sha,
            commit_date=_ts_to_iso(getattr(task, "commit_date", None)),
            stderr_logs=stderr_tail or "",
            stdout_logs=stdout_tail or "",
            failure_more=failure_more or "N/A",
            last_docker_build_script=last_script or "",
            initial_template=building_template,
            repo_facts_json=tool_exec.facts_json(),
            tool_executor=tool_exec,
            max_steps=max_steps,
        )
        script = str(script)
        logger.info("synthesize_script: script length=%d", len(script))
    except Exception:
        logger.exception("synthesize_script: error=%s")
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
    probe: bool = False,
    pull: bool = False,
    force: bool = True,
) -> BuildResult:
    logger.info("build_once_with_context: registering context key=%s", task.get_image_name())
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
        build_args={"REPO_URL": repo_url, "COMMIT_SHA": sha},
        probe=probe,
        force=force,
        timeout_s=timeout_s,
        tail_chars=tail_chars,
        pull=pull,
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


def agent_build_and_validate(  # noqa: C901
    task: Task,
    args: argparse.Namespace,
    client: docker.DockerClient,
    machine_defaults: dict,
    context_registry: ContextRegistry,
    max_attempts: int = 3,
) -> dict:
    """
    Main entry: iteratively synthesize build script, build, and validate via your validate_one.
    Saves attempt pickles and final pickle on success.
    """
    assert task.sha is not None, "task.sha must be set"  # noqa: S101
    default_building_template = context_registry.get_default(tag="env")[1].building_data
    if len(similar_contexts := context_registry.get_similar(task.with_tag("env"))) > 0:
        _, context = similar_contexts[0]
        logger.info(
            "build_once_with_context: found %d similar contexts; using most similar with key=%s",
            len(similar_contexts),
            str(context),
        )
        first_guess = context.building_data
    else:
        _, context = context_registry.get_default(tag="env")
        logger.info("build_once_with_context: no similar context found; using default with key=%s", str(context))
        first_guess = default_building_template

    logger.info(
        "agent_build_and_validate: start for %s/%s@%s (max_attempts=%d)", task.owner, task.repo, task.sha, max_attempts
    )

    program = BuildScriptProgram()

    # image_name = f"asv/{task.owner}/{task.repo}/{task.sha}".lower()

    repo_url = f"https://www.github.com/{task.owner}/{task.repo}"
    logger.debug("agent_build_and_validate: task=%s repo_url=%s", task, repo_url)

    # build probe.
    if not client.images.list(name=task.with_tag("env").get_image_name()):
        logger.info("agent_build_and_validate: probe image not found, building probe image")
        env_res = build_once_with_context(
            client=client,
            task=task.with_tag("env"),
            context=context,
            repo_url=repo_url,
            sha=task.sha,
            timeout_s=args.build_timeout,
            tail_chars=args.tail_chars,
            probe=True,
            pull=True,
            force=True,  # If the env is already present, don't rebuild (saves time)
        )
        if not env_res.ok:
            logger.warning("agent_build_and_validate: probe build failed; something is wrong with Dockerfile")
            # raise RuntimeError("probe build failed; check Dockerfile.")
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
        container_name=task.with_tag("env").get_container_name(),
        workdir="/workspace/repo/",
    )

    try:
        attempts: list[AttemptRecord] = []

        # Attempt loop
        for i in range(max_attempts + 1):
            logger.info("agent_build_and_validate: attempt %d/%d", i, max_attempts)

            if i == 0:
                failure_more = "N/A"
                script = first_guess
            else:
                last = attempts[-1].build_result
                stderr_tail = (last.stderr_tail if last else "") or ""
                stdout_tail = (last.stdout_tail if last else "") or ""
                if last and last.rc == 124:
                    failure_more = "build timeout"
                else:
                    failure_more = f"build failed rc={last.rc}" if last else "build failed"
                logger.debug(
                    "agent_build_and_validate: re-synthesis with last tails (stderr_len=%d, stdout_len=%d, failure=%s)",
                    len(stderr_tail),
                    len(stdout_tail),
                    failure_more,
                )
                try:
                    script = synthesize_script(
                        program,
                        task,
                        attempts[-1].building_data,
                        stderr_tail=stderr_tail,
                        stdout_tail=stdout_tail,
                        building_template=default_building_template,
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
                    attempts.append(AttemptRecord(attempt_idx=i, building_data="", build_result=build_res))
                    break  # exit attempt loop

            ctx = DockerContext(building_data=script)
            # Save attempt pickle
            if i >= 1:
                attempt_pickle = args.output_dir / f"{task.owner}-{task.repo}-{task.sha}-attempt-{i}.pkl"
                _save_pickle(ctx, attempt_pickle)

            # Build
            logger.info("agent_build_and_validate: building image '%s'", task.get_image_name())
            build_res = build_once_with_context(
                client=client,
                task=task.with_tag("pkg"),
                context=ctx,
                repo_url=repo_url,
                sha=task.sha,
                timeout_s=args.build_timeout,
                tail_chars=args.tail_chars * 2,
                force=True,  # Always rebuild package image to pick up new script
            )
            attempts.append(AttemptRecord(attempt_idx=i, building_data=script, build_result=build_res))

            if build_res.ok:
                with context_registry.get_lock():
                    context_registry.register(task.with_tag("pkg"), ctx)

                # Save final pickle and then run full validation using your pipeline
                final_pickle = args.output_dir / f"{task.owner}-{task.repo}-{task.sha}-final.pkl"
                _save_pickle(ctx, final_pickle)
                logger.info("agent_build_and_validate: build succeeded")
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
                # result = validate_one(task.with_tag("pkg"), args, client, context_registry, machine_defaults)
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

            # If the stderr stream doesn't mention docker_build_pkg.sh at all, then iteration is futile
            if build_res.stderr_tail and "docker_build_pkg.sh" not in build_res.stderr_tail:
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

            # otherwise iterate with new logs
            logger.warning(
                "agent_build_and_validate: attempt %d failed (rc=%s). Iterating if attempts remain.",
                i,
                (build_res.rc if build_res else "unknown"),
            )

        # All attempts failed

        last = attempts[-1].build_result
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
        tool_exec.shutdown()
        # remove any containers that were built.
        try:
            cont = client.containers.get(task.with_tag("env").get_container_name())
            cont.remove(force=True)
        except NotFound:
            pass
        try:
            cont = client.containers.get(task.with_tag("pkg").get_container_name())
            cont.remove(force=True)
        except NotFound:
            pass
