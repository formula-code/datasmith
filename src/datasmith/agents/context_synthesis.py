# auto_builder.py
from __future__ import annotations

import argparse
import logging
import pickle
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import docker
import dspy

from datasmith.agents.config import configure_agent_backends
from datasmith.docker.context import BuildResult, DockerContext
from datasmith.docker.context_registry import CONTEXT_REGISTRY
from datasmith.docker.validation import Task, validate_one

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
logger.addHandler(handler)

configure_agent_backends()


def _preview(s: str, n: int = 160) -> str:
    s = s or ""
    s = s.replace("\n", "\\n")
    return s[:n] + ("..." if len(s) > n else "")


# --------------------------------------------------------------------------------------
# Heuristic helpers: we extract hints from stderr/stdout and propose concrete patchlets.
# --------------------------------------------------------------------------------------


@dataclass
class Patchlet:
    name: str
    snippet: str
    rationale: str


COMMON_PATCHLETS: list[tuple[re.Pattern, Patchlet]] = [
    # PEP 517 / wheel build failures -> try modern build tools + (maybe) disable isolation
    (
        re.compile(r"could not build wheels|PEP 517|pyproject\.toml build backend", re.I),
        Patchlet(
            "modern-build-stack",
            'micromamba run -n "asv_${version}" python -m pip install -U pip setuptools wheel build "meson-python"\n',
            "Package uses pyproject/PEP 517; ensure modern build stack including meson-python.",
        ),
    ),
    # C/C++/Fortran compile errors -> make sure compilers & basic headers exist and numpy cython pins are friendly
    (
        re.compile(r"(gcc|clang|fortran|gfortran|fatal error:.*\.h: No such file|undefined reference|linker)", re.I),
        Patchlet(
            "compilers-cffi",
            'micromamba run -n "asv_${version}" python -m pip install "cython<3" "numpy<2" "scipy<1.14"\n',
            "Typical compiled extension builds succeed with conservative numeric pins.",
        ),
    ),
    # Meson detected but not installed
    (
        re.compile(r"meson\.build|meson-python|meson\b", re.I),
        Patchlet(
            "meson-explicit",
            'micromamba run -n "asv_${version}" python -m pip install "meson" "meson-python"\n',
            "Explicitly install meson if meson.build appears in logs.",
        ),
    ),
    # Cython needed
    (
        re.compile(r"cython (is )?required|\.pyx|pyximport", re.I),
        Patchlet(
            "cython-needed",
            'micromamba run -n "asv_${version}" python -m pip install "cython<3"\n',
            "Project wants Cython at buildtime.",
        ),
    ),
    # Old setuptools
    (
        re.compile(r"error: invalid command .*bdist_wheel|setuptools\s*([0-9]+\.){1,2}[0-9]+.*too old", re.I),
        Patchlet(
            "setuptools-up",
            'micromamba run -n "asv_${version}" python -m pip install -U "setuptools>=60" wheel\n',
            "Upgrade setuptools/wheel to unlock newer commands.",
        ),
    ),
    # ResolutionImpossible -> try no-build-isolation (project's own constraints)
    (
        re.compile(r"ResolutionImpossible|conflict|cannot resolve", re.I),
        Patchlet(
            "no-build-isolation",
            "# Fallback to no-build-isolation to let project manage constraints\n"
            'micromamba run -n "asv_${version}" python -m pip install --no-build-isolation --editable "${ROOT_PATH}"\n',
            "Let project control pins if resolver conflicts arise.",
        ),
    ),
]


def derive_patchlets(stderr_tail: str, stdout_tail: str) -> list[Patchlet]:
    logger.debug(
        "derive_patchlets: analyzing tails (stderr_len=%d, stdout_len=%d)",
        len(stderr_tail or ""),
        len(stdout_tail or ""),
    )
    text = f"{stderr_tail}\n{stdout_tail}"
    seen = set()
    picks: list[Patchlet] = []
    for pattern, p in COMMON_PATCHLETS:
        if pattern.search(text) and p.name not in seen:
            seen.add(p.name)
            picks.append(p)
            logger.debug("derive_patchlets: matched pattern '%s' -> patchlet '%s'", pattern.pattern, p.name)
    logger.info("derive_patchlets: selected %d patchlet(s): %s", len(picks), ", ".join([p.name for p in picks]) or "-")
    return picks


class BuildScriptSynthesis(dspy.Signature):
    """Draft a bash script (building_data) to build & install a Python repo inside micromamba envs
    discovered via asv.*.json. The script MUST be idempotent and safe to run in Docker.
    Respect this template:
      - discover and cd into the dir containing asv.*.json
      - for each python version listed there:
          * create micromamba env "asv_${version}"
          * ensure asv + build tooling
          * then perform project install (editable or wheel) with best-guess flags
      - no user prompts, all non-interactive
    """

    # Inputs
    owner = dspy.InputField(desc="GitHub owner/org, e.g. 'scikit-learn'.")
    repo = dspy.InputField(desc="Repository name, e.g. 'scikit-learn'.")
    sha = dspy.InputField(desc="Full commit SHA to build.")
    log_tail = dspy.InputField(desc="Recent stderr/stdout tail (merged, up to ~8k chars).")
    last_building_data = dspy.InputField(desc="Previous building_data script; empty on attempt #1.")
    heuristic_notes = dspy.InputField(desc="Bullet list of concrete shell lines/patchlets to consider.")
    expected_template = dspy.InputField(desc="Stable outer template; only BUILD STEPS may be customized.")

    # Output
    building_data = dspy.OutputField(desc="Final executable bash script with only the BUILD STEPS region customized.")


class BuildScriptProgram(dspy.Module):
    def __init__(self) -> None:
        super().__init__()
        self.predict = dspy.Predict(BuildScriptSynthesis)

    def forward(
        self,
        owner: str,
        repo: str,
        sha: str,
        log_tail: str,
        last_building_data: str,
        heuristic_notes: str,
        expected_template: str,
    ) -> str:
        logger.info(
            "DSPy: synthesizing build script for %s/%s@%s (log_tail_len=%d, has_last=%s)",
            owner,
            repo,
            sha,
            len(log_tail or ""),
            bool(last_building_data),
        )
        logger.debug("DSPy: heuristic notes: %s", _preview(heuristic_notes, 240))
        out = self.predict(
            owner=owner,
            repo=repo,
            sha=sha,
            log_tail=log_tail,
            last_building_data=last_building_data,
            heuristic_notes=heuristic_notes,
            expected_template=expected_template,
        )
        # Safety belt: ensure the required fixed template anchors are present.
        script = out.building_data.strip()  # pyright: ignore[reportAttributeAccessIssue]
        logger.debug("DSPy: candidate script preview: %s", _preview(script, 240))
        must_haves = ["cd_asv_json_dir()", "micromamba", "for version in $python_versions; do"]
        ok_template = all(m in script for m in must_haves)
        if not ok_template:
            logger.warning("DSPy: template anchors missing; falling back to provided template")
            script = expected_template
        logger.info("DSPy: finalized script length=%d", len(script))
        assert isinstance(script, str), "type mismatch"  # noqa: S101
        return script


# --------------------------------------------------------------------------------------
# Template: stable outer shell. The agent fills the "BUILD STEPS" region only.
# --------------------------------------------------------------------------------------

BUILDING_TEMPLATE = """#!/usr/bin/env bash
cd_asv_json_dir() {
  local match
  match=$(find . -type f -name "asv.*.json" | head -n 1)

  if [[ -n "$match" ]]; then
    local dir
    dir=$(dirname "$match")
    cd "$dir" || echo "Failed to change directory to $dir"
  else
    echo "No 'asv.*.json' file found in current directory or subdirectories."
  fi
}
eval "$(micromamba shell hook --shell=bash)"
micromamba activate base

ROOT_PATH=${PWD}
cd_asv_json_dir || exit 1
CONF_NAME=$(basename "$(find . -type f -name "asv.*.json" | head -n 1)")
if [[ -z "$CONF_NAME" ]]; then
    echo "No 'asv.*.json' file found in current directory or subdirectories."
    exit 1
fi
python_versions=$(python -c "import asv; pythons = asv.config.Config.load('$CONF_NAME').pythons; print(' '.join(pythons))")
for version in $python_versions; do
    python -c "import asv, os, pathlib
path = pathlib.Path('/output/'\"$COMMIT_SHA\"'/''\"$version\"')
path.mkdir(parents=True, exist_ok=True)

config = asv.config.Config.load('$CONF_NAME')
config.results_dir = str(path / 'results')
config.html_dir = str(path / 'html')

asv.util.write_json('$CONF_NAME', config.__dict__, api_version=config.api_version)
asv.util.write_json(path / '$CONF_NAME', config.__dict__, api_version=config.api_version)
"
    micromamba create -y -n "asv_${version}" -c conda-forge python="$version" git conda mamba "libmambapy<=1.9.9" numpy scipy cython joblib threadpoolctl pytest compilers
    micromamba run -n "asv_${version}" pip install git+https://github.com/airspeed-velocity/asv
    micromamba activate "asv_${version}"
    # ---------- BUILD STEPS (agent should only modify this region) ----------
    # The agent will append/replace lines here based on logs and heuristics.
    # first attempt: editable install with no build isolation for projects with bespoke pins
    # Remove this line if you want to start from scratch.
    python -m pip install --no-build-isolation --editable "${ROOT_PATH}"
    # ---------- END BUILD STEPS ----------
done
""".strip()


# --------------------------------------------------------------------------------------
# Orchestrator
# --------------------------------------------------------------------------------------


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


def _context_from_script(building_data: str) -> DockerContext:
    # Reuse default Dockerfile & entrypoint from the default context
    base = CONTEXT_REGISTRY["default"]
    logger.debug(
        "_context_from_script: base dockerfile len=%d, entrypoint len=%d",
        len(base.dockerfile_data or ""),
        len(base.entrypoint_data or ""),
    )
    logger.debug("_context_from_script: building_data preview: %s", _preview(building_data, 200))
    return DockerContext(
        building_data=building_data,
        dockerfile_data=base.dockerfile_data,
        entrypoint_data=base.entrypoint_data,
    )


def synthesize_script(
    program: BuildScriptProgram,
    task: Task,
    last_script: str,
    stderr_tail: str,
    stdout_tail: str,
) -> str:
    logger.info(
        "synthesize_script: task=%s/%s@%s, last_script=%s",
        task.owner,
        task.repo,
        task.sha,
        "present" if last_script else "none",
    )
    patchlets = derive_patchlets(stderr_tail, stdout_tail)
    notes = ""
    if patchlets:
        notes = "\n".join(f"- {p.rationale}\n  {p.snippet.strip()}" for p in patchlets)
    merged_log = _merge_tail(stderr_tail, stdout_tail)
    logger.debug("synthesize_script: merged_log_len=%d, notes_len=%d", len(merged_log), len(notes))

    script = program(
        owner=task.owner,
        repo=task.repo,
        sha=task.sha,
        log_tail=merged_log,
        last_building_data=last_script or "",
        heuristic_notes=notes or "(no extra hints)",
        expected_template=BUILDING_TEMPLATE,
    )
    script = str(script)
    logger.info("synthesize_script: script length=%d", len(script))
    return script


def build_once_with_context(
    client: docker.DockerClient,
    image_name: str,
    context: DockerContext,
    repo_url: str,
    sha: str,
    *,
    timeout_s: int,
    tail_chars: int,
    pull: bool = False,
) -> BuildResult:
    logger.info("build_once_with_context: registering context key=%s", image_name)
    with CONTEXT_REGISTRY.get_lock():
        CONTEXT_REGISTRY.register(image_name, context)
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
        image_name=image_name,
        build_args={"REPO_URL": repo_url, "COMMIT_SHA": sha},
        force=True,
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


@dataclass
class ArgsLike:
    build_timeout: int
    run_timeout: int
    tail_chars: int
    output_dir: Path


def agent_build_and_validate(
    task: Task,
    args: argparse.Namespace,
    client: docker.DockerClient,
    machine_defaults: dict,
    max_attempts: int = 3,
) -> dict:
    """
    Main entry: iteratively synthesize build script, build, and validate via your validate_one.
    Saves attempt pickles and final pickle on success.
    """
    logger.info(
        "agent_build_and_validate: start for %s/%s@%s (max_attempts=%d)", task.owner, task.repo, task.sha, max_attempts
    )
    program = BuildScriptProgram()

    image_name = f"asv-{task.owner}-{task.repo}-{task.sha}".lower()
    repo_url = f"https://www.github.com/{task.owner}/{task.repo}"
    logger.debug("agent_build_and_validate: image_name=%s repo_url=%s", image_name, repo_url)

    attempts: list[AttemptRecord] = []
    prior_script = ""  # empty on attempt #1

    # Attempt loop
    for i in range(1, max_attempts + 1):
        logger.info("agent_build_and_validate: attempt %d/%d", i, max_attempts)
        if i == 1:
            script = synthesize_script(program, task, prior_script, stderr_tail="", stdout_tail="")
        else:
            last = attempts[-1].build_result
            logger.debug(
                "agent_build_and_validate: re-synthesis with last tails (stderr_len=%d, stdout_len=%d)",
                len(last.stderr_tail or "") if last else 0,
                len(last.stdout_tail or "") if last else 0,
            )
            script = synthesize_script(
                program,
                task,
                attempts[-1].building_data,
                stderr_tail=(last.stderr_tail if last else ""),
                stdout_tail=(last.stdout_tail if last else ""),
            )

        ctx = _context_from_script(script)
        # Save attempt pickle
        attempt_pickle = args.output_dir / f"{task.owner}-{task.repo}-{task.sha}-attempt-{i}.pkl"
        _save_pickle(ctx, attempt_pickle)

        # Build
        logger.info("agent_build_and_validate: building image '%s'", image_name)
        build_res = build_once_with_context(
            client=client,
            image_name=image_name,
            context=ctx,
            repo_url=repo_url,
            sha=task.sha,
            timeout_s=args.build_timeout,
            tail_chars=args.tail_chars,
        )
        attempts.append(AttemptRecord(attempt_idx=i, building_data=script, build_result=build_res))

        if build_res.ok:
            # Save final pickle and then run full validation using your pipeline
            final_pickle = args.output_dir / f"{task.owner}-{task.repo}-{task.sha}-final.pkl"
            _save_pickle(ctx, final_pickle)

            logger.info("agent_build_and_validate: build succeeded; starting validation run")
            result = validate_one(task, args, client, machine_defaults)
            logger.info(
                "agent_build_and_validate: validation stage=%s ok=%s rc=%s",
                result.get("stage"),
                result.get("ok"),
                result.get("rc"),
            )

            result["attempts"] = [
                {
                    "attempt": a.attempt_idx,
                    "ok": (a.build_result.ok if a.build_result else False),
                    "rc": (a.build_result.rc if a.build_result else None),
                    "stderr_tail": (a.build_result.stderr_tail if a.build_result else ""),
                    "stdout_tail": (a.build_result.stdout_tail if a.build_result else ""),
                }
                for a in attempts
            ]
            result["context_pickle"] = str(final_pickle)
            return result

        # otherwise iterate with new logs
        logger.warning(
            "agent_build_and_validate: attempt %d failed (rc=%s). Iterating if attempts remain.",
            i,
            (build_res.rc if build_res else "unknown"),
        )

    # All attempts failed
    last = attempts[-1].build_result
    logger.error("agent_build_and_validate: all attempts failed for %s", image_name)
    # merged_tail = _merge_tail(last.stderr_tail if last else "", last.stdout_tail if last else "")
    return {
        "owner": task.owner,
        "repo": task.repo,
        "sha": task.sha,
        "image_name": image_name,
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
            }
            for a in attempts
        ],
        "files": [],
    }


# def main(args: argparse.Namespace) -> None:
#     from scratch.scripts.parallel_validate_containers import process_inputs

#     logger.info("main: starting auto_builder with args: %s", args)
#     client = get_docker_client()
#     logger.info("main: docker client acquired")

#     all_states = process_inputs(args)
#     logger.info("main: process_inputs done; repos=%d", len(all_states))

#     # Prepare tasks
#     tasks: list[Task] = []
#     for (owner, repo), uniq in all_states.items():
#         limited = list(uniq)[: max(0, args.limit_per_repo)] if args.limit_per_repo > 0 else list(uniq)
#         logger.debug("main: repo %s/%s -> %d sha(s) after limit", owner, repo, len(limited))
#         for sha in limited:
#             tasks.append(Task(owner, repo, sha))

#     logger.info("main: total tasks prepared=%d", len(tasks))

#     (args.output_dir / "results").mkdir(parents=True, exist_ok=True)
#     # reset outputs
#     (args.output_dir / "errors.txt").unlink(missing_ok=True)
#     (args.output_dir / "failures.jsonl").unlink(missing_ok=True)
#     logger.debug("main: output directories prepared and old outputs cleared")

#     machine_defaults: dict[str, str] = asv.machine.Machine.get_defaults()  # pyright: ignore[reportAttributeAccessIssue]
#     machine_defaults = {
#         k: str(v.replace(" ", "_").replace("'", "").replace('"', "")) for k, v in machine_defaults.items()
#     }
#     logger.debug("main: machine_defaults keys=%d", len(machine_defaults))

#     single_task = tasks[0] if len(tasks) >= 1 else None
#     if single_task is None:
#         logger.info(
#             "main: multi-task mode -> %d tasks with up to %d workers (exiting early by design)",
#             len(tasks),
#             args.max_workers,
#         )
#         return

#     # Single task mode: useful for debugging
#     logger.info("main: single-task mode for %s/%s@%s", single_task.owner, single_task.repo, single_task.sha)
#     res = agent_build_and_validate(
#         task=single_task,
#         client=client,
#         args=args,
#         machine_defaults=machine_defaults,
#         max_attempts=10,
#     )
#     print(json.dumps(res, indent=2))


# if __name__ == "__main__":
#     from scratch.scripts.parallel_validate_containers import parse_args

#     args = parse_args()
#     main(args)
