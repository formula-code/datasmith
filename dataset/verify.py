#!/usr/bin/env python3
"""Verify Docker build contexts for FormulaCode dataset tasks.

Builds a Docker image from a task directory's multi-stage Dockerfile, then
runs run-tests.sh (which includes profile.sh) inside the container to validate the build.

Usage:
    # Single task
    python dataset/verify.py --task dataset/formulacode_verified_new/networkx_networkx/<sha>

    # All tasks in a repo directory
    python dataset/verify.py --task dataset/formulacode_verified_new/networkx_networkx

    # All tasks in dataset
    python dataset/verify.py --task dataset/formulacode_verified_new
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from python_on_whales import DockerClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Path(__file__).with_suffix(".log"), mode="a"),
    ],
)
logger = logging.getLogger("verify")


@dataclass(frozen=True)
class Task:
    """Mirrors the Task dataclass used in task.txt files."""

    owner: str = ""
    repo: str = ""
    sha: str | None = None
    commit_date: float = 0.0
    env_payload: str = ""
    python_version: str = ""
    tag: str = "pkg"
    benchmarks: str = ""


def _parse_task(task_dir: Path) -> Task:
    """Parse task.txt into a Task object."""
    text = (task_dir / "task.txt").read_text().strip()
    return eval(text, {"__builtins__": {}}, {"Task": Task})  # noqa: S307


def _image_tag(task: Task, stage: str = "run") -> str:
    sha_short = (task.sha or "unknown")[:12]
    return f"formulacode/{task.owner}-{task.repo}:{sha_short}-{stage}"


def _write_failure(task_dir: Path, stage: str, detail: str, rc: int = 1) -> None:
    failure = {
        "stage": stage,
        "return_code": rc,
        "error_message": detail[:8000],
    }
    (task_dir / "failure.json").write_text(json.dumps(failure, indent=2))


def _write_success(task_dir: Path, image_tag: str) -> None:
    info = {"local_image": image_tag, "verified_at": time.time()}
    (task_dir / "verification_success.json").write_text(json.dumps(info, indent=2))


def build_image(docker: DockerClient, task_dir: Path, task: Task, target: str = "run") -> str:
    """Build a Docker image from the task directory's multi-stage Dockerfile."""
    tag = _image_tag(task, target)
    repo_url = f"https://github.com/{task.owner}/{task.repo}"

    build_args = {
        "REPO_URL": repo_url,
        "COMMIT_SHA": task.sha or "",
        "ENV_PAYLOAD": task.env_payload,
    }
    if task.python_version:
        build_args["PY_VERSION"] = task.python_version
    if task.benchmarks:
        build_args["BENCHMARKS"] = task.benchmarks

    logger.info("Building %s (target=%s) from %s", tag, target, task_dir)
    docker.build(
        str(task_dir),
        tags=[tag],
        target=target,
        build_args=build_args,
    )
    return tag


def run_profile(docker: DockerClient, image_tag: str, timeout: int = 3600) -> tuple[bool, str]:
    """Run profile.sh inside the built image. Returns (ok, output)."""
    try:
        output = docker.run(
            image_tag,
            # ENTRYPOINT is /bin/bash, so just pass the script path
            ["/profile.sh", "/tmp/profile_log"],
            remove=True,
        )
        return True, str(output or "")
    except Exception as e:
        err = str(e)
        # timeout (rc=124) is treated as success for profiling
        if "124" in err or "timeout" in err.lower():
            return True, f"Profile timed out (treated as success): {err[:2000]}"
        return False, err[:4000]


def run_tests(docker: DockerClient, image_tag: str) -> tuple[bool, str]:
    """Run run-tests.sh inside the built image. Returns (ok, output)."""
    try:
        output = docker.run(
            image_tag,
            ["/run-tests.sh"],
            remove=True,
        )
        out_str = str(output or "")
        if "FORMULACODE_NO_BENCHMARKS" in out_str:
            return False, "No ASV benchmarks discovered — task cannot be used in FormulaCode"
        return True, out_str
    except Exception as e:
        err = str(e)
        if "FORMULACODE_NO_BENCHMARKS" in err:
            return False, "No ASV benchmarks discovered — task cannot be used in FormulaCode"
        return False, err[:4000]


def verify_single(task_dir: Path, skip_tests: bool = False) -> bool:
    """Verify a single task directory. Returns True on success."""
    docker = DockerClient()

    # Parse task
    try:
        task = _parse_task(task_dir)
    except Exception as e:
        logger.error("Failed to parse task in %s: %s", task_dir, e)
        _write_failure(task_dir, "parse", str(e))
        return False

    if not task.sha:
        logger.error("Task in %s has no SHA", task_dir)
        _write_failure(task_dir, "parse", "Task.sha is None")
        return False

    tag = _image_tag(task)

    # Build
    try:
        tag = build_image(docker, task_dir, task, target="run")
        logger.info("Build succeeded: %s", tag)
    except Exception as e:
        logger.error("Build failed for %s: %s", task_dir.name, str(e)[:200])
        _write_failure(task_dir, "build", str(e)[:4000])
        return False

    # Tests (optional — profile.sh runs inside run-tests.sh)
    if not skip_tests:
        ok, output = run_tests(docker, tag)
        if not ok:
            logger.error("Tests failed for %s", task_dir.name)
            _write_failure(task_dir, "tests", output)
            return False
        logger.info("Tests passed for %s", task_dir.name)

    _write_success(task_dir, tag)
    logger.info("SUCCESS: %s", task_dir.name)
    return True


def _find_task_dirs(root: Path) -> list[Path]:
    """Find all task directories (dirs containing task.txt) under root."""
    if (root / "task.txt").exists():
        return [root]
    dirs = []
    for task_txt in sorted(root.rglob("task.txt")):
        dirs.append(task_txt.parent)
    return dirs


def is_already_done(task_dir: Path) -> bool:
    """Check if task already has a recent verification_success.json."""
    success = task_dir / "verification_success.json"
    failure = task_dir / "failure.json"
    if not success.exists():
        return False
    if failure.exists() and failure.stat().st_mtime > success.stat().st_mtime:
        return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify FormulaCode dataset tasks")
    parser.add_argument("--task", type=Path, required=True, help="Task dir or parent dir")
    parser.add_argument("--workers", type=int, default=2, help="Parallel workers for batch mode")
    parser.add_argument("--skip-tests", action="store_true", help="Skip run-tests.sh step")
    parser.add_argument("--force", action="store_true", help="Re-verify already-successful tasks")
    args = parser.parse_args()

    task_dirs = _find_task_dirs(args.task)
    if not task_dirs:
        logger.error("No task directories found under %s", args.task)
        sys.exit(1)

    # Filter already-done tasks
    if not args.force:
        pending = [d for d in task_dirs if not is_already_done(d)]
        skipped = len(task_dirs) - len(pending)
        if skipped:
            logger.info("Skipping %d already-verified tasks", skipped)
        task_dirs = pending

    if not task_dirs:
        logger.info("All tasks already verified. Use --force to re-verify.")
        return

    logger.info("Verifying %d task(s) with %d worker(s)", len(task_dirs), args.workers)

    if len(task_dirs) == 1:
        ok = verify_single(task_dirs[0], skip_tests=args.skip_tests)
        sys.exit(0 if ok else 1)

    # Batch mode
    successes = 0
    failures = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(verify_single, td, args.skip_tests): td for td in task_dirs
        }
        for future in as_completed(futures):
            td = futures[future]
            try:
                if future.result():
                    successes += 1
                else:
                    failures += 1
            except Exception as e:
                failures += 1
                logger.exception("Unexpected error for %s", td)
                _write_failure(td, "unknown", str(e))

    logger.info("Batch complete: %d succeeded, %d failed out of %d", successes, failures, len(task_dirs))
    sys.exit(0 if failures == 0 else 1)


if __name__ == "__main__":
    main()
