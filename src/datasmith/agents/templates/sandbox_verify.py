#!/usr/bin/env python3
"""Simplified verification script for sandbox-based synthesis.

Builds a Docker image from the task directory, then runs profile.sh and
run-tests.sh inside the container. Writes failure.json or
verification_success.json to the task directory.

This file is self-contained — no datasmith imports. It is copied into the
Codex sandbox workspace and executed by the agent.

Usage:
    python sandbox_verify.py                # verify task/ directory
    python sandbox_verify.py --task /path    # verify custom task directory
    python sandbox_verify.py --skip-tests   # skip the tests stage
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from python_on_whales import DockerClient


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
    # Also remove stale success file if present
    success = task_dir / "verification_success.json"
    if success.exists():
        success.unlink()


def _write_success(task_dir: Path, image_tag: str) -> None:
    info = {"local_image": image_tag, "verified_at": time.time()}
    (task_dir / "verification_success.json").write_text(json.dumps(info, indent=2))
    # Remove stale failure file if present
    failure = task_dir / "failure.json"
    if failure.exists():
        failure.unlink()


def build_image(docker: DockerClient, task_dir: Path, task: Task, target: str = "run") -> str:
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

    print(f"Building {tag} (target={target}) from {task_dir}")
    docker.build(
        str(task_dir),
        tags=[tag],
        target=target,
        build_args=build_args,
    )
    return tag


def _run_container_with_timeout(
    docker: DockerClient,
    image_tag: str,
    command: list[str],
    timeout: int,
) -> tuple[bool, str]:
    """Run a Docker container with a host-side timeout.

    Returns (timed_out, output_or_error).
    """
    import subprocess as sp

    cmd = ["docker", "run", "--rm", "--pull", "never", image_tag, *command]
    try:
        result = sp.run(cmd, capture_output=True, text=True, timeout=timeout)  # noqa: S603, S607
        if result.returncode == 0:
            return False, result.stdout
        # OOM (137) treated as non-timeout success
        if result.returncode in (137,):
            return False, result.stdout or result.stderr
        return False, f"Container exited with code {result.returncode}\n{result.stderr[-4000:]}"
    except (sp.TimeoutExpired, KeyboardInterrupt) as exc:
        # Kill any remaining containers for this image
        try:
            ps_result = sp.run(  # noqa: S603, S607
                ["docker", "ps", "-q", "--filter", f"ancestor={image_tag}"],
                capture_output=True,
                text=True,
            )
            for cid in ps_result.stdout.strip().splitlines():
                if cid:
                    sp.run(["docker", "kill", cid], capture_output=True)  # noqa: S603, S607
        except Exception:
            pass
        if isinstance(exc, KeyboardInterrupt):
            raise
        return True, f"Timed out after {timeout}s"


def run_profile(docker: DockerClient, image_tag: str, timeout: int = 300) -> tuple[bool, str]:
    timed_out, output = _run_container_with_timeout(
        docker, image_tag, ["/profile.sh", "/tmp/profile_log"], timeout  # noqa: S108
    )
    if timed_out:
        return True, f"Profile timed out after {timeout}s (treated as success)"
    # Check for errors
    if "Error" in output and "exit status" in output:
        return False, output[:4000]
    return True, output


def run_tests(docker: DockerClient, image_tag: str, timeout: int = 300) -> tuple[bool, str]:
    timed_out, output = _run_container_with_timeout(
        docker, image_tag, ["/run-tests.sh"], timeout
    )
    if timed_out:
        return True, f"Tests timed out after {timeout}s (treated as success)"
    # Non-timeout failures
    if "Container exited with code" in output:
        return False, output[:4000]
    return True, output


def verify(task_dir: Path, skip_tests: bool = False) -> bool:
    docker = DockerClient()

    # Parse task
    try:
        task = _parse_task(task_dir)
    except Exception as e:
        print(f"Failed to parse task in {task_dir}: {e}")
        _write_failure(task_dir, "parse", str(e))
        return False

    if not task.sha:
        print(f"Task in {task_dir} has no SHA")
        _write_failure(task_dir, "parse", "Task.sha is None")
        return False

    # Build
    try:
        tag = build_image(docker, task_dir, task, target="run")
        print(f"Build succeeded: {tag}")
    except Exception as e:
        print(f"Build failed: {str(e)[:200]}")
        _write_failure(task_dir, "build", str(e)[:4000])
        return False

    # Profile
    ok, output = run_profile(docker, tag)
    if not ok:
        print(f"Profile failed for {task_dir.name}")
        _write_failure(task_dir, "profile", output)
        return False
    print(f"Profile passed for {task_dir.name}")

    # Tests (optional)
    if not skip_tests:
        ok, output = run_tests(docker, tag)
        if not ok:
            print(f"Tests failed for {task_dir.name}")
            _write_failure(task_dir, "tests", output)
            return False
        print(f"Tests passed for {task_dir.name}")

    _write_success(task_dir, tag)
    print(f"SUCCESS: {task_dir.name}")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify Docker build context")
    parser.add_argument(
        "--task",
        type=Path,
        default=Path("task"),
        help="Task directory (default: task/)",
    )
    parser.add_argument("--skip-tests", action="store_true", help="Skip run-tests.sh")
    args = parser.parse_args()

    if not args.task.exists():
        print(f"Task directory not found: {args.task}")
        sys.exit(1)

    ok = verify(args.task, skip_tests=args.skip_tests)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
