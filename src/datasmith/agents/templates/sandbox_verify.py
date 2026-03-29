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
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess as sp
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from python_on_whales import DockerClient
from python_on_whales.exceptions import DockerException

# Files the agent must NOT modify.  Hashes are checked before every build.
_IMMUTABLE_FILES = (
    "Dockerfile.pr",
    "docker_build_base.sh",
    "docker_build_env.sh",
    "docker_build_final.sh",
    "profile.sh",
    "run-tests.sh",
    "entrypoint.sh",
    "task.txt",
)


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
    repo_image: str = ""


def _parse_task(task_dir: Path) -> Task:
    text = (task_dir / "task.txt").read_text().strip()
    return eval(text, {"__builtins__": {}}, {"Task": Task})  # noqa: S307


def _image_tag(task: Task, stage: str = "run") -> str:
    sha_short = (task.sha or "unknown")[:12]
    owner_repo = f"{task.owner}-{task.repo}".lower()
    return f"formulacode/{owner_repo}:{sha_short}-{stage}"


def _write_failure(task_dir: Path, stage: str, *, stdout: str = "", stderr: str = "", rc: int = 1) -> None:
    # Truncate output to keep failure.json readable by CLI agents.
    # The Read tool rejects files larger than ~25K tokens, causing cascading
    # failures when the agent tries to read failure.json alongside other files.
    _MAX_OUTPUT = 20_000  # chars — keeps total file well under the token limit
    failure = {
        "stage": stage,
        "return_code": rc,
        "error_message": f"Verification failed during '{stage}' stage (rc={rc}).",
        "stdout": stdout[-_MAX_OUTPUT:] if len(stdout) > _MAX_OUTPUT else stdout,
        "stderr": stderr[-_MAX_OUTPUT:] if len(stderr) > _MAX_OUTPUT else stderr,
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

    build_args = {
        "REPO_IMAGE": task.repo_image,
        "COMMIT_SHA": task.sha or "",
        "ENV_PAYLOAD": task.env_payload,
    }
    if task.python_version:
        build_args["PY_VERSION"] = task.python_version
    if task.benchmarks:
        build_args["BENCHMARKS"] = task.benchmarks

    print(f"Building {tag} (target={target}) from {task_dir}")
    log_lines: list[str] = []
    try:
        for line in docker.build(
            str(task_dir),
            tags=[tag],
            target=target,
            build_args=build_args,
            file=str(task_dir / "Dockerfile.pr"),
            stream_logs=True,
        ):
            log_lines.append(line)
    except DockerException as e:
        stdout = "".join(log_lines)
        raise BuildError(
            f"Docker build failed (rc={e.return_code})",
            stdout=stdout,
            stderr=e.stderr or "",
            rc=e.return_code,
        ) from e
    return tag


class BuildError(Exception):
    """Raised when a Docker build fails, carrying captured output."""

    def __init__(self, message: str, stdout: str, stderr: str, rc: int) -> None:
        super().__init__(message)
        self.stdout = stdout
        self.stderr = stderr
        self.rc = rc


def _run_container_with_timeout(
    image_tag: str,
    command: list[str],
    timeout: int,
) -> tuple[bool, str, str, int]:
    """Run a Docker container with a host-side timeout.

    Returns (timed_out, stdout, stderr, returncode).
    """
    cmd = ["docker", "run", "--rm", "--pull", "never", image_tag, *command]
    try:
        result = sp.run(cmd, capture_output=True, text=True, timeout=timeout)  # noqa: S603, S607
        return False, result.stdout, result.stderr, result.returncode
    except (sp.TimeoutExpired, KeyboardInterrupt) as exc:
        # Capture partial output from the timed-out process
        stdout = getattr(exc, "stdout", None) or ""
        stderr = getattr(exc, "stderr", None) or ""
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
        return True, stdout, stderr, -1


def _parse_test_summary(stdout: str) -> dict | None:
    """Extract the JSON summary between FORMULACODE_TESTS_START/END markers."""
    start = "FORMULACODE_TESTS_START"
    end = "FORMULACODE_TESTS_END"
    s = stdout.find(start)
    e = stdout.find(end)
    if s == -1 or e == -1:
        return None
    payload = stdout[s + len(start) : e].strip()
    try:
        return json.loads(payload)  # type: ignore[no-any-return]
    except json.JSONDecodeError:
        return None


def _parse_snapshot_summary(stdout: str) -> dict | None:
    """Extract the JSON summary between FORMULACODE_SNAPSHOT_START/END markers."""
    start = "FORMULACODE_SNAPSHOT_START"
    end = "FORMULACODE_SNAPSHOT_END"
    s = stdout.find(start)
    e = stdout.find(end)
    if s == -1 or e == -1:
        return None
    payload = stdout[s + len(start) : e].strip()
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return None


def run_tests(image_tag: str, timeout: int = 720) -> tuple[bool, str, str, int]:
    timed_out, stdout, stderr, rc = _run_container_with_timeout(
        image_tag, ["/run-tests.sh", "--all"], timeout
    )
    if timed_out:
        return True, stdout, f"Tests timed out after {timeout}s (treated as success)", rc

    # Exit code 78 (EX_CONFIG) or sentinel = no benchmarks discovered
    if rc == 78 or "FORMULACODE_NO_BENCHMARKS" in stdout:
        return False, stdout, "No ASV benchmarks discovered — task cannot be used in FormulaCode", rc

    # Non-timeout failures
    if rc != 0:
        return False, stdout, stderr, rc

    # run-tests.sh may exit 0 even on collection errors — check structured output
    summary = _parse_test_summary(stdout)
    if summary is not None:
        if summary.get("total", 0) == 0 or summary.get("error", 0) > 0:
            return False, stdout, stderr, rc

    # Check that benchmarks were actually discovered
    snapshot = _parse_snapshot_summary(stdout)
    if snapshot is not None and snapshot.get("total", 0) == 0:
        return False, stdout, "No ASV benchmarks discovered (snapshot total=0)", rc

    return True, stdout, stderr, rc


def _check_file_integrity(task_dir: Path) -> str | None:
    """Verify that immutable files haven't been modified since workspace setup.

    Returns ``None`` if all files are intact, or an error description.
    """
    hashes_file = task_dir.parent / ".immutable_hashes.json"
    if not hashes_file.exists():
        return None  # no hash manifest = no enforcement (e.g. manual runs)

    expected: dict[str, str] = json.loads(hashes_file.read_text())
    modified: list[str] = []
    deleted: list[str] = []

    for fname, expected_hash in expected.items():
        fp = task_dir / fname
        if not fp.exists():
            deleted.append(fname)
        elif hashlib.md5(fp.read_bytes()).hexdigest() != expected_hash:  # noqa: S324
            modified.append(fname)

    if not modified and not deleted:
        return None

    parts = [
        "File integrity violation — only docker_build_pkg.sh and "
        "docker_build_run.sh may be edited.",
    ]
    if modified:
        parts.append(f"Modified: {', '.join(modified)}")
    if deleted:
        parts.append(f"Deleted: {', '.join(deleted)}")
    parts.append("Revert your changes to these files and try again.")
    return "\n".join(parts)


def verify(task_dir: Path) -> bool:
    docker = DockerClient()

    # Check file integrity before anything else
    integrity_error = _check_file_integrity(task_dir)
    if integrity_error:
        print(f"INTEGRITY ERROR:\n{integrity_error}")
        _write_failure(task_dir, "integrity", stderr=integrity_error)
        return False

    # Parse task
    try:
        task = _parse_task(task_dir)
    except Exception as e:
        print(f"Failed to parse task in {task_dir}: {e}")
        _write_failure(task_dir, "parse", stderr=str(e))
        return False

    if not task.sha:
        print(f"Task in {task_dir} has no SHA")
        _write_failure(task_dir, "parse", stderr="Task.sha is None")
        return False

    # Build
    try:
        tag = build_image(docker, task_dir, task, target="run")
        print(f"Build succeeded: {tag}")
    except BuildError as e:
        print(f"Build failed: {e}")
        _write_failure(task_dir, "build", stdout=e.stdout, stderr=e.stderr, rc=e.rc)
        return False
    except Exception as e:
        print(f"Build failed: {str(e)[:200]}")
        _write_failure(task_dir, "build", stderr=str(e))
        return False

    # Tests — mandatory, no skip option (profile.sh runs inside run-tests.sh)
    ok, stdout, stderr, rc = run_tests(tag)
    if not ok:
        print(f"Tests failed for {task_dir.name}")
        _write_failure(task_dir, "tests", stdout=stdout, stderr=stderr, rc=rc)
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
    args = parser.parse_args()

    if not args.task.exists():
        print(f"Task directory not found: {args.task}")
        sys.exit(1)

    ok = verify(args.task)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
