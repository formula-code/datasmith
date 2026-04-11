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
import re
import subprocess as sp
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from python_on_whales import DockerClient
from python_on_whales.exceptions import DockerException

# Files the agent must NOT modify.  Hashes are checked before every build.
_IMMUTABLE_FILES = (
    "Dockerfile.pr",
    "docker_build_base.sh",
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


def _write_failure(
    task_dir: Path,
    stage: str,
    *,
    stdout: str = "",
    stderr: str = "",
    rc: int = 1,
    metrics: dict | None = None,
) -> None:
    # Truncate output to keep failure.json readable by CLI agents.
    # The Read tool rejects files larger than ~25K tokens, causing cascading
    # failures when the agent tries to read failure.json alongside other files.
    _MAX_OUTPUT = 20_000  # chars — keeps total file well under the token limit
    failure: dict = {
        "stage": stage,
        "return_code": rc,
        "error_message": f"Verification failed during '{stage}' stage (rc={rc}).",
        "stdout": stdout[-_MAX_OUTPUT:] if len(stdout) > _MAX_OUTPUT else stdout,
        "stderr": stderr[-_MAX_OUTPUT:] if len(stderr) > _MAX_OUTPUT else stderr,
    }
    if metrics:
        failure["resource_metrics"] = metrics
    (task_dir / "failure.json").write_text(json.dumps(failure, indent=2))
    # Also remove stale success file if present
    success = task_dir / "verification_success.json"
    if success.exists():
        success.unlink()


def _write_success(task_dir: Path, image_tag: str, metrics: dict | None = None) -> None:
    info: dict = {"local_image": image_tag, "verified_at": time.time()}
    if metrics:
        info["resource_metrics"] = metrics
    (task_dir / "verification_success.json").write_text(json.dumps(info, indent=2))
    # Remove stale failure file if present
    failure = task_dir / "failure.json"
    if failure.exists():
        failure.unlink()


def build_image(
    docker: DockerClient,
    task_dir: Path,
    task: Task,
    target: str = "run",
    metrics: dict | None = None,
) -> str:
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
    start = time.time()
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
        if metrics is not None:
            metrics["build_duration_s"] = round(time.time() - start, 2)
        raise BuildError(
            f"Docker build failed (rc={e.return_code})",
            stdout=stdout,
            stderr=e.stderr or "",
            rc=e.return_code,
        ) from e

    if metrics is not None:
        metrics["build_duration_s"] = round(time.time() - start, 2)
        try:
            img = docker.image.inspect(tag)
            metrics["image_size_bytes"] = img.size
        except Exception:
            pass

    return tag


class BuildError(Exception):
    """Raised when a Docker build fails, carrying captured output."""

    def __init__(self, message: str, stdout: str, stderr: str, rc: int) -> None:
        super().__init__(message)
        self.stdout = stdout
        self.stderr = stderr
        self.rc = rc


def _parse_failed_stage(build_log: str) -> str:
    """Identify which Dockerfile stage (env/pkg/run) failed from Docker build output.

    Scans for BuildKit stage markers like ``[env 2/2]`` or legacy markers like
    ``FROM env AS pkg``.  Returns the name of the last stage seen before the
    error, or ``"build"`` as a fallback.
    """
    # BuildKit format: #N [stage_name step/total] ...
    buildkit_re = re.compile(r"\[(\w+)\s+\d+/\d+\]")
    # Legacy format: Step N/M : FROM x AS stage
    legacy_re = re.compile(r"Step \d+/\d+\s*:\s*FROM\s+\S+\s+AS\s+(\w+)", re.IGNORECASE)

    last_stage = ""
    for line in build_log.splitlines():
        m = buildkit_re.search(line)
        if m:
            last_stage = m.group(1)
            continue
        m = legacy_re.search(line)
        if m:
            last_stage = m.group(1)

    return last_stage if last_stage else "build"


_MEM_UNITS = {"B": 1, "KIB": 1024, "MIB": 1024**2, "GIB": 1024**3, "TIB": 1024**4}
_MEM_RE = re.compile(r"([\d.]+)\s*((?:[KMGT]i)?B)", re.IGNORECASE)


def _parse_mem_usage(text: str) -> int:
    """Parse a Docker memory string like ``'123.4MiB / 16GiB'`` → bytes (first value only)."""
    m = _MEM_RE.search(text)
    if not m:
        return 0
    value, unit = float(m.group(1)), m.group(2).upper()
    return int(value * _MEM_UNITS.get(unit, 1))


def _run_container_with_timeout(
    image_tag: str,
    command: list[str],
    timeout: int,
    metrics: dict | None = None,
) -> tuple[bool, str, str, int]:
    """Run a Docker container with a host-side timeout.

    Returns (timed_out, stdout, stderr, returncode).

    When *metrics* is provided, records ``test_duration_s`` and
    ``peak_memory_bytes`` by polling ``docker stats`` in a background thread.
    """
    name = f"fc-{uuid.uuid4().hex[:8]}"
    cmd = ["docker", "run", "--name", name, "--pull", "never", image_tag, *command]
    # No --rm: we clean up manually after collecting metrics.

    peak_mem: list[int] = [0]
    stop_event = threading.Event()

    def _poll_stats() -> None:
        """Background thread: poll ``docker stats`` every 2 s to track peak memory."""
        while not stop_event.is_set():
            try:
                r = sp.run(  # noqa: S603, S607
                    ["docker", "stats", name, "--no-stream", "--format", "{{.MemUsage}}"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                if r.returncode == 0 and r.stdout.strip():
                    current = _parse_mem_usage(r.stdout.strip())
                    if current > peak_mem[0]:
                        peak_mem[0] = current
            except Exception:
                pass
            stop_event.wait(2.0)

    poller = threading.Thread(target=_poll_stats, daemon=True)
    poller.start()

    start = time.time()
    try:
        result = sp.run(cmd, capture_output=True, text=True, timeout=timeout)  # noqa: S603, S607
        timed_out = False
        stdout, stderr, rc = result.stdout, result.stderr, result.returncode
    except (sp.TimeoutExpired, KeyboardInterrupt) as exc:
        stdout = getattr(exc, "stdout", None) or ""
        stderr = getattr(exc, "stderr", None) or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode(errors="replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode(errors="replace")
        sp.run(["docker", "kill", name], capture_output=True)  # noqa: S603, S607
        if isinstance(exc, KeyboardInterrupt):
            stop_event.set()
            poller.join(timeout=5)
            sp.run(["docker", "rm", "-f", name], capture_output=True)  # noqa: S603, S607
            raise
        timed_out, rc = True, -1
    finally:
        stop_event.set()
        poller.join(timeout=5)

    wall_time = time.time() - start

    if metrics is not None:
        metrics["test_duration_s"] = round(wall_time, 2)
        if peak_mem[0] > 0:
            metrics["peak_memory_bytes"] = peak_mem[0]

    # Cleanup container
    sp.run(["docker", "rm", "-f", name], capture_output=True)  # noqa: S603, S607
    return timed_out, stdout, stderr, rc


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


def run_tests(image_tag: str, timeout: int = 720, metrics: dict | None = None) -> tuple[bool, str, str, int]:
    timed_out, stdout, stderr, rc = _run_container_with_timeout(
        image_tag, ["/run-tests.sh", "--all"], timeout, metrics=metrics
    )
    if timed_out:
        return True, stdout, f"Tests timed out after {timeout}s (treated as success)", rc

    # Anti-cheating: run-tests.sh emits this marker when it finds pre-fabricated
    # validator artifacts (forged /logs/summary_*.json or test_results.json) or
    # injected test_*.py files at the repo root that are not in the PR base
    # commit.  Either is direct evidence the agent tried to bypass validation.
    if "FORMULACODE_TAMPER_DETECTED" in stdout:
        marker_line = next(
            (ln for ln in stdout.splitlines() if "FORMULACODE_TAMPER_DETECTED" in ln),
            "FORMULACODE_TAMPER_DETECTED",
        )
        return (
            False,
            stdout,
            (
                f"Validation tamper detected: {marker_line.strip()}. "
                "Do not pre-create /logs/*.json files or commit test_*.py files at "
                "the repo root — the validator detects and rejects these. "
                "Remove the offending steps from docker_build_pkg.sh / docker_build_run.sh."
            ),
            rc,
        )

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
    metrics: dict = {}

    # Check file integrity before anything else
    integrity_error = _check_file_integrity(task_dir)
    if integrity_error:
        print(f"INTEGRITY ERROR:\n{integrity_error}")
        _write_failure(task_dir, "integrity", stderr=integrity_error, metrics=metrics)
        return False

    # Parse task
    try:
        task = _parse_task(task_dir)
    except Exception as e:
        print(f"Failed to parse task in {task_dir}: {e}")
        _write_failure(task_dir, "parse", stderr=str(e), metrics=metrics)
        return False

    if not task.sha:
        print(f"Task in {task_dir} has no SHA")
        _write_failure(task_dir, "parse", stderr="Task.sha is None", metrics=metrics)
        return False

    # Check for env_payload override written by the agent
    override_file = task_dir / "env_payload_override.json"
    if override_file.exists():
        try:
            raw = override_file.read_text()
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                print(f"Using env_payload_override.json ({len(parsed)} packages)")
                task = Task(
                    owner=task.owner,
                    repo=task.repo,
                    sha=task.sha,
                    commit_date=task.commit_date,
                    env_payload=json.dumps(parsed),
                    python_version=task.python_version,
                    tag=task.tag,
                    benchmarks=task.benchmarks,
                    repo_image=task.repo_image,
                )
            else:
                print("WARNING: env_payload_override.json is not a JSON list, ignoring")
        except (json.JSONDecodeError, Exception) as e:
            print(f"WARNING: Failed to parse env_payload_override.json: {e}")

    # Build
    try:
        tag = build_image(docker, task_dir, task, target="run", metrics=metrics)
        print(f"Build succeeded: {tag}")
    except BuildError as e:
        stage = _parse_failed_stage(e.stdout)
        print(f"Build failed at stage '{stage}': {e}")
        _write_failure(task_dir, stage, stdout=e.stdout, stderr=e.stderr, rc=e.rc, metrics=metrics)
        return False
    except Exception as e:
        print(f"Build failed: {str(e)[:200]}")
        _write_failure(task_dir, "build", stderr=str(e), metrics=metrics)
        return False

    # Tests — mandatory, no skip option (profile.sh runs inside run-tests.sh)
    ok, stdout, stderr, rc = run_tests(tag, metrics=metrics)
    if not ok:
        print(f"Tests failed for {task_dir.name}")
        _write_failure(task_dir, "tests", stdout=stdout, stderr=stderr, rc=rc, metrics=metrics)
        return False
    print(f"Tests passed for {task_dir.name}")

    _write_success(task_dir, tag, metrics=metrics)
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
