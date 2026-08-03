#!/usr/bin/env python3
"""Simplified verification script for sandbox-based synthesis.

Builds a Docker image from the task directory, then runs profile.sh and
run-tests.sh inside the container. Writes failure.json or
verification_success.json to the task directory.

This file is self-contained — no datasmith imports. It is copied into the
Codex sandbox workspace and executed by the agent.

Usage:
    python local_ci.py                # verify task/ directory
    python local_ci.py --task /path    # verify custom task directory
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
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
    "emit_manifest.py",
    "profile.sh",
    "run-tests.sh",
    "entrypoint.sh",
    "task.txt",
)

# Verification test timeout. 720 was the historical default and silently
# passed ~34% of builds because a timeout was scored as success; 3600 matches
# every other timeout in the tree and dataset/CLAUDE.md.
DATASMITH_VERIFY_TEST_TIMEOUT_S: int = int(
    os.environ.get("DATASMITH_VERIFY_TEST_TIMEOUT_S", "3600")
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


def run_tests(
    image_tag: str,
    timeout: int | None = None,
    metrics: dict | None = None,
) -> tuple[bool, str, str, int]:
    """Run /run-tests.sh inside the built image.

    A host-side timeout is a FAILURE.  It previously returned success, which
    meant a container killed at the limit was indistinguishable from one that
    passed — that inversion silently verified ~34% of candidate_containers.

    rc == 78 (EX_CONFIG) or the FORMULACODE_NO_BENCHMARKS sentinel still means
    the repo has no ASV suite at this commit: task-level unsuitability, not a
    fixable bug.
    """
    limit = DATASMITH_VERIFY_TEST_TIMEOUT_S if timeout is None else timeout
    timed_out, stdout, stderr, rc = _run_container_with_timeout(
        image_tag, ["/run-tests.sh", "--all"], limit, metrics=metrics
    )
    if metrics is not None:
        metrics["timeout_s"] = limit
        metrics["test_timed_out"] = timed_out
    if timed_out:
        return (
            False,
            stdout,
            f"Tests exceeded the {limit}s limit and were killed. Raise "
            f"DATASMITH_VERIFY_TEST_TIMEOUT_S or --test-timeout if this repo "
            f"legitimately needs longer.",
            rc,
        )
    if rc == 78 or "FORMULACODE_NO_BENCHMARKS" in stdout:
        return False, stdout, "No ASV benchmarks discovered — task cannot be used in FormulaCode", rc
    if rc != 0:
        return False, stdout, stderr, rc
    return True, stdout, stderr, rc


_TESTS_BLOCK_RE = re.compile(r"FORMULACODE_TESTS_START\s*\n(.*?)\nFORMULACODE_TESTS_END", re.DOTALL)


def _parse_test_summary(stdout: str) -> dict:
    """Extract the JSON object run-tests.sh prints between
    FORMULACODE_TESTS_START / FORMULACODE_TESTS_END.

    That block's shape varies by which code path produced it (see
    run-tests.sh and docker/templates/pytest_runner.py):
      - a normal pytest run prints pytest_runner.py's flat summary dict
        (total/passed/failed/skipped/xfailed/xpassed/error/rerun/warnings)
      - the no-ASV-benchmarks early exit prints the same flat shape with
        hardcoded zeros
      - a run_pytest=False template config prints an unrelated
        {"results": {"exit_code": ..., "details": ...}} shape carrying
        neither "failed" nor "error"

    Returns ``{}`` for a missing block, malformed JSON, or a top level that
    isn't a JSON object -- callers must treat every field as independently
    present-or-absent, never guess a value for it.
    """
    m = _TESTS_BLOCK_RE.search(stdout)
    if not m:
        return {}
    try:
        parsed = json.loads(m.group(1).strip())
    except (json.JSONDecodeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _pytest_verify_fields(stdout: str) -> dict:
    """Derive ``verify.pytest_collect_ok`` / ``verify.pytest_failed_at_base``
    from run_tests()'s captured stdout.

    pytest_runner.py's ``summary["error"]`` is incremented both by
    ``pytest_collectreport`` (a genuine broken-import/collection failure --
    the class of failure invariant #7 exists to catch, see
    docs/superpowers/specs/2026-07-31-build-manifest-verification-design.md)
    and by per-test setup/teardown errors (``pytest_runtest_logreport``).
    The printed block carries no separate collection-only counter, so
    "error > 0" is the closest available signal without a second read of
    /logs/test_results.json inside the container. A field is set only when
    its source key is present and coerces cleanly; otherwise it's omitted
    so the corresponding invariant is skipped rather than guessed.
    """
    summary = _parse_test_summary(stdout)
    fields: dict = {}
    if "error" in summary:
        try:
            fields["pytest_collect_ok"] = int(summary["error"]) == 0
        except (TypeError, ValueError):
            pass
    if "failed" in summary:
        try:
            fields["pytest_failed_at_base"] = int(summary["failed"])
        except (TypeError, ValueError):
            pass
    return fields


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


# ── Build manifest (mirrors datasmith.docker.manifest) ───────────────────
# local_ci.py runs in the sandbox without datasmith installed, so the fatal
# invariant set is duplicated here.  tests/docker/test_manifest.py
# ::TestLocalCiSync fails if the two drift apart.
_MANIFEST_PATH = "/opt/formulacode/build_manifest.json"


def _c_timed_out(b: dict, v: dict) -> bool | None:
    return None if v.get("test_timed_out") is None else v["test_timed_out"] is False


def _c_discovered_n(b: dict, v: dict) -> bool | None:
    return None if b.get("discovered_n") is None else b["discovered_n"] > 0


def _c_dest_present(b: dict, v: dict) -> bool | None:
    key = "benchmark_dest_present_post_clean"
    return None if b.get(key) is None else bool(b[key])


def _c_secrets(b: dict, v: dict) -> bool | None:
    return None if b.get("secrets_scan_clean") is None else bool(b["secrets_scan_clean"])


def _c_collect(b: dict, v: dict) -> bool | None:
    # Not wired into _FATAL_INVARIANTS below: pytest_collect_failed is warn
    # severity in datasmith.docker.manifest (pytest_runner.py's
    # summary["error"] mixes genuine collection failures with ordinary
    # per-test setup/teardown errors, and only the flat summary reaches this
    # file, so the two are indistinguishable here). Kept, not deleted, so
    # datasmith.docker.manifest.evaluate_invariants -- which re-evaluates the
    # merged manifest downstream with no such gap -- still has a local_ci.py
    # equivalent to diff against. Returns to _FATAL_INVARIANTS once
    # len(results["errors"]) is read from /logs/test_results.json (already
    # written by pytest_runner.py) to separate the two failure classes.
    return None if v.get("pytest_collect_ok") is None else bool(v["pytest_collect_ok"])


# Every check returns True (holds), False (violated), or None (inputs absent
# -> skipped, per check_fatal_invariants below).  This mirrors the three-
# valued _c_* functions in datasmith.docker.manifest exactly -- a bool()-only
# form would treat "not yet populated" as a violation, which is precisely
# the inversion this task exists to prevent.
_FATAL_INVARIANTS = (
    # Unreachable via verify()'s own control flow: run_tests() already
    # returns ok=False on timeout, and verify() returns before
    # check_fatal_invariants runs in that path. Retained (not dead code)
    # because the merged manifest -- including this field -- is persisted
    # and re-evaluated downstream by datasmith.docker.manifest.evaluate_invariants,
    # which has no such short-circuit.
    ("test_timed_out", _c_timed_out),
    ("discovered_n_zero", _c_discovered_n),
    # INERT pending a producer for BENCHMARK_DEST: see the matching comment
    # on this Invariant in datasmith.docker.manifest -- nothing in this tree
    # sets $BENCHMARK_DEST yet, so this check returns None (skipped) on
    # every build. Kept registered rather than removed; the value is
    # expected to come from the per-task override record once one exists.
    ("benchmark_dest_missing", _c_dest_present),
    # benchmark_init_missing is intentionally NOT here: it is warn-severity
    # in datasmith.docker.manifest (build-time it's either a false-positive
    # gate on repos that legitimately lack __init__.py, or tautological if
    # we create it unconditionally -- see the comment on that Invariant for
    # the full reasoning). Do not add it back without also flipping
    # manifest.py's severity, or the parity test below will catch the drift.
    ("head_commit_drift", lambda b, v: _shas_match(b.get("head_at_seal"), b.get("declared_commit"))),
    ("secrets_present", _c_secrets),
    # pytest_collect_failed is intentionally NOT here: it is warn-severity in
    # datasmith.docker.manifest (see the comment on _c_collect above and on
    # that Invariant in manifest.py). Do not add it back without also
    # flipping manifest.py's severity, or the parity test below will catch
    # the drift.
)


def _shas_match(a: str | None, b: str | None) -> bool | None:
    if not a or not b:
        return None
    n = min(len(a), len(b))
    return a[:n] == b[:n]


def read_manifest_from_image(image_tag: str) -> dict | None:
    """Read the sealed manifest out of a built image, or None if absent."""
    try:
        out = sp.run(  # noqa: S603
            ["docker", "run", "--rm", "--entrypoint", "cat", image_tag, _MANIFEST_PATH],  # noqa: S607
            capture_output=True, text=True, timeout=120,
        )
    except (OSError, sp.SubprocessError):
        return None
    if out.returncode != 0 or not out.stdout.strip():
        return None
    try:
        parsed = json.loads(out.stdout)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def check_fatal_invariants(manifest: dict | None) -> list[str]:
    """Return the ids of violated FATAL invariants. Empty means clean.

    An invariant whose inputs are absent is skipped, so an image built before
    manifests existed yields no violations rather than spurious failures.
    """
    if not manifest:
        return []
    build = manifest.get("build") or {}
    verify_block = manifest.get("verify") or {}
    violations = []
    for inv_id, check in _FATAL_INVARIANTS:
        try:
            result = check(build, verify_block)
        except Exception:  # noqa: BLE001 - a broken check must not fail the build
            continue
        if result is False:
            violations.append(inv_id)
    return violations


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
        tag = build_image(docker, task_dir, task, target="final", metrics=metrics)
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

    # Tests — mandatory, no skip option
    ok, stdout, stderr, rc = run_tests(tag, metrics=metrics)

    # Merge post-run observations into the sealed manifest, then gate.
    manifest = read_manifest_from_image(tag)
    if manifest is not None:
        verify_fields = {
            "test_duration_s": metrics.get("test_duration_s"),
            "test_timed_out": metrics.get("test_timed_out"),
            "timeout_s": metrics.get("timeout_s"),
        }
        verify_fields.update(_pytest_verify_fields(stdout))
        manifest.setdefault("verify", {}).update(verify_fields)
        metrics["build_manifest"] = manifest

    if not ok:
        print(f"Tests failed for {task_dir.name}")
        _write_failure(task_dir, "tests", stdout=stdout, stderr=stderr, rc=rc, metrics=metrics)
        return False

    violations = check_fatal_invariants(manifest)
    if violations:
        detail = "Build manifest invariant violations: " + ", ".join(violations)
        print(detail)
        _write_failure(task_dir, "invariants", stdout=stdout, stderr=detail, rc=1, metrics=metrics)
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
    parser.add_argument(
        "--test-timeout",
        type=int,
        default=None,
        help="Override DATASMITH_VERIFY_TEST_TIMEOUT_S for this run (seconds)",
    )
    args = parser.parse_args()

    if not args.task.exists():
        print(f"Task directory not found: {args.task}")
        sys.exit(1)

    if args.test_timeout is not None:
        global DATASMITH_VERIFY_TEST_TIMEOUT_S
        DATASMITH_VERIFY_TEST_TIMEOUT_S = args.test_timeout

    ok = verify(args.task)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
