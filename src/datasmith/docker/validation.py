from __future__ import annotations

import argparse
import contextlib
import logging
import os
import shlex
import threading
from pathlib import Path

import docker
from docker.models.containers import Container

from datasmith.docker.context import BuildResult, ContextRegistry, Task
from datasmith.docker.orchestrator import log_container_output

logger = logging.getLogger(__name__)

_err_lock = threading.Lock()


def format_cmds(image_name: str, owner: str, repo: str, sha: str, out_dir: Path) -> tuple[str, str]:
    build_cmd = (
        f"docker build -t {shlex.quote(image_name)} src/datasmith/docker/ "
        f"--build-arg REPO_URL=https://www.github.com/{owner}/{repo} "
        f"--build-arg COMMIT_SHA={sha}"
    )
    run_cmd = (
        f"docker run --rm -v {shlex.quote(str((out_dir / 'results').absolute()))}:/output "
        f"{shlex.quote(image_name)} asv run --quick --python=same --set-commit-hash={sha}"
    )
    return build_cmd, run_cmd


def append_error_line(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with _err_lock, open(path, "a") as f:
        f.write(text.rstrip() + "\n")


def tail_chars(text: str | bytes, n: int) -> str:
    if isinstance(text, bytes):
        try:
            text = text.decode("utf-8", errors="replace")
        except Exception:
            if isinstance(text, bytes):
                text = text.decode("latin-1", errors="replace")
    return str((text or "")[-n:])


def wait_container_with_timeout(container: Container, timeout_s: int) -> tuple[int | None, bool]:
    """
    Wait for container to exit; on timeout, stop it. Returns (exit_code or None, timed_out).
    """
    code_box: dict[str, int | None] = {"code": None}
    done = threading.Event()

    def _wait() -> None:
        try:
            res = container.wait()  # blocking
            code_box["code"] = res.get("StatusCode")
        except Exception:
            code_box["code"] = None
        finally:
            done.set()

    t = threading.Thread(target=_wait, daemon=True)
    t.start()
    finished = done.wait(timeout=timeout_s)
    if finished:
        return code_box["code"], False

    # Timeout: stop the container
    with contextlib.suppress(Exception):
        container.stop(timeout=10)
    # Make a best-effort to fetch a code after stop
    try:
        res = container.wait(timeout=10)  # docker-py may ignore timeout; best effort
        return res.get("StatusCode"), True
    except Exception:
        return None, True


def _handle_build_error(
    task: Task,
    build_cmd: str,
    run_cmd: str,
    build_res: BuildResult,
    args: argparse.Namespace,
    image_name: str,
    build_stage: str,
) -> dict:
    msg = f"$ {build_cmd}\n$ {run_cmd}\n[build FAILED rc={build_res.rc} in {build_res.duration_s:.1f}s]"
    if build_res.stderr_tail:
        msg += f"\n---- build stderr tail ----\n{build_res.stderr_tail}"
    append_error_line(args.output_dir / "errors.txt", msg)
    logger.error(msg)
    return {
        "owner": task.owner,
        "repo": task.repo,
        "sha": task.sha,
        "image_name": image_name,
        "stage": build_stage,
        "ok": False,
        "rc": build_res.rc,
        "duration_s": build_res.duration_s,
        "cmd_build": build_cmd,
        "cmd_run": run_cmd,
        "stderr_tail": build_res.stderr_tail,
        "stdout_tail": build_res.stdout_tail,
        "files": [],
    }


def _handle_run_error(
    task: Task,
    build_cmd: str,
    run_cmd: str,
    rc: int,
    logs_tail: str,
    args: argparse.Namespace,
    image_name: str,
    run_stage: str,
    build_stage: str,
    files: dict[str, str],
) -> dict:
    msg = f"$ {build_cmd}\n$ {run_cmd}\n[run FAILED rc={rc} in (<= {args.run_timeout}s)]"
    if logs_tail:
        msg += f"\n---- run logs tail ----\n{logs_tail}"
    append_error_line(args.output_dir / "errors.txt", msg)
    logger.error(msg)
    return {
        "owner": task.owner,
        "repo": task.repo,
        "sha": task.sha,
        "image_name": image_name,
        "stage": f"{run_stage}+{build_stage}",
        "ok": False,
        "rc": rc,
        "duration_s": None,
        "cmd_build": build_cmd,
        "cmd_run": run_cmd,
        "stderr_tail": logs_tail,
        "stdout_tail": "",
        "files": files,
    }


def _handle_run_exception(
    task: Task, build_cmd: str, run_cmd: str, args: argparse.Namespace, image_name: str, build_stage: str
) -> dict:
    logger.exception("%s failed to run.", image_name)
    msg = f"$ {build_cmd}\n$ {run_cmd}\n[run FAILED: exception during start]"
    append_error_line(args.output_dir / "errors.txt", msg)
    return {
        "owner": task.owner,
        "repo": task.repo,
        "sha": task.sha,
        "image_name": image_name,
        "stage": f"run-exception+{build_stage}",
        "ok": False,
        "rc": 1,
        "duration_s": None,
        "cmd_build": build_cmd,
        "cmd_run": run_cmd,
        "stderr_tail": "",
        "stdout_tail": "",
        "files": [],
    }


def validate_one(  # noqa: C901
    task: Task,
    args: argparse.Namespace,
    client: docker.DockerClient,
    context_registry: ContextRegistry,
    machine_defaults: dict,
) -> dict:
    """
    Build via Docker SDK streaming (with timeout), then run container (with timeout).
    Emits errors immediately on failure (build or run).
    Returns a structured dict for JSONL summarization.
    """
    assert task.sha is not None, "Task.sha must be set"  # noqa: S101
    docker_ctx = context_registry[task.get_image_name()]

    build_cmd, run_cmd = format_cmds(task.get_image_name(), task.owner, task.repo, task.sha, args.output_dir)

    build_res: BuildResult = docker_ctx.build_container_streaming(
        client=client,
        image_name=task.get_image_name(),
        build_args={
            "REPO_URL": f"https://www.github.com/{task.owner}/{task.repo}",
            "COMMIT_SHA": task.sha,
        },
        force=False,
        timeout_s=args.build_timeout,
        tail_chars=args.tail_chars,
        pull=False,
    )
    if build_res.rc == 124:
        build_stage = "build-timeout"
    elif build_res.rc != 0:
        build_stage = "build-failed"
    else:
        build_stage = "build-ok"

    if not build_res.ok:
        return _handle_build_error(task, build_cmd, run_cmd, build_res, args, task.get_image_name(), build_stage)

    # --- RUN ---
    # prepare env (clone default Machine args and set machine=sha)
    machine_args = dict(machine_defaults)
    machine_args["machine"] = task.sha
    env = {
        "ASV_ARGS": f"--quick --python=same --set-commit-hash={task.sha}",
        "ASV_MACHINE_ARGS": " ".join([f"--{k}='{v}'" for k, v in machine_args.items()]),
    }

    container = None
    files = {}
    try:
        logger.debug("validate_one: running container %s", task.get_container_name())
        container = client.containers.run(
            image=task.get_image_name(),
            detach=True,
            name=task.get_container_name(),
            environment=env,
            volumes={str((args.output_dir / "results").absolute()): {"bind": "/output", "mode": "rw"}},
            network_mode=os.environ.get("DOCKER_NETWORK_MODE", None),
        )

        # Wait with timeout; stop on timeout
        exit_code, timed_out = wait_container_with_timeout(container, args.run_timeout)

        # Collect logs tail
        try:
            raw_logs = container.logs(stdout=True, stderr=True)
        except Exception:
            raw_logs = b""

        logs_tail = tail_chars(raw_logs, args.tail_chars)
        rc = 124 if timed_out else (exit_code if exit_code is not None else 1)

        # Archive logs/artifacts (your helper)
        try:
            files = log_container_output(container, archive="/output")
        except Exception:
            logger.exception("Failed to archive output for %s", task.get_image_name())

        ok = rc == 0

        # set stage to "run-{failed/ok/timeout}" + "build-{failed/ok/timeout}" for clarity
        run_stage = "run"
        if timed_out:
            run_stage += "-timeout"
        elif not ok:
            run_stage += "-failed"
        else:
            run_stage += "-ok"

        if not ok:
            return _handle_run_error(
                task, build_cmd, run_cmd, rc, logs_tail, args, task.get_image_name(), run_stage, build_stage, files
            )

        return {
            "owner": task.owner,
            "repo": task.repo,
            "sha": task.sha,
            "image_name": task.get_image_name(),
            "stage": f"{run_stage}+{build_stage}",
            "ok": ok,
            "rc": rc,
            "duration_s": None,
            "cmd_build": build_cmd,
            "cmd_run": run_cmd,
            "stderr_tail": logs_tail,
            "stdout_tail": "",
            "files": files,
        }
    except Exception:
        return _handle_run_exception(task, build_cmd, run_cmd, args, task.get_image_name(), build_stage)
    finally:
        # best-effort cleanup
        try:
            if container:
                container.remove(force=True)
        except Exception:
            logger.exception("Failed to remove container for %s", task.get_image_name())
