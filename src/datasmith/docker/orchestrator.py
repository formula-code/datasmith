from __future__ import annotations

import asyncio
import hashlib
import io
import json
import os
import sys
import tarfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import docker
from docker.errors import DockerException
from docker.models.containers import Container

from datasmith.docker.context import BuildResult, DockerContext, Task
from datasmith.docker.disk_management import docker_data_root, guard_loop
from datasmith.logging_config import get_logger

logger = get_logger("docker.orchestrator")


def _compute_deterministic_run_id(
    contexts: Sequence[tuple[Task, DockerContext]],
    *,
    asv_args: str,
    machine_args: dict[str, str],
    n_cores: int,
) -> str:
    """Compute a deterministic run_id from tasks, contexts, and config.

    The hash includes:
    - Task identity: (owner, repo, sha, tag)
    - DockerContext hash (relies on DockerContext.__hash__)
    - Config that affects execution: asv_args, machine_args, n_cores
    """
    # Canonical payload
    items: list[tuple[str, str, str | None, str | None, int]] = []
    for task, ctx in contexts:
        items.append((task.owner, task.repo, task.sha, getattr(task, "tag", None), hash(ctx)))

    payload: dict[str, Any] = {
        "tasks": sorted(items),
        "asv_args": asv_args,
        "machine_args": {k: machine_args[k] for k in sorted(machine_args.keys())},
        "n_cores": n_cores,
    }
    s = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:32]


def gen_run_labels(t: Task, runid: str) -> dict[str, str]:
    return {
        "datasmith.run": runid,
        "datasmith.task": f"{t.owner}/{t.repo}",
        "datasmith.sha": t.sha if t.sha else "unknown",
    }


def get_docker_client(max_concurrency: int = 10) -> docker.DockerClient:
    """Return an authenticated Docker client or exit with an error."""
    try:
        return docker.from_env(timeout=1800, max_pool_size=max_concurrency)
    except DockerException as exc:
        sys.exit(f"Could not connect to Docker daemon: {exc}")


def build_repo_sha_image(
    client: docker.DockerClient, docker_ctx: DockerContext, task: Task, force: bool = False, run_id: str | None = None
) -> BuildResult:
    assert task.sha is not None, "Task.sha must be set"  # noqa: S101
    repo_url = f"https://www.github.com/{task.owner}/{task.repo}"
    build_args = {
        "REPO_URL": repo_url,
        "COMMIT_SHA": task.sha,
        "ENV_PAYLOAD": task.env_payload,
    }
    if task.python_version:
        build_args["PY_VERSION"] = task.python_version

    build_res: BuildResult = docker_ctx.build_container_streaming(
        client=client,
        image_name=task.get_image_name(),
        build_args=build_args,
        probe=False,
        force=force,
        timeout_s=45 * 60,  # 45 minutes
        tail_chars=10_000,
        pull=False,
        run_labels=gen_run_labels(task, runid="unknown" if run_id is None else run_id),
    )
    return build_res


def log_container_output(container: Container, archive: str = "/output") -> dict[str, str]:
    stream, _stat = container.get_archive(archive)
    # 3) Load tar stream into memory and walk files
    buf = io.BytesIO()
    for chunk in stream:
        buf.write(chunk)
    buf.seek(0)

    files_by_abs_path = {}

    with tarfile.open(fileobj=buf, mode="r:*") as tar:
        base = archive  # basename of "/output"
        for member in tar.getmembers():
            if not member.isfile():
                continue

            # Normalize member path to an absolute container path under /output
            name = member.name.lstrip("./")
            if name.startswith(base + "/"):
                rel = name[len(base) + 1 :]  # strip leading "output/"
            elif name == base:
                continue  # it's the directory entry itself
            else:
                # fallback: treat member.name as already relative to /output
                rel = name.lstrip("/")

            abs_path = os.path.join(archive, rel)

            fobj = tar.extractfile(member)
            if not fobj:
                continue
            data = fobj.read()

            # Store text as str when possible, otherwise bytes
            try:
                files_by_abs_path[abs_path] = data.decode("utf-8")
            except UnicodeDecodeError:
                files_by_abs_path[abs_path] = str(data)
    return files_by_abs_path


async def orchestrate(
    contexts: Sequence[tuple[Task, DockerContext]],
    asv_args: str,
    machine_args: dict[str, str],
    max_concurrency: int,
    n_cores: int,
    output_dir: Path,
    client: docker.DockerClient,
    *,
    guard_min_free_gb: float = float(os.getenv("DATASMITH_MIN_FREE_GB", "1200")),  # default: 1200GB
    guard_interval_s: int = int(os.getenv("DATASMITH_GUARD_INTERVAL_S", "120")),  # default: 2 min
    guard_hard_fail: bool = bool(int(os.getenv("DATASMITH_GUARD_HARD_FAIL", "0"))),
    guard_data_root: str | None = None,
) -> dict[Task, dict[str, str]]:
    """
    Schedule all <repo, sha> pairs while ensuring that each container
    receives `n_cores` dedicated, non-overlapping CPU cores.
    """
    run_id = os.environ.get("DATASMITH_RUN_ID") or _compute_deterministic_run_id(
        [(t, ctx) for t, ctx in contexts], asv_args=asv_args, machine_args=machine_args, n_cores=n_cores
    )
    data_root = guard_data_root or docker_data_root()
    # Build one contiguous block of `n_cores` for each worker slot
    core_sets = [list(range(i * n_cores, (i + 1) * n_cores)) for i in range(max_concurrency)]

    # Queue doubles as a resource pool and a concurrency guard
    core_pool: asyncio.Queue[list[int]] = asyncio.Queue(max_concurrency)
    for s in core_sets:
        core_pool.put_nowait(s)

    async def worker(task: Task, context: DockerContext) -> tuple[int, dict[str, str]]:
        core_set = await core_pool.get()  # blocks until a free set exists
        cpuset_str = ",".join(map(str, core_set))  # "0,1,2,3"

        logger.info("▶︎ cores=%s image=%s", cpuset_str, task.get_image_name())
        try:
            try:
                rc, files = await run_container(
                    client=client,
                    task=task,
                    ctx=context,
                    cores=cpuset_str,
                    asv_args=asv_args,
                    machine_args=machine_args,
                    output_dir=output_dir,
                    run_id=run_id,
                )
            except Exception:
                logger.exception("Container run failed for %s", task.get_container_name())
                rc, files = 1, {}
            status = "OK" if rc == 0 else f"FAIL({rc})"
            files = files if rc == 0 else {}
            print(status, files)
            logger.info("■ cores=%s -> %s", cpuset_str, status)
            if len(files):
                interim_path = Path(os.environ["CACHE_LOCATION"]).parent / "interim"
                os.makedirs(interim_path, exist_ok=True)
                (interim_path / f"{task.get_container_name()}.json").write_text(json.dumps(files))
            return (rc, files)
        finally:
            # Always release the core set, even on failure
            core_pool.put_nowait(core_set)

    tasks = [asyncio.create_task(worker(t, ctx)) for t, ctx in contexts]
    stop_event = asyncio.Event()
    guard_task = asyncio.create_task(
        guard_loop(
            client=client,
            min_free_gb=guard_min_free_gb,
            data_root=data_root,
            run_id=run_id,
            interval_s=guard_interval_s,
            hard_fail=guard_hard_fail,
            stop_event=stop_event,
        )
    )
    logger.info("Starting %d benchmark tasks with max concurrency %d", len(tasks), max_concurrency)

    results = await asyncio.gather(*tasks)
    stop_event.set()
    try:
        await guard_task
    except SystemExit:
        # Propagate a hard-fail from guard if configured
        raise
    except Exception:
        logger.exception("Guard task ended with error")
    try:
        report = client.images.prune(filters={"label": [f"datasmith.run={run_id}"]})
        logger.info("Final prune for run %s reclaimed %s bytes", run_id, report.get("SpaceReclaimed", 0))
    except Exception:
        logger.debug("Final prune skipped or failed", exc_info=True)

    status_codes, files_by_image = zip(*results)
    failures = sum(rc != 0 for rc in status_codes)
    if failures:
        # sys.exit(f"{failures} container(s) failed")
        logger.warning("%d container(s) failed", failures)
    logger.info("All benchmarks finished")
    return dict(zip([t for t, _ in contexts], files_by_image))


async def batch_orchestrate(
    contexts: Sequence[tuple[Task, DockerContext]],
    asv_args: str,
    machine_args: dict[str, str],
    max_concurrency: int,
    n_cores: int,
    output_dir: Path,
    client: docker.DockerClient | None,
    *,
    guard_min_free_gb: float = float(os.getenv("DATASMITH_MIN_FREE_GB", "1200")),
    guard_interval_s: int = int(os.getenv("DATASMITH_GUARD_INTERVAL_S", "120")),
    guard_hard_fail: bool = bool(int(os.getenv("DATASMITH_GUARD_HARD_FAIL", "0"))),
    guard_data_root: str | None = None,
) -> dict[Task, dict[str, str]]:
    """
    Orchestrate benchmark execution locally.

    Args:
        contexts: List of (Task, DockerContext) pairs to execute
        asv_args: ASV command line arguments
        machine_args: ASV machine configuration
        max_concurrency: Maximum number of concurrent local tasks
        n_cores: Number of CPU cores per task
        output_dir: Directory to store results
        client: Docker client
        guard_min_free_gb: Minimum free disk space for local execution
        guard_interval_s: Disk space check interval for local execution
        guard_hard_fail: Whether to fail hard on low disk space
        guard_data_root: Docker data root for disk space checks

    Returns:
        Dictionary mapping Task to benchmark result files
    """
    if client is None:
        client = get_docker_client(max_concurrency)
    return await orchestrate(
        contexts=contexts,
        asv_args=asv_args,
        machine_args=machine_args,
        max_concurrency=max_concurrency,
        n_cores=n_cores,
        output_dir=output_dir,
        client=client,
        guard_min_free_gb=guard_min_free_gb,
        guard_interval_s=guard_interval_s,
        guard_hard_fail=guard_hard_fail,
        guard_data_root=guard_data_root,
    )
