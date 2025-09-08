from __future__ import annotations

import asyncio
import io
import json
import os
import shutil
import sys
import tarfile
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import docker
from docker.errors import APIError, DockerException, ImageNotFound, NotFound
from docker.models.containers import Container

from datasmith.docker.context import BuildResult, DockerContext, Task
from datasmith.logging_config import get_logger

logger = get_logger("docker.orchestrator")


def gen_run_labels(t: Task, runid: str) -> dict[str, str]:
    return {
        "datasmith.run": runid,
        "datasmith.task": f"{t.owner}/{t.repo}",
        "datasmith.sha": t.sha if t.sha else "unknown",
    }


def _docker_data_root() -> str:
    # Override if you keep Docker data somewhere else
    return os.environ.get("DOCKER_DATA_ROOT", "/var/lib/docker")


def _free_gb(path: str) -> float:
    try:
        usage = shutil.disk_usage(path)
        return usage.free / (1024**3)
    except FileNotFoundError:
        # Fallback: if path doesn't exist, skip guard
        return float("inf")


def _soft_prune(client: docker.DockerClient, run_id: str | None) -> None:
    # Prune stopped containers older than 1h
    try:
        client.containers.prune(filters={"until": "1h"})
    except Exception:
        logger.exception("containers.prune failed")

    # Prune dangling/unused images; filter by run label if available
    try:
        flt: dict[str, Any] = {"until": "1h"}
        if run_id:
            flt["label"] = [f"datasmith.run={run_id}"]  # pyright: ignore[reportArgumentType]
        report = client.images.prune(filters=flt)
        logger.info("images.prune reclaimed %s bytes", report.get("SpaceReclaimed", 0))
    except Exception:
        logger.exception("images.prune failed")

    # Optional: BuildKit cache prune (API may not exist on older docker-py)
    try:
        if hasattr(client.api, "prune_builds"):
            client.api.prune_builds(filters={"until": "24h"})
    except Exception:
        logger.debug("build cache prune not available or failed", exc_info=True)


async def _guard_and_prune(
    client: docker.DockerClient,
    min_free_gb: float,
    data_root: str,
    run_id: str | None,
    hard_fail: bool,
) -> None:
    free = _free_gb(data_root)
    if free >= min_free_gb:
        return

    logger.warning("Low disk on %s: %.1f GB free < %.1f GB. Pruning…", data_root, free, min_free_gb)
    # Run pruning in a thread to keep the event loop responsive
    await asyncio.to_thread(_soft_prune, client, run_id)
    free2 = _free_gb(data_root)
    logger.info("After prune: %.1f GB free (target: %.1f GB)", free2, min_free_gb)

    if hard_fail and free2 < min_free_gb:
        raise SystemExit(f"Insufficient disk space after prune: {free2:.1f} GB free (need {min_free_gb:.1f} GB).")


async def _guard_loop(
    client: docker.DockerClient,
    min_free_gb: float,
    data_root: str,
    run_id: str | None,
    interval_s: int,
    hard_fail: bool,
    stop_event: asyncio.Event,
) -> None:
    # First check immediately
    try:
        await _guard_and_prune(client, min_free_gb, data_root, run_id, hard_fail)
    except SystemExit:
        raise
    except Exception:
        logger.exception("Initial disk guard failed")

    # Then periodic checks
    while not stop_event.is_set():
        try:
            await _guard_and_prune(client, min_free_gb, data_root, run_id, hard_fail)
        except SystemExit:
            raise
        except Exception:
            logger.exception("Periodic disk guard failed")
        await asyncio.wait_for(stop_event.wait(), timeout=interval_s)


def get_docker_client(max_concurrency: int = 10) -> docker.DockerClient:
    """Return an authenticated Docker client or exit with an error."""
    try:
        return docker.from_env(timeout=300, max_pool_size=max_concurrency)
    except DockerException as exc:
        sys.exit(f"Could not connect to Docker daemon: {exc}")


def build_repo_image(client: docker.DockerClient, image_name: str, repo_url: str, docker_dir: str) -> None:
    """Ensure IMAGE exists locally, optionally pulling it."""
    try:
        client.images.get(image_name)
        logger.info("Docker image '%s' found locally.", image_name)
    except ImageNotFound as exc:
        if repo_url:
            logger.info("Docker image '%s' not found locally, building it with REPO_URL=%s", image_name, repo_url)
            try:
                client.images.build(
                    path=docker_dir,
                    tag=image_name,
                    buildargs={"REPO_URL": repo_url},
                )
            except DockerException as exc2:
                sys.exit(f"Failed to build image {image_name}: {exc2}")

        else:
            raise RuntimeError from exc

    if not client.images.get(image_name):
        raise RuntimeError


def build_repo_sha_image(
    client: docker.DockerClient, docker_ctx: DockerContext, task: Task, force: bool = False, run_id: str | None = None
) -> BuildResult:
    assert task.sha is not None, "Task.sha must be set"  # noqa: S101
    repo_url = f"https://www.github.com/{task.owner}/{task.repo}"
    build_res: BuildResult = docker_ctx.build_container_streaming(
        client=client,
        image_name=task.get_image_name(),
        build_args={"REPO_URL": repo_url, "COMMIT_SHA": task.sha},
        probe=False,
        force=False,
        timeout_s=15 * 60,  # 15 minutes
        tail_chars=10_000,
        pull=False,
        run_labels=gen_run_labels(task, runid="unknown" if run_id is None else run_id),
    )
    return build_res


async def run_container(  # noqa: C901
    client: docker.DockerClient,
    task: Task,
    ctx: DockerContext,
    cores: str | Sequence[int],
    asv_args: str,
    machine_args: dict[str, str],
    output_dir: Path,
    run_id: str | None = None,
) -> tuple[int, dict[str, str]]:
    """
    Launch one container pinned to *cores* (a cpuset string like ``"4,5,6,7"`` or
    an iterable of ints) and wait for it to finish.

    Returns the container's exit status code.
    """
    assert task.sha is not None, "Task.sha must be set"  # noqa: S101

    # Normalise to the cpuset string Docker expects
    cpuset = ",".join(map(str, cores)) if not isinstance(cores, str) else cores
    num_cores = len(cpuset.split(","))

    if "machine" not in machine_args:
        raise ValueError("machine_args must contain a 'machine' key")
    machine_args["machine"] = task.sha
    env = {
        "ASV_ARGS": f"{asv_args} --cpu-affinity {cpuset} --parallel {num_cores} --set-commit-hash={task.sha} --machine={task.sha}",
        "ASV_MACHINE": machine_args.get("machine", ""),
        "ASV_OS": machine_args.get("os", ""),
        "ASV_NUM_CPU": machine_args.get("num_cpu", "1"),
        "ASV_ARCH": machine_args.get("arch", ""),
        "ASV_CPU": machine_args.get("cpu", ""),
        "ASV_RAM": machine_args.get("ram", ""),
    }

    def _launch() -> tuple[int, dict[str, str]]:  # noqa: C901
        assert task.sha is not None, "Task.sha must be set"  # noqa: S101
        logger.debug("docker build name=%s cpuset=%s env=%s", task.get_image_name(), cpuset, env)
        repo_url = f"https://github.com/{task.owner}/{task.repo}.git"
        res = ctx.build_container_streaming(
            client=client,
            image_name=task.get_image_name(),
            build_args={"REPO_URL": repo_url, "COMMIT_SHA": task.sha},
            probe=False,
            force=False,
            timeout_s=1800,  # 30 minutes
            tail_chars=10_000,
            pull=True,
            run_labels=gen_run_labels(task, runid="unknown" if run_id is None else run_id),
        )
        if not res.ok:
            logger.error("Failed to build image %s: %s", task.get_image_name(), res.stderr_tail)
            return 1, {}

        logger.debug("docker run name=%s cpuset=%s env=%s", task.get_container_name(), cpuset, env)
        # Start the container on the specified CPUs
        run_dir = (output_dir / "results" / task.get_container_name()).resolve()
        run_dir.mkdir(parents=True, exist_ok=True)

        # Ensure that task.get_image_name() isn't already running. If it is, remove it.
        try:
            container = client.containers.run(
                task.get_image_name(),
                detach=True,
                name=task.get_container_name(),
                environment=env,
                cpuset_cpus=cpuset,
                volumes={str(run_dir): {"bind": "/output", "mode": "rw"}},
                network_mode=os.environ.get("DOCKER_NETWORK_MODE", None),
            )
        except Exception as exc:
            if "Conflict" in str(exc):
                logger.warning(
                    "Container name conflict, trying to remove existing container %s.", task.get_container_name()
                )
                try:
                    old_container = client.containers.get(task.get_container_name())
                    old_container.stop(timeout=60)
                    old_container.remove(force=True)
                except NotFound:
                    logger.warning("Container %s not found when trying to remove it.", task.get_container_name())
                    return 1, {}
                except APIError:
                    logger.exception(
                        "Failed to remove existing container %s, cannot continue.", task.get_container_name()
                    )
                    return 1, {}
                # Try to run again
                container = client.containers.run(
                    task.get_image_name(),
                    detach=True,
                    name=task.get_container_name(),
                    environment=env,
                    cpuset_cpus=cpuset,
                    volumes={str(run_dir): {"bind": "/output", "mode": "rw"}},
                    network_mode=os.environ.get("DOCKER_NETWORK_MODE", None),
                )
            else:
                logger.exception("Failed to start container %s", task.get_container_name())
                return 1, {}

        # Dump container stdout/stderr to a per-container log file
        log_file = output_dir / "logs" / f"{task.get_container_name()}.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        for line in container.logs(stream=True, follow=True):
            with log_file.open("a") as f:
                f.write(line.decode())

        logger.info("Container %s started, waiting for it to finish...", task.get_container_name())
        result = container.wait()  # blocks until exit
        logger.info("Container %s finished with status %s", task.get_container_name(), result.get("StatusCode", 1))

        # get the contents of all files in the /output folder and return dictionary.
        # files = log_container_output(container, archive="/output")
        files = {}
        for p in run_dir.rglob("*"):
            if p.is_file():
                try:
                    files[str(p.relative_to(output_dir))] = p.read_text()
                except UnicodeDecodeError:
                    files[str(p.relative_to(output_dir))] = f"<{p.stat().st_size} bytes>"

        # remove container
        try:
            container.remove(force=True)
        except Exception:
            logger.exception("Failed to remove container %s", task.get_container_name())
            pass
        try:
            client.images.remove(image=task.get_image_name(), force=True, noprune=True)
        except Exception:
            logger.exception("Failed to remove image %s", task.get_image_name())
            pass
        return result.get("StatusCode", 1), files

    # Keep the event loop responsive
    return await asyncio.to_thread(_launch)


def log_container_output(container: Container, archive: str = "/output") -> dict[str, str]:
    stream, stat = container.get_archive(archive)
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
    guard_min_free_gb: float = float(os.getenv("DATASMITH_MIN_FREE_GB", "200")),  # default: 200GB
    guard_interval_s: int = int(os.getenv("DATASMITH_GUARD_INTERVAL_S", "120")),  # default: 2 min
    guard_hard_fail: bool = bool(int(os.getenv("DATASMITH_GUARD_HARD_FAIL", "0"))),
    guard_data_root: str | None = None,
) -> dict[Task, dict[str, str]]:
    """
    Schedule all <repo, sha> pairs while ensuring that each container
    receives `n_cores` dedicated, non-overlapping CPU cores.
    """
    run_id = os.environ.get("DATASMITH_RUN_ID", uuid.uuid4().hex)
    data_root = guard_data_root or _docker_data_root()
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
            status, files = ("OK", files) if rc == 0 else (f"FAIL({rc})", {})
            logger.info("■ cores=%s → %s", cpuset_str, status)
            if len(files):
                interim_path = Path(os.environ["CACHE_LOCATION"]).parent / "interim"
                os.makedirs(interim_path, exist_ok=True)
                with open(os.path.join(interim_path, f"{task.get_container_name()}.json"), "w") as f:
                    json.dump(files, f)
            return (rc, files)
        finally:
            # Always release the core set, even on failure
            core_pool.put_nowait(core_set)

    tasks = [asyncio.create_task(worker(t, ctx)) for t, ctx in contexts]
    stop_event = asyncio.Event()
    guard_task = asyncio.create_task(
        _guard_loop(
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
    failures = sum(rc == 0 for rc in status_codes)
    if failures:
        # sys.exit(f"{failures} container(s) failed")
        logger.warning("%d container(s) failed", failures)
    logger.info("All benchmarks finished")
    return dict(zip([t for t, _ in contexts], files_by_image))
