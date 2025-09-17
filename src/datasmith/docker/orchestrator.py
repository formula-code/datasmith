from __future__ import annotations

import asyncio
import contextlib
import hashlib
import io
import json
import os
import shutil
import stat
import sys
import tarfile
import time
from collections.abc import Sequence
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

import docker
from docker.errors import APIError, DockerException, ImageNotFound, NotFound
from docker.models.containers import Container
from requests.exceptions import ReadTimeout

from datasmith.docker.context import BuildResult, DockerContext, Task, _new_api_client
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


# helpers: reproducible, minimally .dockerignore-aware tar of a directory
def _read_dockerignore(root: Path) -> tuple[list[str], list[str]]:
    path = root / ".dockerignore"
    if not path.exists():
        return [], []
    ignores: list[str] = []
    negates: list[str] = []
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("!"):
            negates.append(line[1:].strip())
        else:
            ignores.append(line)
    # Always ignore .git if not explicitly negated
    if ".git" not in ignores:
        ignores.append(".git")
    return ignores, negates


def _path_matches_any(rel_posix: str, pats: list[str]) -> bool:
    # naive but effective: support *, ?, ** via fnmatch; also treat dir/ as prefix
    for p in pats:
        if p.endswith("/") and (rel_posix == p[:-1] or rel_posix.startswith(p)):
            return True
        if fnmatch(rel_posix, p) or fnmatch("/" + rel_posix, p):
            return True
    return False


def _dir_context_tar_bytes(root_dir: str, dockerfile_name: str = "Dockerfile") -> bytes:  # noqa: C901
    root = Path(root_dir).resolve()
    ignores, negates = _read_dockerignore(root)

    def is_included(p: Path) -> bool:
        rel = p.relative_to(root).as_posix()
        if rel == "":
            return True
        ignored = _path_matches_any(rel, ignores)
        if ignored and _path_matches_any(rel, negates):
            ignored = False
        return not ignored

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        # walk deterministically
        for dirpath, dirnames, filenames in os.walk(root):
            dirpath_p = Path(dirpath)
            # sort for deterministic order
            dirnames.sort()
            filenames.sort()
            # ensure directory entries are added with stable metadata
            rel_dir = dirpath_p.relative_to(root).as_posix()
            if rel_dir != "" and is_included(dirpath_p):
                ti = tarfile.TarInfo(name=rel_dir)
                ti.type = tarfile.DIRTYPE
                ti.mode = 0o755
                ti.mtime = 0
                ti.uid = ti.gid = 0
                ti.uname = ti.gname = ""
                tar.addfile(ti)

            # files
            for name in filenames:
                p = dirpath_p / name
                if not is_included(p):
                    continue
                rel = p.relative_to(root).as_posix()

                try:
                    st = os.lstat(p)
                except FileNotFoundError:
                    continue  # raced; skip

                if stat.S_ISLNK(st.st_mode):
                    # preserve symlink
                    ti = tarfile.TarInfo(name=rel)
                    ti.type = tarfile.SYMTYPE
                    ti.linkname = os.readlink(p)
                    ti.mode = 0o777
                    ti.mtime = 0
                    ti.uid = ti.gid = 0
                    ti.uname = ti.gname = ""
                    tar.addfile(ti)
                elif stat.S_ISREG(st.st_mode):
                    ti = tarfile.TarInfo(name=rel)
                    ti.size = st.st_size
                    ti.mode = stat.S_IMODE(st.st_mode) or 0o644
                    ti.mtime = 0
                    ti.uid = ti.gid = 0
                    ti.uname = ti.gname = ""
                    with open(p, "rb") as f:
                        tar.addfile(ti, fileobj=f)
                # other types (sockets, pipes) are skipped

        # Ensure the Dockerfile exists at root with canonical name
        df = root / dockerfile_name
        if df.exists() and dockerfile_name != "Dockerfile":
            # duplicate/alias to "Dockerfile" for the builder
            with open(df, "rb") as f:
                data = f.read()
            ti = tarfile.TarInfo(name="Dockerfile")
            ti.size = len(data)
            ti.mode = 0o644
            ti.mtime = 0
            ti.uid = ti.gid = 0
            ti.uname = ti.gname = ""
            tar.addfile(ti, io.BytesIO(data))
    buf.seek(0)
    return buf.getvalue()


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
        with contextlib.suppress(SystemExit, Exception):
            await _guard_and_prune(client, min_free_gb, data_root, run_id, hard_fail)

        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop_event.wait(), timeout=interval_s)


def get_docker_client(max_concurrency: int = 10) -> docker.DockerClient:
    """Return an authenticated Docker client or exit with an error."""
    try:
        return docker.from_env(timeout=1800, max_pool_size=max_concurrency)
    except DockerException as exc:
        sys.exit(f"Could not connect to Docker daemon: {exc}")


# def build_repo_image(client: docker.DockerClient, image_name: str, repo_url: str, docker_dir: str) -> None:
#     """Ensure IMAGE exists locally, building it from docker_dir with low-level API.

#     Uses a reproducible tar context (stable mtimes/owners/order) to improve cache
#     hits and a fresh API client to avoid connection contention/timeouts.
#     """
#     try:
#         client.images.get(image_name)
#         logger.info("Docker image '%s' found locally.", image_name)
#         return
#     except ImageNotFound:
#         pass

#     if not repo_url:
#         raise RuntimeError(f"Image '{image_name}' not found and no REPO_URL provided.")

#     logger.info("Docker image '%s' not found locally, building it with REPO_URL=%s", image_name, repo_url)

#     # Prepare reproducible context bytes once
#     try:
#         context_bytes = _dir_context_tar_bytes(docker_dir, dockerfile_name="Dockerfile")
#     except Exception as exc:
#         raise SystemExit(f"Failed to prepare build context from {docker_dir}: {exc}") from exc

#     api = _new_api_client_from(client, timeout=1800)

#     buildargs = {"REPO_URL": repo_url, "BUILDKIT_INLINE_CACHE": "1"}
#     cache_from = None
#     if base_image := os.environ.get("DOCKER_CACHE_FROM", None):
#         logger.info("Using DOCKER_CACHE_FROM='%s' for build cache.", base_image)
#         cache_from = [base_image]

#     network_mode = os.environ.get("DOCKER_NETWORK_MODE", "") or None

#     # Pretty log
#     logger.info("$ docker build -t %s %s --build-arg REPO_URL=%s", image_name, docker_dir, repo_url)

#     try:
#         stream = api.build(
#             fileobj=io.BytesIO(context_bytes),
#             custom_context=True,
#             tag=image_name,
#             buildargs=buildargs,
#             decode=True,
#             rm=True,
#             pull=False,
#             network_mode=network_mode,
#             cache_from=cache_from,
#         )
#         # Drain stream to completion (avoids premature socket close)
#         for _ in stream:
#             pass
#     except ReadTimeout:
#         logger.exception("Build timed out for image %s", image_name)
#         raise
#     except DockerException as exc2:
#         raise SystemExit(f"Failed to build image {image_name}: {exc2}") from exc2


#     # Ensure image present
#     try:
#         client.images.get(image_name)
#     except ImageNotFound as exc:
#         raise RuntimeError(f"Build completed but image '{image_name}' not found.") from exc
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

    Returns the container's exit status code and collected files.
    """
    assert task.sha is not None, "Task.sha must be set"  # noqa: S101

    output_dir = output_dir.resolve()

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

        # Build (streaming) — unchanged, but keep pull=True if you need fresh bases
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

        run_dir = (output_dir / "results" / task.get_container_name()).resolve()
        run_dir.mkdir(parents=True, exist_ok=True)

        # Low-level client per-call avoids session/socket contention
        api = _new_api_client(client, timeout=600)

        # HostConfig and create_container; only set network_mode when provided
        network_mode = os.environ.get("DOCKER_NETWORK_MODE", "") or None
        host_config = api.create_host_config(
            cpuset_cpus=cpuset,
            binds={str(run_dir): {"bind": "/output", "mode": "rw"}},
            network_mode=network_mode,  # None => omitted by docker-py
        )

        # Handle name conflicts explicitly
        container_id = None
        # Run profile.sh explicitly with LOG_PATH under /output and empty ADDITIONAL_ASV_ARGS
        profile_cmd = [
            "/profile.sh",
            "/output/profile",
            f"--cpu-affinity {cpuset} --parallel {num_cores} --quick --dry-run",
        ]
        try:
            container_resp = api.create_container(
                image=task.get_image_name(),
                name=task.get_container_name(),
                entrypoint=profile_cmd,
                # environment=env,
                command=profile_cmd,
                host_config=host_config,
            )
            container_id = container_resp.get("Id")
        except APIError as e:
            response = getattr(e, "response", None)
            if response is not None and getattr(response, "status_code", None) == 409:
                logger.warning("Container name conflict, removing existing %s", task.get_container_name())
                try:
                    old = client.containers.get(task.get_container_name())
                    with contextlib.suppress(Exception):
                        old.stop(timeout=60)
                    with contextlib.suppress(Exception):
                        old.remove(force=True)
                except NotFound:
                    pass
                # retry once
                container_resp = api.create_container(
                    image=task.get_image_name(),
                    name=task.get_container_name(),
                    # environment=env,
                    entrypoint=profile_cmd,
                    command=profile_cmd,
                    host_config=host_config,
                )
                container_id = container_resp.get("Id")
            else:
                logger.exception("create_container failed for %s", task.get_container_name())
                return 1, {}

        if not container_id:
            logger.error("No container id returned for %s", task.get_container_name())
            return 1, {}

        # Start with retry/backoff to ride out daemon stalls instead of hard 300s timeout
        backoff = [1, 2, 4, 8, 16, 32]
        for i, d in enumerate(backoff, start=1):
            try:
                api.start(container_id)  # returns 204
                break
            except ReadTimeout:
                if i == len(backoff):
                    logger.exception(
                        "Timed out starting container %s after %ss", task.get_container_name(), sum(backoff)
                    )
                    return 1, {}
                logger.warning("ReadTimeout on start(%s); retrying in %ss", task.get_container_name(), d)
                time.sleep(d)
            except APIError:
                if i == len(backoff):
                    logger.exception("APIError starting container %s", task.get_container_name())
                    return 1, {}
                logger.warning("APIError on start(%s); retrying in %ss", task.get_container_name(), d)
                time.sleep(d)

        # Switch back to high-level object for logs/wait ergonomics
        container = client.containers.get(container_id)

        # Stream logs to file efficiently (open once, write bytes)
        log_file = output_dir / "logs" / f"{task.get_container_name()}.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            for chunk in container.logs(stream=True, follow=True):
                # chunk is bytes already
                with log_file.open("ab") as f:
                    f.write(chunk)
        except Exception:
            logger.exception("Log streaming failed for %s (continuing)", task.get_container_name())

        logger.info("Container %s started, waiting for it to finish...", task.get_container_name())
        result = container.wait()  # blocks until exit
        logger.info("Container %s finished with status %s", task.get_container_name(), result.get("StatusCode", 1))

        # collect files from the run directory
        files: dict[str, str] = {}
        for p in run_dir.rglob("*"):
            if p.is_file():
                try:
                    files[str(p.relative_to(output_dir))] = p.read_text()
                except UnicodeDecodeError:
                    files[str(p.relative_to(output_dir))] = f"<{p.stat().st_size} bytes>"

        # add logs to files
        files["logs"] = log_file.read_text()

        # cleanup
        try:
            container.remove(force=True)
        except Exception:
            logger.exception("Failed to remove container %s", task.get_container_name())
        try:
            client.images.remove(image=task.get_image_name(), force=True, noprune=True)
        except Exception:
            logger.exception("Failed to remove image %s", task.get_image_name())

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
            logger.info("■ cores=%s → %s", cpuset_str, status)
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
    use_aws_batch: bool = False,
    aws_batch_config: dict[str, Any] | None = None,
    guard_min_free_gb: float = float(os.getenv("DATASMITH_MIN_FREE_GB", "1200")),
    guard_interval_s: int = int(os.getenv("DATASMITH_GUARD_INTERVAL_S", "120")),
    guard_hard_fail: bool = bool(int(os.getenv("DATASMITH_GUARD_HARD_FAIL", "0"))),
    guard_data_root: str | None = None,
) -> dict[Task, dict[str, str]]:
    """
    Orchestrate benchmark execution with optional AWS batch processing.

    This function provides a unified interface that can either:
    1. Run benchmarks locally using the existing orchestrate function
    2. Run benchmarks on AWS EC2 instances in batches for scalability

    Args:
        contexts: List of (Task, DockerContext) pairs to execute
        asv_args: ASV command line arguments
        machine_args: ASV machine configuration
        max_concurrency: Maximum number of concurrent local tasks (ignored for AWS)
        n_cores: Number of CPU cores per task
        output_dir: Directory to store results
        client: Docker client (ignored for AWS)
        use_aws_batch: If True, use AWS batch execution instead of local
        aws_batch_config: Configuration for AWS batch execution
        guard_min_free_gb: Minimum free disk space for local execution
        guard_interval_s: Disk space check interval for local execution
        guard_hard_fail: Whether to fail hard on low disk space
        guard_data_root: Docker data root for disk space checks

    Returns:
        Dictionary mapping Task to benchmark result files
    """
    if not use_aws_batch:
        # Use existing local orchestration
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

    # AWS batch execution
    if not aws_batch_config:
        raise ValueError("aws_batch_config is required when use_aws_batch=True")

    from datasmith.docker.aws_batch_executor import AwsBatchConfig, AWSBatchExecutor

    # Create AWS batch config
    aws_cfg = AwsBatchConfig(
        region=aws_batch_config["region"],
        s3_bucket=aws_batch_config["s3_bucket"],
        s3_prefix=aws_batch_config.get("s3_prefix", "datasmith-batch-execution"),
        subnet_id=aws_batch_config["subnet_id"],
        security_group_ids=aws_batch_config["security_group_ids"],
        iam_instance_profile_name=aws_batch_config["iam_instance_profile_name"],
        ami_id=aws_batch_config["ami_id"],
        instance_type=aws_batch_config.get("instance_type", "c6i.xlarge"),
        key_name=aws_batch_config.get("key_name"),
        spot_max_price=aws_batch_config.get("spot_max_price"),
        tags=aws_batch_config.get("tags", {}),
        max_tasks_per_instance=aws_batch_config.get("max_tasks_per_instance", 100),
        batch_timeout_s=aws_batch_config.get("batch_timeout_s", 2 * 60 * 60),
        poll_interval_s=aws_batch_config.get("poll_interval_s", 30),
        max_batch_retries=aws_batch_config.get("max_batch_retries", 1),
        num_cores_per_task=n_cores,
        asv_args=asv_args,
    )

    # Create batch executor
    batch_executor = AWSBatchExecutor(aws_cfg)

    # Execute batch
    run_id = os.environ.get("DATASMITH_RUN_ID") or _compute_deterministic_run_id(
        contexts, asv_args=asv_args, machine_args=machine_args, n_cores=n_cores
    )
    batch_results = batch_executor.execute_batch(
        tasks=contexts,
        machine_args=machine_args,
        asv_args=asv_args,
        run_id=run_id,
    )

    # Convert batch results to the expected format
    files_by_image = {}
    for batch_result in batch_results:
        # Find the corresponding task
        task = None
        for t, _ in contexts:
            assert t.sha is not None  # noqa: S101
            if f"{run_id}-task-" in batch_result.task_id and t.sha in batch_result.task_id:
                task = t
                break

        if task is None:
            logger.warning("Could not find task for batch result %s", batch_result.task_id)
            continue

        # Store benchmark files
        files_by_image[task] = batch_result.benchmark_files

        # Save individual result to output directory
        result_dir = output_dir / "results" / task.get_container_name()
        result_dir.mkdir(parents=True, exist_ok=True)

        for filename, content in batch_result.benchmark_files.items():
            file_path = result_dir / filename
            file_path.write_text(content)

        # Save logs
        log_file = output_dir / "logs" / f"{task.get_container_name()}.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        log_file.write_text(batch_result.benchmark_logs)

        logger.info("Saved results for %s: %d files", task.get_container_name(), len(batch_result.benchmark_files))

    logger.info("AWS batch execution completed: %d successful results", len(files_by_image))
    return files_by_image
