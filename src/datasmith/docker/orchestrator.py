from __future__ import annotations

import asyncio
import io
import os
import sys
import tarfile
from collections.abc import Sequence
from pathlib import Path

import docker
from docker.errors import DockerException, ImageNotFound
from docker.models.containers import Container

from datasmith.docker.context import ContextRegistry
from datasmith.logging_config import get_logger

logger = get_logger("docker.orchestrator")


def get_docker_client() -> docker.DockerClient:
    """Return an authenticated Docker client or exit with an error."""
    try:
        return docker.from_env()
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
    client: docker.DockerClient, context_registry: ContextRegistry, owner: str, repo: str, sha: str, force: bool = False
) -> str:
    image_name = f"asv/{owner}/{repo}/{sha}"
    docker_ctx = context_registry[image_name]
    docker_ctx.build_container(
        client=client,
        image_name=image_name,
        build_args={
            "REPO_URL": f"https://www.github.com/{owner}/{repo}",
            "COMMIT_SHA": sha,
        },
        force=force,
    )
    return image_name


async def run_container(
    client: docker.DockerClient,
    idx: int,
    cores: str | Sequence[int],
    image: str,
    asv_args: str,
    machine_args: dict[str, str],
    output_dir: Path,
) -> tuple[int, dict[str, str]]:
    """
    Launch one container pinned to *cores* (a cpuset string like ``"4,5,6,7"`` or
    an iterable of ints) and wait for it to finish.

    Returns the container's exit status code.
    """

    # Normalise to the cpuset string Docker expects
    cpuset = ",".join(map(str, cores)) if not isinstance(cores, str) else cores
    num_cores = len(cpuset.split(","))
    sha = image.split(":")[0].split("-")[-1]  # Extract the commit SHA from the image name
    if "machine" not in machine_args:
        raise ValueError("machine_args must contain a 'machine' key")
    machine_args["machine"] = sha
    env = {
        "ASV_ARGS": f"{asv_args} --cpu-affinity {cpuset} --parallel {num_cores} --set-commit-hash={sha} --machine={sha}",
        "ASV_MACHINE_ARGS": " ".join([f"--{k} '{v}'" for k, v in machine_args.items()]),
    }

    def _launch() -> tuple[int, dict[str, str]]:
        container_name = f"{image.split(':')[0]}-{idx:03d}"
        logger.debug("docker run name=%s cpuset=%s env=%s", container_name, cpuset, env)

        # Log the exact command a human could copy-paste
        logger.info(
            "$ docker run --rm --name %s -e ASV_ARGS='%s' -e ASV_MACHINE_ARGS='%s' --cpuset-cpus %s %s",
            container_name,
            env["ASV_ARGS"],
            env["ASV_MACHINE_ARGS"],
            cpuset,
            image,
        )

        # Start the container on the specified CPUs
        container = client.containers.run(
            image,
            detach=True,
            name=container_name,
            environment=env,
            cpuset_cpus=cpuset,
            volumes={str(output_dir / "results"): {"bind": "/output", "mode": "rw"}},
        )

        # Dump container stdout/stderr to a per-container log file
        log_file = output_dir / "logs" / f"{container_name}.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        for line in container.logs(stream=True, follow=True):
            with log_file.open("a") as f:
                f.write(line.decode())

        logger.info("Container %s started, waiting for it to finish...", container_name)
        result = container.wait()  # blocks until exit
        logger.info("Container result: %s", result)

        # get the contents of all files in the /output folder and return dictionary.
        files = log_container_output(container, archive="/output")

        # remove container
        container.remove(force=True)
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
    docker_image_names: Sequence[str],
    asv_args: str,
    machine_args: dict[str, str],
    max_concurrency: int,
    n_cores: int,
    output_dir: Path,
    client: docker.DockerClient,
) -> dict[str, dict[str, str]]:
    """
    Schedule all <repo, sha> pairs while ensuring that each container
    receives `n_cores` dedicated, non-overlapping CPU cores.
    """

    # Build one contiguous block of `n_cores` for each worker slot
    core_sets = [list(range(i * n_cores, (i + 1) * n_cores)) for i in range(max_concurrency)]

    # Queue doubles as a resource pool and a concurrency guard
    core_pool: asyncio.Queue[list[int]] = asyncio.Queue(max_concurrency)
    for s in core_sets:
        core_pool.put_nowait(s)

    async def worker(idx: int, image: str) -> tuple[int, dict[str, str]]:
        core_set = await core_pool.get()  # blocks until a free set exists
        cpuset_str = ",".join(map(str, core_set))  # "0,1,2,3"

        logger.info("▶︎ cores=%s image=%s", cpuset_str, image)
        try:
            rc, files = await run_container(
                client=client,
                idx=idx,
                cores=cpuset_str,
                image=image,
                asv_args=asv_args,
                machine_args=machine_args,
                output_dir=output_dir,
            )
            status, files = ("OK", files) if rc == 0 else (f"FAIL({rc})", {})
            logger.info("■ cores=%s → %s", cpuset_str, status)
            return (rc, files)
        finally:
            # Always release the core set, even on failure
            core_pool.put_nowait(core_set)

    tasks = [asyncio.create_task(worker(i, img)) for i, img in enumerate(docker_image_names)]

    results = await asyncio.gather(*tasks)
    status_codes, files_by_image = zip(*results)
    failures = sum(rc != 0 for rc in status_codes)
    if failures:
        sys.exit(f"{failures} container(s) failed")
    logger.info("All benchmarks finished")
    return dict(zip(docker_image_names, files_by_image))
