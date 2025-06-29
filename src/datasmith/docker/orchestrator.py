from __future__ import annotations

import asyncio
import logging
import sys
from collections.abc import Sequence
from pathlib import Path

import docker
from docker.errors import DockerException, ImageNotFound


def get_docker_client() -> docker.DockerClient:
    """Return an authenticated Docker client or exit with an error."""
    try:
        return docker.from_env()
    except DockerException as exc:
        sys.exit(f"Could not connect to Docker daemon: {exc}")


def ensure_image(client: docker.DockerClient, image_name: str, repo_url: str, docker_dir: str) -> None:
    """Ensure IMAGE exists locally, optionally pulling it."""
    try:
        client.images.get(image_name)
        logging.info("Docker image '%s' found locally.", image_name)
    except ImageNotFound as exc:
        if repo_url:
            logging.info("Docker image '%s' not found locally, building it with REPO_URL=%s", image_name, repo_url)
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


async def run_container(
    client: docker.DockerClient,
    idx: int,
    cores: str | Sequence[int],  # ← was “core: int”
    commit_sha: str,
    asv_conf_path: str,
    image: str,
    asv_args: str,
    output_dir: Path,
) -> int:
    """
    Launch one container pinned to *cores* (a cpuset string like ``"4,5,6,7"`` or
    an iterable of ints) and wait for it to finish.

    Returns the container's exit status code.
    """

    # Normalise to the cpuset string Docker expects
    cpuset = ",".join(map(str, cores)) if not isinstance(cores, str) else cores
    num_cores = len(cpuset.split(","))
    env = {
        "COMMIT_SHA": commit_sha,
        "ASV_CONF_PATH": asv_conf_path,
        # asv can take a comma-separated list for --cpu-affinity
        "ASV_ARGS": f"{asv_args} --cpu-affinity {cpuset} --parallel {num_cores}",
    }

    def _launch() -> int:
        container_name = f"asv_{idx}_{commit_sha[:7]}"
        logging.debug("docker run name=%s cpuset=%s env=%s", container_name, cpuset, env)

        # Log the exact command a human could copy-paste
        logging.info(
            "$ docker run --rm --name %s -e COMMIT_SHA=%s -e ASV_CONF_PATH=%s -e ASV_ARGS='%s' --cpuset-cpus %s %s",
            container_name,
            commit_sha,
            asv_conf_path,
            env["ASV_ARGS"],
            cpuset,
            image,
        )

        # Start the container on the specified CPUs
        container = client.containers.run(
            image,
            detach=True,
            remove=True,
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

        logging.info("Container %s started, waiting for it to finish…", container_name)
        result = container.wait()  # blocks until exit
        logging.info("Container result: %s", result)
        return result.get("StatusCode", 1)

    # Keep the event loop responsive
    return await asyncio.to_thread(_launch)


async def orchestrate(
    commit_shas: Sequence[str],
    asv_conf_paths: Sequence[str],
    docker_image_names: Sequence[str],
    asv_args: str,
    max_concurrency: int,
    n_cores: int,
    output_dir: Path,
    client: docker.DockerClient,
) -> None:
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

    async def worker(idx: int, commit_sha: str, asv_conf_path: str, image: str) -> int:
        core_set = await core_pool.get()  # blocks until a free set exists
        cpuset_str = ",".join(map(str, core_set))  # "0,1,2,3"

        logging.info("▶︎ cores=%s sha=%s", cpuset_str, commit_sha)
        try:
            rc = await run_container(
                client=client,
                idx=idx,
                cores=cpuset_str,
                commit_sha=commit_sha,
                asv_conf_path=asv_conf_path,
                image=image,
                asv_args=asv_args,
                output_dir=output_dir,
            )
            status = "OK" if rc == 0 else f"FAIL({rc})"
            logging.info("■ cores=%s → %s", cpuset_str, status)
            return rc
        finally:
            # Always release the core set, even on failure
            core_pool.put_nowait(core_set)

    tasks = [
        asyncio.create_task(worker(i, sha, conf, img))
        for i, (sha, conf, img) in enumerate(zip(commit_shas, asv_conf_paths, docker_image_names))
    ]

    results = await asyncio.gather(*tasks)
    failures = sum(rc != 0 for rc in results)
    if failures:
        sys.exit(f"{failures} container(s) failed")
    logging.info("All benchmarks finished successfully ✔")
