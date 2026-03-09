from __future__ import annotations

import contextlib
import copy
import datetime
import io
import json
import os
import subprocess
import tarfile
import threading
import time
from collections import deque
from collections.abc import Mapping
from pathlib import Path
from typing import Any, ClassVar

import docker
from docker.errors import APIError, DockerException, ImageNotFound

from datasmith.core.models.build import BuildResult
from datasmith.core.models.task import Task
from datasmith.docker.dockerhub import publish_images_to_dockerhub
from datasmith.docker.s3_cache_manager import S3DockerCacheManager
from datasmith.execution.utils import _get_commit_info
from datasmith.logging_config import get_logger

logger = get_logger("docker.context")


def _new_api_client(client: docker.DockerClient, timeout: int = 600) -> docker.APIClient:
    """
    Create a fresh low-level APIClient for each build to avoid connection
    contention across threads and to align API versions with the daemon.

    Critically, this preserves max_pool_size from the parent client to enable
    true parallelism across multiple threads.
    """
    try:
        base_url = client.api.base_url  # e.g., 'unix://var/run/docker.sock'
    except Exception:
        base_url = None

    try:
        api_version = client.version().get("ApiVersion", "auto")
    except Exception:
        api_version = "auto"

    # Extract max_pool_size from the original client to enable concurrent connections
    try:
        max_pool_size = client.api.max_pool_size  # type: ignore[attr-defined]
    except Exception:
        max_pool_size = 10  # fallback to requests default

    try:
        return docker.APIClient(base_url=base_url, version=api_version, timeout=timeout, max_pool_size=max_pool_size)
    except Exception:
        return docker.APIClient(version="auto", timeout=timeout, max_pool_size=max_pool_size)


def build_base_image(client: docker.DockerClient, ctx: DockerContext) -> str:
    base_key = hash(ctx)
    base_tag = f"asv-base-rev-{base_key}:base"

    res = ctx.build_container_streaming(
        client=client,
        image_name=base_tag,
        build_args={},
        probe=True,
        force=False,
        pull=False,
        timeout_s=1800,
    )
    if not res.ok:
        logger.exception("Failed to build base image %s error=%s", base_tag, res.stderr_tail)
        raise RuntimeError("Failed to build base image")
    return base_tag


class DockerContext:
    """
    A docker context stores all the necessary files to build a docker container
    for running ASV benchmarks. It includes the Dockerfile, entrypoint script,
    and a script to build the container.

    This allows customizing the Docker image without needing to modify the Dockerfile directly.
    """

    default_dockerfile_loc = Path(__file__).parent / "Dockerfile"
    default_entrypoint_loc = Path(__file__).parent / "entrypoint.sh"
    default_docker_build_base_loc = Path(__file__).parent / "docker_build_base.sh"
    default_docker_build_run_loc = Path(__file__).parent / "docker_build_run.sh"
    default_docker_build_env_loc = Path(__file__).parent / "docker_build_env.sh"
    default_docker_build_final_loc = Path(__file__).parent / "docker_build_final.sh"
    default_docker_build_pkg_loc = Path(__file__).parent / "docker_build_pkg.sh"
    default_profile_loc = Path(__file__).parent / "profile.sh"
    default_run_tests_loc = Path(__file__).parent / "run_tests.sh"
    dockerfile_data: str
    entrypoint_data: str
    env_building_data: str
    run_building_data: str
    base_building_data: str
    building_data: str
    profile_data: str
    run_tests_data: str
    final_building_data: str
    # Unix timestamp (float) when this context was registered. Defaults to 0.0
    created_unix: float

    # Cached, reproducible tar bytes per (probe: bool). Immutable => thread-safe reuse.
    _context_tar_bytes: dict[bool, bytes]

    def __init__(
        self,
        building_data: str | None = None,
        dockerfile_data: str | None = None,
        entrypoint_data: str | None = None,
        env_building_data: str | None = None,
        base_building_data: str | None = None,
        run_building_data: str | None = None,
        profile_data: str | None = None,
        run_tests_data: str | None = None,
        final_building_data: str | None = None,
        *,
        created_unix: float | None = None,
    ) -> None:
        if dockerfile_data is None:
            dockerfile_data = self.default_dockerfile_loc.read_text()
        if entrypoint_data is None:
            entrypoint_data = self.default_entrypoint_loc.read_text()
        if base_building_data is None:
            base_building_data = self.default_docker_build_base_loc.read_text()
        if env_building_data is None:
            env_building_data = self.default_docker_build_env_loc.read_text()
        if building_data is None:
            building_data = self.default_docker_build_pkg_loc.read_text()
        if run_building_data is None:
            run_building_data = self.default_docker_build_run_loc.read_text()
        if profile_data is None:
            profile_data = self.default_profile_loc.read_text()
        if run_tests_data is None:
            run_tests_data = self.default_run_tests_loc.read_text()
        if final_building_data is None:
            final_building_data = self.default_docker_build_final_loc.read_text()

        self.dockerfile_data = dockerfile_data
        self.entrypoint_data = entrypoint_data
        self.env_building_data = env_building_data
        self.base_building_data = base_building_data
        self.run_building_data = run_building_data
        self.building_data = building_data
        self.profile_data = profile_data
        self.run_tests_data = run_tests_data

        # By default, creation time is unix epoch 0.0. It is updated on registry registration.
        self.created_unix = 0.0 if created_unix is None else float(created_unix)

        self._context_tar_bytes = {}
        self.final_building_data = final_building_data

    @staticmethod
    def add_bytes(tar: tarfile.TarFile, name: str, data: bytes, mode: int = 0o644) -> None:
        info = tarfile.TarInfo(name=name)
        info.size = len(data)
        info.mode = mode
        info.mtime = 0  # stable for cache keys
        info.uid = info.gid = 0
        info.uname = info.gname = ""
        tar.addfile(info, io.BytesIO(data))

    def _build_tarball_bytes(self, probe: bool = False) -> bytes:
        """
        Build a reproducible tarball (stable mtimes/owners and deterministic order)
        and return its raw bytes for fast reuse across parallel builds.
        """
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            # Deterministic order
            DockerContext.add_bytes(tar, "Dockerfile", self.dockerfile_data.encode("utf-8"))
            DockerContext.add_bytes(tar, "entrypoint.sh", self.entrypoint_data.encode("utf-8"), mode=0o755)
            DockerContext.add_bytes(tar, "docker_build_env.sh", self.env_building_data.encode("utf-8"), mode=0o755)
            DockerContext.add_bytes(tar, "docker_build_run.sh", self.run_building_data.encode("utf-8"), mode=0o755)
            DockerContext.add_bytes(tar, "docker_build_base.sh", self.base_building_data.encode("utf-8"), mode=0o755)
            DockerContext.add_bytes(tar, "profile.sh", self.profile_data.encode("utf-8"), mode=0o755)
            DockerContext.add_bytes(tar, "run_tests.sh", self.run_tests_data.encode("utf-8"), mode=0o755)
            DockerContext.add_bytes(tar, "docker_build_final.sh", self.final_building_data.encode("utf-8"), mode=0o755)
            if not probe:
                DockerContext.add_bytes(tar, "docker_build_pkg.sh", self.building_data.encode("utf-8"), mode=0o755)
        buf.seek(0)
        return buf.getvalue()

    def _get_context_bytes(self, probe: bool = False) -> bytes:
        """
        Return cached tar bytes for the requested probe flag, building once lazily.
        """
        if probe not in self._context_tar_bytes:
            self._context_tar_bytes[probe] = self._build_tarball_bytes(probe=probe)
        return self._context_tar_bytes[probe]

    def build_tarball_stream(self, probe: bool = False) -> io.BytesIO:
        """
        Backwards-compatible: return a new BytesIO over the cached tar bytes.
        """
        return io.BytesIO(self._get_context_bytes(probe=probe))

    def process_image_name(self, image_name: str) -> tuple[str, str]:
        """Split image name into (repo, target). Target is required."""
        assert ":" in image_name and image_name.rsplit(":", 1)[1], "Image name must include a ':target' suffix."  # noqa: S101
        repo, target = image_name.rsplit(":", 1)
        return repo, target

    def build_container(  # noqa: C901
        self,
        client: docker.DockerClient,
        image_name: str,
        build_args: dict[str, str],
        force: bool = False,
        probe: bool = False,
        run_labels: dict[str, str] | None = None,
    ) -> None:
        """Builds the Docker image if it does not exist or if force is True."""
        run_labels = run_labels if run_labels else {}
        _, target = self.process_image_name(image_name)
        image_exists = False
        try:
            image = client.images.get(image_name)
            image_exists = True
            if force:
                logger.info("Force rebuild requested. Removing existing Docker image '%s'.", image_name)
                client.images.remove(image=image.id, force=True)
                image_exists = False
            else:
                logger.info("Docker image '%s' found locally.", image_name)
        except ImageNotFound:
            logger.info("Docker image '%s' not found locally. Building new image.", image_name)

        if not image_exists:
            cache_from = None
            if base_image := os.environ.get("DOCKER_CACHE_FROM", None):
                logger.info("Using DOCKER_CACHE_FROM='%s' for build cache.", base_image)
                build_args = {**build_args, "BASE_IMAGE": base_image}
                cache_from = [base_image]

            if len(build_args) == 0 and not probe:
                raise RuntimeError(f"Docker image '{image_name}' not found and no REPO_URL provided for build.")

            # Ensure all build-arg values are strings (Docker expects strings)
            safe_build_args: dict[str, str] = {}
            for k, v in (build_args or {}).items():
                if isinstance(v, str):
                    safe_build_args[k] = v
                elif isinstance(v, (bytes, bytearray)):
                    try:
                        safe_build_args[k] = v.decode("utf-8", errors="replace")
                    except Exception:
                        safe_build_args[k] = str(v)
                else:
                    try:
                        safe_build_args[k] = json.dumps(v)
                    except Exception:
                        safe_build_args[k] = str(v)

            # Pretty log
            if len(safe_build_args):
                build_args_str = " --build-arg ".join(f"{k}={v}" for k, v in safe_build_args.items())
                logger.info("$ docker build -t %s . --build-arg %s", image_name, build_args_str)
            else:
                logger.info("$ docker build -t %s .", image_name)

            api = _new_api_client(client)
            try:
                stream = api.build(
                    fileobj=io.BytesIO(self._get_context_bytes(probe=probe)),
                    custom_context=True,
                    tag=image_name,
                    buildargs={**safe_build_args, "BUILDKIT_INLINE_CACHE": "1"},
                    target=target,
                    rm=True,
                    labels=run_labels,
                    network_mode=os.environ.get("DOCKER_NETWORK_MODE", None),
                    cache_from=cache_from,
                    decode=True,
                    pull=False,
                )
                # Drain stream to ensure completion
                for _ in stream:
                    pass
            except DockerException:
                logger.exception("Failed to build Docker image '%s'", image_name)

        if not client.images.get(image_name):
            raise RuntimeError(f"Image '{image_name}' failed to build and is not found.")

    def _build_with_buildx(  # noqa: C901
        self,
        client: docker.DockerClient,
        image_name: str,
        build_args: dict[str, str],
        run_labels: dict[str, str] | None = None,
        probe: bool = False,
        *,
        force: bool = False,
        delete_img: bool = False,
        timeout_s: float = float("inf"),
        tail_chars: int = 4000,
        pull: bool = False,
        s3_cache_config: dict[str, str] | None = None,
    ) -> BuildResult:
        """
        Build using docker buildx with advanced caching support.
        Falls back to SDK build if buildx is not available.
        """
        run_labels = run_labels if run_labels else {}
        _, target = self.process_image_name(image_name)
        t0 = time.time()
        success = False

        try:
            # Fast path: respect existing image when not forcing
            try:
                img = client.images.get(image_name)
                if force:
                    logger.info("Force rebuild requested. Removing '%s'.", image_name)
                    with contextlib.suppress(Exception):
                        client.images.remove(image=img.id, force=True)
                else:
                    logger.info("Docker image '%s' found locally (skip build).", image_name)
                    success = True
                    return BuildResult(
                        ok=True,
                        image_name=image_name,
                        image_id=img.id,
                        rc=0,
                        duration_s=time.time() - t0,
                        stderr_tail="",
                        stdout_tail="",
                    )
            except ImageNotFound:
                logger.info("Docker image '%s' not found locally. Building with buildx.", image_name)

            # Check if buildx is available
            def _check_buildx_available() -> None:
                # Note: docker path is trusted system path
                result = subprocess.run(  # noqa: S603
                    ["/usr/bin/docker", "buildx", "version"], capture_output=True, text=True, timeout=10, check=False
                )
                if result.returncode != 0:
                    raise RuntimeError("buildx not available")

            try:
                _check_buildx_available()
            except (subprocess.TimeoutExpired, FileNotFoundError, RuntimeError) as exc:
                raise RuntimeError("docker buildx not available") from exc

            # Create build context tarball
            tar_bytes = self._get_context_bytes(probe=probe)

            # Build buildx command
            # Note: cmd is constructed from trusted inputs (build args, image names, etc.)
            cmd = ["/usr/bin/docker", "buildx", "build"]

            # Add load flag to ensure image is available locally
            cmd.extend(["--load"])

            # Add progress flag for stable output
            cmd.extend(["--progress=plain"])

            # Add tag
            cmd.extend(["-t", image_name])

            # Add build args
            for key, value in build_args.items():
                cmd.extend(["--build-arg", f"{key}={value}"])
            cmd.extend(["--build-arg", "BUILDKIT_INLINE_CACHE=1"])

            # Add labels
            for key, value in run_labels.items():
                cmd.extend(["--label", f"{key}={value}"])

            # Add target
            cmd.extend(["--target", target])

            # Add pull flag
            if pull:
                cmd.append("--pull")

            # Add network mode
            if network_mode := os.environ.get("DOCKER_NETWORK_MODE"):
                cmd.extend(["--network", network_mode])

            # Add cache configuration
            cache_from_list = []
            if base_image := os.environ.get("DOCKER_CACHE_FROM"):
                logger.info("Using DOCKER_CACHE_FROM='%s' for build cache.", base_image)
                cache_from_list.append(base_image)

            if s3_cache_config and os.environ.get("DOCKER_S3_CACHE_READ", "0") in ("1", "true", "yes"):
                s3_cache_mount = (
                    f"type=s3,bucket={s3_cache_config['bucket']},"
                    f"region={s3_cache_config['region']},"
                    f"prefix={s3_cache_config['prefix']}"
                )
                cache_from_list.append(s3_cache_mount)
                logger.info("Using S3 cache: %s", s3_cache_mount)

            # Add cache-from flags
            for cache_source in cache_from_list:
                cmd.extend(["--cache-from", cache_source])

            # Add cache-to for S3 cache (export cache for future builds)
            if s3_cache_config and os.environ.get("DOCKER_S3_CACHE_WRITE", "0") in ("1", "true", "yes"):
                s3_cache_to = f"type=s3,bucket={s3_cache_config['bucket']},region={s3_cache_config['region']},prefix={s3_cache_config['prefix']},mode=max"
                cmd.extend(["--cache-to", s3_cache_to])

            # Add context from stdin
            cmd.append("-")

            logger.info("$ %s", " ".join(cmd).replace("\n", " "))

            # Execute buildx with timeout and streaming
            stdout_buf: deque[str] = deque(maxlen=2000)
            stderr_buf: deque[str] = deque(maxlen=2000)

            # Note: cmd is constructed from trusted inputs (build args, image names, etc.)
            with subprocess.Popen(  # noqa: S603
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=False,
                bufsize=1,
            ) as proc:
                # Send tarball via stdin
                if proc.stdin is not None:
                    proc.stdin.write(tar_bytes)
                    proc.stdin.close()
                error_seen = None

                try:
                    while True:
                        # Time check
                        if time.time() - t0 > timeout_s:
                            error_seen = "[TIMEOUT]"
                            proc.terminate()
                            break

                        # Read output line by line
                        if proc.stdout is None:
                            break
                        line_bytes = proc.stdout.readline()
                        if not line_bytes:
                            # Process finished
                            break

                        line = line_bytes.decode("utf-8", errors="ignore")
                        stdout_buf.append(line)

                        # Check for error patterns in output
                        if "error" in line.lower() or "failed" in line.lower():
                            stderr_buf.append(line)

                except Exception as e:
                    error_seen = f"Process error: {e}"
                    proc.terminate()

                # Wait for process to complete
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    error_seen = "[TIMEOUT]"

            # Check final return code
            if proc.returncode != 0 and not error_seen:
                error_seen = f"buildx failed with exit code {proc.returncode}"

            duration = time.time() - t0

            # Success path: ensure image exists
            if not error_seen:
                try:
                    img = client.images.get(image_name)
                    logger.info("buildx build completed successfully for '%s' in %.1f sec.", image_name, duration)
                    success = True
                    return BuildResult(
                        ok=True,
                        image_name=image_name,
                        image_id=img.id,
                        rc=0,
                        duration_s=duration,
                        stderr_tail="".join(stderr_buf)[-tail_chars:],
                        stdout_tail="".join(stdout_buf)[-tail_chars:],
                    )
                except ImageNotFound:
                    error_seen = "buildx build completed but image not found"

            # Failure
            rc = 124 if error_seen == "[TIMEOUT]" else 1
            logger.error(
                "buildx build failed for '%s' in %.1f sec: [%s][%s]",
                image_name,
                duration,
                error_seen or "unknown",
                "".join(stdout_buf)[-100:] if stdout_buf else "",
            )
            success = False
            return BuildResult(
                ok=False,
                image_name=image_name,
                image_id=None,
                rc=rc,
                duration_s=duration,
                stderr_tail="".join(stderr_buf)[-tail_chars:] or (error_seen or "")[-tail_chars:],
                stdout_tail="".join(stdout_buf)[-tail_chars:],
            )

        finally:
            if delete_img or (not success):
                try:
                    img = client.images.get(image_name)
                    logger.debug("Deleting image '%s' after buildx build.", image_name)
                    client.images.remove(image=img.id, force=True)
                except ImageNotFound:
                    pass
                except DockerException:
                    logger.exception("Failed to delete image '%s' after buildx build.", image_name)

    def build_container_streaming(  # noqa: C901
        self,
        client: docker.DockerClient,
        image_name: str,
        build_args: dict[str, str],
        run_labels: dict[str, str] | None = None,
        probe: bool = False,
        *,
        force: bool = False,
        delete_img: bool = False,
        timeout_s: float = float("inf"),
        tail_chars: int = 4000,
        pull: bool = False,
        s3_cache_config: S3DockerCacheManager | dict[str, str] | None = None,
        use_buildx: bool | None = None,
    ) -> BuildResult:
        """
        Build with streamed logs, tail capture, and a wall-clock timeout.
        Returns a BuildResult and does NOT raise for typical failures (so callers can
        report immediately).

        Changes vs previous version:
        - Reuses a cached, reproducible tarball to avoid per-build tarring & cache drift.
        - Uses a fresh low-level API client per call to avoid connection contention in ThreadPools.
        - Optional buildx support for advanced caching and multi-platform builds.

        Args:
            use_buildx: If True, use docker buildx; if False, use SDK; if None, auto-detect
        """
        if isinstance(s3_cache_config, S3DockerCacheManager):
            s3_cache_config = s3_cache_config.get_cache_mount_config(
                dockerfile_content=self.dockerfile_data,
                build_args=build_args,
            )
        elif (s3_cache_config is None) and (os.environ.get("AWS_S3_CACHE_BUCKET")):
            s3_cache_config = {
                "bucket": os.environ["AWS_S3_BUCKET_DOCKER"],
                "region": os.environ.get("AWS_REGION", "us-east-1"),
                "prefix": os.environ.get("AWS_S3_BUCKET_DOCKER_PREFIX", "docker-cache"),
            }

        # Determine whether to use buildx
        if use_buildx is None:
            use_buildx = os.environ.get("DOCKER_USE_BUILDX", "").lower() in ("1", "true", "yes")
        # Route to buildx if requested and available
        if use_buildx:
            try:
                return self._build_with_buildx(
                    client=client,
                    image_name=image_name,
                    build_args=build_args,
                    run_labels=run_labels,
                    probe=probe,
                    force=force,
                    delete_img=delete_img,
                    timeout_s=timeout_s,
                    tail_chars=tail_chars,
                    pull=pull,
                    s3_cache_config=s3_cache_config,
                )
            except Exception as e:
                logger.warning("buildx build failed, falling back to SDK: %s", e)
                # Fall through to SDK build

        run_labels = run_labels if run_labels else {}
        _, target = self.process_image_name(image_name)
        t0 = time.time()
        success = False
        try:
            # Fast path: respect existing image when not forcing
            try:
                img = client.images.get(image_name)
                if force:
                    logger.info("Force rebuild requested. Removing '%s'.", image_name)
                    with contextlib.suppress(Exception):
                        client.images.remove(image=img.id, force=True)
                else:
                    logger.info("Docker image '%s' found locally (skip build).", image_name)
                    success = True
                    return BuildResult(
                        ok=True,
                        image_name=image_name,
                        image_id=img.id,
                        rc=0,
                        duration_s=time.time() - t0,
                        stderr_tail="",
                        stdout_tail="",
                    )
            except ImageNotFound:
                logger.info("Docker image '%s' not found locally. Building.", image_name)

            # Streamed build via fresh low-level API client
            api = _new_api_client(client)
            tar_bytes = self._get_context_bytes(probe=probe)
            stdout_buf: deque[str] = deque(maxlen=2000)  # chunk-tail buffers
            stderr_buf: deque[str] = deque(maxlen=2000)

            cache_from = None
            if base_image := os.environ.get("DOCKER_CACHE_FROM", None):
                logger.info("Using DOCKER_CACHE_FROM='%s' for build cache.", base_image)
                build_args = {**build_args, "BASE_IMAGE": base_image}
                cache_from = [base_image]

            # Ensure all build-arg values are strings (Docker expects strings)
            safe_build_args: dict[str, str] = {}
            for k, v in (build_args or {}).items():
                if isinstance(v, str):
                    safe_build_args[k] = v
                elif isinstance(v, (bytes, bytearray)):
                    try:
                        safe_build_args[k] = v.decode("utf-8", errors="replace")
                    except Exception:
                        safe_build_args[k] = str(v)
                else:
                    # JSON-encode non-string args (e.g., lists/dicts)
                    try:
                        safe_build_args[k] = json.dumps(v)
                    except Exception:
                        safe_build_args[k] = str(v)

            # Pretty log line for transparency
            if safe_build_args:
                build_args_str = " --build-arg ".join(f"{k}='{v}'" for k, v in safe_build_args.items())
                logger.info("$ docker build -t %s . --build-arg %s", image_name, build_args_str)
            else:
                logger.info("$ docker build -t %s .", image_name)

            # Prepare cache configuration
            cache_from_list = None
            if s3_cache_config:
                cache_from_list = cache_from if cache_from else []
                # Add S3 cache mount to cache_from
                s3_cache_mount = (
                    f"type=s3,bucket={s3_cache_config['bucket']},"
                    f"region={s3_cache_config['region']},"
                    f"prefix={s3_cache_config['prefix']}"
                )
                cache_from_list.append(s3_cache_mount)
                logger.info("Using S3 cache: %s", s3_cache_mount)

            # Try build with cache first, then retry without cache if we hit a broken cache error
            error_seen = None
            duration = 0.0
            for attempt in range(2):
                nocache_param = attempt == 1  # Second attempt uses nocache=True
                if nocache_param:
                    logger.warning("Retrying build for '%s' with nocache=True due to broken cache error", image_name)

                try:
                    stream = api.build(
                        fileobj=io.BytesIO(tar_bytes),
                        custom_context=True,
                        tag=image_name,
                        buildargs={**safe_build_args, "BUILDKIT_INLINE_CACHE": "1"},
                        decode=True,
                        rm=True,
                        pull=pull,
                        target=target,
                        labels=run_labels,
                        network_mode=os.environ.get("DOCKER_NETWORK_MODE", None),
                        cache_from=cache_from_list if not nocache_param else None,
                        nocache=nocache_param,
                    )
                except DockerException:
                    logger.exception("Failed to initiate build for '%s'", image_name)
                    success = False
                    return BuildResult(
                        ok=False,
                        image_name=image_name,
                        image_id=None,
                        rc=1,
                        duration_s=time.time() - t0,
                        stderr_tail="",
                        stdout_tail="",
                    )

                try:
                    for chunk in stream:
                        # Time check first
                        if time.time() - t0 > timeout_s:
                            error_seen = "[TIMEOUT]"
                            break

                        # Typical keys: 'stream', 'status', 'error', 'errorDetail'
                        if chunk.get("stream"):
                            s = str(chunk["stream"])
                            if s:
                                stdout_buf.append(s)
                        if "status" in chunk and chunk.get("progressDetail"):
                            s = str(chunk.get("status", ""))
                            if s:
                                stdout_buf.append(s + "\n")
                        if "error" in chunk or "errorDetail" in chunk:
                            error_seen = (chunk.get("error") or str(chunk.get("errorDetail", ""))).strip()
                            if error_seen:
                                stderr_buf.append(error_seen + "\n")
                            break
                except APIError:
                    logger.exception("Build stream APIError for '%s'", image_name)
                    error_seen = "APIError during build"

                duration = time.time() - t0

                # Success path: ensure image exists
                if not error_seen:
                    try:
                        img = client.images.get(image_name)
                        logger.info("Build completed successfully for '%s' in %.1f sec.", image_name, duration)
                        success = True
                        return BuildResult(
                            ok=True,
                            image_name=image_name,
                            image_id=img.id,
                            rc=0,
                            duration_s=duration,
                            stderr_tail="".join(stderr_buf)[-tail_chars:],
                            stdout_tail="".join(stdout_buf)[-tail_chars:],
                        )
                    except ImageNotFound:
                        error_seen = "Build completed but image not found"

                # Check if this is a broken cache error that we should retry
                if attempt == 0 and error_seen and "unable to find image" in error_seen.lower():
                    logger.warning(
                        "Detected broken cache error for '%s': %s. Will retry with nocache=True.",
                        image_name,
                        error_seen,
                    )
                    # Clear buffers for retry
                    stdout_buf.clear()
                    stderr_buf.clear()
                    continue  # Retry with nocache=True

                # Either successful (returned above) or failed with non-retryable error
                break

            # Failure (exhausted retries or non-retryable error)
            rc = 124 if error_seen == "[TIMEOUT]" else 1
            logger.error(
                "Build failed for '%s' in %.1f sec: [%s][%s]",
                image_name,
                duration,
                error_seen or "unknown",
                "".join(stdout_buf)[-100:] if stdout_buf else "",
            )
            success = False
            return BuildResult(
                ok=False,
                image_name=image_name,
                image_id=None,
                rc=rc,
                duration_s=duration,
                stderr_tail="".join(stderr_buf)[-tail_chars:] or (error_seen or "")[-tail_chars:],
                stdout_tail="".join(stdout_buf)[-tail_chars:],
            )
        finally:
            if delete_img or (not success):
                try:
                    img = client.images.get(image_name)
                    logger.debug("Deleting image '%s' after build.", image_name)
                    client.images.remove(image=img.id, force=True)
                except ImageNotFound:
                    pass
                except DockerException:
                    logger.exception("Failed to delete image '%s' after build.", image_name)
                except Exception:
                    logger.exception("Unexpected error deleting image '%s' after build.", image_name)

    def build_and_publish_to_dockerhub(
        self,
        client: docker.DockerClient,
        task: Task,
        namespace: str,
        *,
        repository_mode: str = "single",  # "single" or "mirror"
        single_repo: str = "all",
        dockerhub_repo_prefix: str | None = None,
        skip_existing: bool = True,
        parallelism: int = 1,
        force: bool = False,
        run_labels: dict[str, str] | None = None,
        timeout_s: float = 15 * 60,
        tail_chars: int = 10_000,
        pull: bool = False,
        use_buildx: bool | None = None,
        username: str | None = None,
        password: str | None = None,
    ) -> tuple[BuildResult, dict[str, str]]:
        """
        Build the Docker image for ``task`` and publish it to DockerHub.

        Returns (BuildResult, {local_ref: dockerhub_ref}). If the build fails,
        the push step is skipped and the mapping is empty.

        Args:
            client: Docker client instance
            task: Task to build and publish
            namespace: DockerHub namespace (username or organization)
            repository_mode: "single" (all images in one repo) or "mirror" (one repo per image)
            single_repo: Repository name for single mode
            dockerhub_repo_prefix: Prefix for mirror mode repos
            skip_existing: Skip pushing images that already exist
            parallelism: Number of concurrent push operations
            force: Force rebuild even if image exists
            run_labels: Docker labels for the container
            timeout_s: Build timeout in seconds
            tail_chars: Number of characters to keep from build logs
            pull: Pull base image before building
            use_buildx: Use Docker BuildKit/buildx
            username: DockerHub username (or from DOCKERHUB_USERNAME env)
            password: DockerHub password/token (or from DOCKERHUB_TOKEN env)

        Returns:
            Tuple of (BuildResult, push_results_dict)
        """
        if task.sha is None and task.tag in {"pkg", "run"}:
            raise ValueError("Task.sha must be set for building package/run images")

        image_name = task.get_image_name()
        repo_url = f"https://www.github.com/{task.owner}/{task.repo}"
        build_args: dict[str, str] = {"REPO_URL": repo_url}
        if task.sha is not None:
            build_args["COMMIT_SHA"] = task.sha
        if getattr(task, "env_payload", ""):
            build_args["ENV_PAYLOAD"] = task.env_payload
        if getattr(task, "python_version", ""):
            build_args["PY_VERSION"] = task.python_version
        if getattr(task, "benchmarks", ""):
            build_args["BENCHMARKS"] = task.benchmarks

        if run_labels is None:
            run_labels = {
                "datasmith.task": f"{task.owner}/{task.repo}",
                "datasmith.sha": task.sha or "unknown",
                "datasmith.run": "publish",
            }

        logger.info("Building image %s for DockerHub publish", image_name)
        build_res = self.build_container_streaming(
            client=client,
            image_name=image_name,
            build_args=build_args,
            run_labels=run_labels,
            probe=False,
            force=force,
            delete_img=False,
            timeout_s=timeout_s,
            tail_chars=tail_chars,
            pull=pull,
            s3_cache_config=None,
            use_buildx=use_buildx,
        )

        if not build_res.ok:
            logger.error(
                "Build failed for %s (rc=%s); skipping DockerHub publish.",
                image_name,
                build_res.rc,
            )
            return build_res, {}

        logger.info("Build succeeded for %s; publishing to DockerHub (namespace=%s)", image_name, namespace)
        push_results = publish_images_to_dockerhub(
            local_refs=[image_name],
            namespace=namespace,
            repository_mode=repository_mode,
            single_repo=single_repo,
            dockerhub_repo_prefix=dockerhub_repo_prefix,
            skip_existing=skip_existing,
            verbose=True,
            parallelism=parallelism,
            docker_client=client,
            username=username,
            password=password,
        )

        if image_name in push_results:
            logger.info("Published %s to %s", image_name, push_results[image_name])
        else:
            logger.warning("DockerHub publish did not return mapping for %s", image_name)

        return build_res, push_results

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable mapping of this context's contents."""
        return {
            "dockerfile_data": self.dockerfile_data,
            "entrypoint_data": self.entrypoint_data,
            "building_data": self.building_data,
            "env_building_data": self.env_building_data,
            "base_building_data": self.base_building_data,
            "run_building_data": self.run_building_data,
            "profile_data": self.profile_data,
            "run_tests_data": self.run_tests_data,
            "final_building_data": self.final_building_data,
            "created_unix": self.created_unix,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> DockerContext:
        """
        Construct a DockerContext from a mapping. Missing keys fall back to the
        default files via the DockerContext __init__ (which accepts None).
        """
        return cls(
            dockerfile_data=data.get("dockerfile_data"),
            entrypoint_data=data.get("entrypoint_data"),
            building_data=data.get("building_data", None),
            env_building_data=data.get("env_building_data", None),
            base_building_data=data.get("base_building_data", None),
            run_building_data=data.get("run_building_data", None),
            profile_data=data.get("profile_data", None),
            run_tests_data=data.get("run_tests_data", None),
            final_building_data=data.get("final_building_data", None),
            created_unix=float(data.get("created_unix", 0.0) or 0.0),
        )

    def __hash__(self) -> int:
        return hash((
            self.dockerfile_data,
            self.entrypoint_data,
            self.building_data,
            self.env_building_data,
            self.base_building_data,
            self.run_building_data,
            self.profile_data,
            self.run_tests_data,
            self.final_building_data,
        ))


class ContextRegistry:
    """Registry for Docker contexts keyed by owner/repo[/sha], independent of tag.

    Input key format (required): "owner/repo[/sha]:{tag}", where {tag} ∈ {"env","pkg"}.
    The `tag` is validated and preserved on returned `Task`s, but **ignored for storage**;
    all contexts are stored under a canonical key with tag='pkg'.
    """

    VALID_TAGS: ClassVar[set[str]] = {"env", "pkg", "run", "base", "final"}

    def __init__(self, registry: dict[Task, DockerContext] | None = None, default_context: DockerContext | None = None):
        if registry is None:
            registry = {}
        self.registry = registry
        self._lock = threading.Lock()

        if default_context is None:
            default_context = DockerContext()

        # Single default context (canonicalized to tag='pkg')
        default_task_canonical = Task.default_task()
        if default_task_canonical not in self.registry:
            self.registry[default_task_canonical] = default_context
        logger.debug("Default Docker context initialized (single canonical context).")

    @staticmethod
    def _canonicalize(task: Task) -> Task:
        """Return a copy of Task with tag='pkg' for registry keying."""
        if task.tag == "pkg":
            return task
        return task.with_tag("pkg")

    def _canonicalize_from_key(self, key: str | Task) -> Task:
        """Parse if needed, then canonicalize to tag='pkg' for dict keying."""
        t = self.parse_key(key) if isinstance(key, str) else key
        return self._canonicalize(t)

    def get_default(self, tag: str = "pkg") -> tuple[Task, DockerContext]:
        with self._lock:
            if tag not in self.VALID_TAGS:
                raise ValueError(f"Unknown tag '{tag}'. Valid tags: {sorted(self.VALID_TAGS)}")
            # lookup under canonical default; return Task with requested tag
            user_task = Task(owner="default", repo="default", sha=None, tag=tag)
            canonical = self._canonicalize(user_task)
            ctx = copy.deepcopy(self.registry[canonical])
            return user_task, ctx

    def get_lock(self) -> threading.Lock:
        return self._lock

    def parse_key(self, key: str | Task) -> Task:
        """Parse 'owner/repo[/sha]:{tag}' into a Task. Tag is required and validated."""
        if isinstance(key, Task):
            return key  # already parsed

        # Hard assertion per request: all keys MUST include a ':tag'
        assert ":" in key and key.rsplit(":", 1)[1], "All keys must include a ':tag' suffix (e.g., ':env' or ':pkg')."  # noqa: S101

        prefix, tag = key.rsplit(":", 1)
        tag = tag.strip()
        if tag not in self.VALID_TAGS:
            raise ValueError(f"Unknown tag '{tag}'. Valid tags: {sorted(self.VALID_TAGS)}")

        parts = prefix.split("/")
        if not (2 <= len(parts) <= 3):
            raise ValueError("Key must be 'owner/repo[:tag]' or 'owner/repo/sha[:tag]'")

        owner, repo = parts[0], parts[1]
        sha = None if len(parts) != 3 else parts[2]

        date_unix = 0.0
        if sha:
            try:
                logger.debug(f"Fetching commit info for {owner}/{repo}@{sha}")
                commit_info = _get_commit_info(f"{owner}/{repo}", sha)
                date_iso = commit_info["date"]
                date_unix = datetime.datetime.fromisoformat(date_iso.replace("Z", "+00:00")).timestamp()
            except Exception as exc:
                logger.warning("Failed to fetch commit info for %s/%s@%s: %s", owner, repo, sha, exc)
                date_unix = 0.0

        return Task(owner=owner, repo=repo, sha=sha, commit_date=date_unix, tag=tag)

    def register(self, key: str | Task, context: DockerContext) -> None:
        """Register a new Docker context. Stored under canonical (tag='pkg')."""
        t = self.parse_key(key) if isinstance(key, str) else key
        canonical = self._canonicalize(t)
        if canonical in self.registry:
            logger.warning(f"Context '{canonical}' is already registered, overwriting.")

        # if the tag is "env" and we already have a "pkg" version, warn the user
        # and instead of changing the context completely, overwrite all files
        # except the building_data (which are pkg-specific)
        if t.tag == "env" and canonical in self.registry:
            existing = self.registry[canonical]
            context = DockerContext(
                dockerfile_data=context.dockerfile_data,
                entrypoint_data=context.entrypoint_data,
                env_building_data=context.env_building_data,
                building_data=existing.building_data,
                base_building_data=context.base_building_data,
                run_building_data=context.run_building_data,
                profile_data=context.profile_data,
                run_tests_data=existing.run_tests_data,
            )
            logger.warning(
                f"Registering 'env' context for '{canonical}' which already has a 'pkg' version; preserving 'pkg' building_data."
            )
        # Update creation timestamp on every registration
        try:
            context.created_unix = float(time.time())
        except Exception:
            context.created_unix = 0.0

        self.registry[canonical] = context
        logger.debug(f"Registered Docker context under canonical key: {canonical}")

    def get(self, key: str | Task) -> DockerContext:
        """
        Retrieve a Docker context by key using hierarchical matching (tag-insensitive).
        'owner/repo/sha:tag' queries in-order:
            1) owner/repo/sha (canonical key, tag='pkg')
            2) owner/repo     (canonical key, tag='pkg')
            3) default        (canonical key, tag='pkg')
        """
        with self._lock:
            # Keep the user's tag but look up under canonical keys
            user_task = self.parse_key(key) if isinstance(key, str) else key
            canonical = self._canonicalize(user_task)

            # exact match first (canonical)
            if canonical.sha is not None and canonical in self.registry:
                logger.debug(f"Found exact context for key '{user_task}' via '{canonical}'.")
                return self.registry[canonical]

            # owner/repo base (canonical)
            base = Task(owner=canonical.owner, repo=canonical.repo, sha=None, tag="pkg")
            if base in self.registry:
                logger.debug(f"Found fallback context '{base}' for key '{user_task}'.")
                return self.registry[base]

            logger.info(f"No context found for key '{user_task}'. Using default context.")
            return self.registry[Task(owner="default", repo="default", sha=None, tag="pkg")]

    def pop(self, key: str | Task) -> DockerContext | None:
        """Remove a context by key (canonicalized to tag='pkg')."""
        with self._lock:
            user_task = self.parse_key(key) if isinstance(key, str) else key
            canonical = self._canonicalize(user_task)
            if canonical in self.registry:
                logger.debug(f"Popping context for key '{user_task}' via '{canonical}'.")
                return self.registry.pop(canonical)
            if user_task in self.registry:
                logger.debug(f"Popping context for key '{user_task}' directly.")
                return self.registry.pop(user_task)
            logger.debug(f"No context found to pop for key '{user_task}'.")
            return None

    def get_similar(self, key: str | Task) -> list[tuple[Task, DockerContext]]:  # noqa: C901
        """
        Retrieve contexts similar to a key, constrained to SAME owner/repo (tag-insensitive).
        Order:
          1) exact match (if present)  — returned Task uses the caller's tag
          2) other SHAs for owner/repo — returned Tasks use the caller's tag
             sorted by |commit_date diff| if available, else by SHA
          3) base owner/repo           — returned Tasks use the caller's tag
        """
        with self._lock:
            user_task = self.parse_key(key) if isinstance(key, str) else key
            canonical = self._canonicalize(user_task)

            results: list[tuple[Task, DockerContext]] = []
            seen_canonical: set[Task] = set()

            # 1) Exact match (if present)
            if canonical in self.registry:
                results.append((canonical.with_tag(user_task.tag), self.registry[canonical]))
                seen_canonical.add(canonical)

            # 2) Other SHAs for same owner/repo (canonical keys in registry)
            candidates: list[tuple[Task, DockerContext]] = []
            for t, ctx in self.registry.items():
                if t in seen_canonical:
                    continue
                if t.owner == canonical.owner and t.repo == canonical.repo and t.sha is not None:
                    candidates.append((t, ctx))

            has_valid_commit_date = (
                getattr(canonical, "sha", None) is not None and getattr(canonical, "commit_date", None) is not None
            )
            if has_valid_commit_date:

                def _sort(item: tuple[Task, DockerContext]) -> tuple[float, str]:
                    t, _ = item
                    cand_cd = getattr(t, "commit_date", None)
                    if cand_cd is None:
                        return (float("inf"), str(t.sha))
                    try:
                        return (abs(canonical.commit_date - cand_cd), str(t.sha))
                    except Exception:
                        return (float("inf"), str(t.sha))

                candidates.sort(key=_sort)
            else:
                candidates.sort(key=lambda item: str(item[0].sha))

            for t, ctx in candidates:
                if t not in seen_canonical:
                    # Present with the user's tag for downstream execution behavior
                    results.append((t.with_tag(user_task.tag), ctx))
                    seen_canonical.add(t)

            # 3) Base owner/repo
            base = Task(owner=canonical.owner, repo=canonical.repo, sha=None, tag="pkg")
            if base in self.registry and base not in seen_canonical:
                results.append((base.with_tag(user_task.tag), self.registry[base]))

            return results

    def __getitem__(self, key: str) -> DockerContext:
        return self.get(key)

    def __setitem__(self, key: str, context: DockerContext) -> None:
        self.register(key, context)

    def __contains__(self, key: str | Task) -> bool:
        canonical = self._canonicalize_from_key(key)
        return canonical in self.registry

    def save_to_file(self, path: Path) -> None:
        dat = self.serialize(pretty=True)
        with self._lock:
            path.write_text(dat)
        logger.info("Context registry saved to %s", path)

    @classmethod
    def load_from_file(cls, path: Path) -> ContextRegistry:
        dat = path.read_text()
        return cls.deserialize(dat)

    def serialize(self, *, pretty: bool = False) -> str:
        """
        Serialize the registry (including the canonical 'default' context) to a JSON string.
        The thread lock itself is not serialized; a fresh lock will be created when deserializing.
        """
        with self._lock:
            payload = {
                "version": 2,  # bumped: tag-insensitive storage
                "contexts": {repr(k): v.to_dict() for k, v in self.registry.items()},
            }
        return json.dumps(payload, indent=2 if pretty else None, sort_keys=pretty)

    @classmethod
    def deserialize(cls, payload: str) -> ContextRegistry:
        """
        Reconstruct a ContextRegistry from a JSON string produced by `serialize`.
        Ensures a canonical 'default' context exists.
        """
        data = json.loads(payload)
        raw = data.get("contexts", {})
        registry: dict[Task, DockerContext] = {eval(k): DockerContext.from_dict(v) for k, v in raw.items()}  # noqa: S307

        # Ensure canonical default exists
        default_task_canonical = Task(owner="default", repo="default", sha=None, tag="pkg")
        registry[default_task_canonical] = DockerContext()

        # Normalize any accidentally stored 'env' keys to canonical 'pkg'
        # (in case old payloads had per-tag entries)
        to_move: list[tuple[Task, DockerContext]] = []
        for t, ctx in list(registry.items()):
            if t.tag != "pkg":
                to_move.append((t, ctx))
        if to_move:
            logger.warning(
                "ContextRegistry.deserialize: Found %d non-canonical entries with tag!='pkg'; normalizing to tag='pkg'.",
                len(to_move),
            )
        for t, ctx in to_move:
            del registry[t]
            canonical = Task(owner=t.owner, repo=t.repo, sha=t.sha, commit_date=t.commit_date, tag="pkg")
            registry[canonical] = ctx

        return cls(registry=registry)

    # make helper methods to make cls pickle friendly
    def __getstate__(self) -> dict[str, Any]:
        """Prepare the state for pickling."""
        serialized_str = self.serialize(pretty=False)
        return json.loads(serialized_str)  #  type: ignore[no-any-return]

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Restore the state from pickling."""
        serialized_str = json.dumps(state)
        obj = self.deserialize(serialized_str)
        self.registry = obj.registry
        self._lock = threading.Lock()
