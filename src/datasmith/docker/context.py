from __future__ import annotations

import contextlib
import datetime
import io
import json
import tarfile
import threading
import time
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import docker
from docker.errors import APIError, DockerException, ImageNotFound

from datasmith.execution.utils import _get_commit_info
from datasmith.logging_config import get_logger

logger = get_logger("docker.context")


@dataclass
class BuildResult:
    ok: bool
    image_name: str
    image_id: str | None
    rc: int  # 0 ok, 124 timeout, 1 generic failure
    duration_s: float
    stderr_tail: str  # tail of error-ish build logs
    stdout_tail: str  # tail of normal build stream (may help triage)


@dataclass(frozen=True)
class Task:
    owner: str
    repo: str
    sha: str | None = None
    commit_date: float = 0.0
    kind: str = "asv"


class DockerContext:
    """
    A docker context stores all the necessary files to build a docker container
    for running ASV benchmarks. It includes the Dockerfile, entrypoint script,
    and a script to build the container.

    This allows customizing the Docker image without needing to modify the Dockerfile directly.
    """

    default_dockerfile_loc = Path(__file__).parent / "Dockerfile"
    default_entrypoint_loc = Path(__file__).parent / "entrypoint.sh"
    default_builder_loc = Path(__file__).parent / "docker_build.sh"
    default_probe_loc = Path(__file__).parent / "probe_build.sh"
    dockerfile_data: str
    entrypoint_data: str
    building_data: str
    probing_data: str

    def __init__(
        self,
        building_data: str | None = None,
        dockerfile_data: str | None = None,
        entrypoint_data: str | None = None,
        probing_data: str | None = None,
    ) -> None:
        if building_data is None:
            building_data = self.default_builder_loc.read_text()
        if dockerfile_data is None:
            dockerfile_data = self.default_dockerfile_loc.read_text()
        if entrypoint_data is None:
            entrypoint_data = self.default_entrypoint_loc.read_text()
        if probing_data is None:
            probing_data = self.default_probe_loc.read_text()

        self.building_data = building_data
        self.dockerfile_data = dockerfile_data
        self.entrypoint_data = entrypoint_data
        self.probing_data = probing_data

    def build_tarball_stream(self, probe: bool = False) -> io.BytesIO:
        tar_stream = io.BytesIO()
        with tarfile.open(fileobj=tar_stream, mode="w") as tar:
            # Add Dockerfile
            dockerfile_bytes = self.dockerfile_data.encode("utf-8")
            dockerfile_info = tarfile.TarInfo(name="Dockerfile")
            dockerfile_info.size = len(dockerfile_bytes)
            tar.addfile(dockerfile_info, io.BytesIO(dockerfile_bytes))

            # Add entrypoint.sh
            entrypoint_data = self.entrypoint_data.encode("utf-8")
            entrypoint_info = tarfile.TarInfo(name="entrypoint.sh")
            entrypoint_info.size = len(entrypoint_data)
            entrypoint_info.mode = 0o755  # Make it executable
            tar.addfile(entrypoint_info, io.BytesIO(entrypoint_data))

            # Add docker_build.sh
            building_data = self.probing_data.encode("utf-8") if probe else self.building_data.encode("utf-8")
            builder_info = tarfile.TarInfo(name="docker_build.sh")
            builder_info.size = len(building_data)
            builder_info.mode = 0o755  # Make it executable
            tar.addfile(builder_info, io.BytesIO(building_data))

        # Reset the stream position to the beginning
        tar_stream.seek(0)
        return tar_stream

    def build_container(
        self,
        client: docker.DockerClient,
        image_name: str,
        build_args: dict[str, str],
        force: bool = False,
        probe: bool = False,
    ) -> None:
        """Builds the Docker image if it does not exist or if force is True."""
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
            pass  # Image doesn't exist or was removed, proceed to build

        if not image_exists:
            if len(build_args):
                build_args_str = " --build-arg ".join(f"{k}={v}" for k, v in build_args.items())
                logger.info("$ docker build -t %s src/datasmith/docker/ --build-arg %s", image_name, build_args_str)
                try:
                    client.images.build(
                        fileobj=self.build_tarball_stream(probe=probe),
                        custom_context=True,
                        tag=image_name,
                        buildargs=build_args,
                    )
                except DockerException:
                    logger.exception("Failed to build Docker image '%s'", image_name)
            else:
                raise RuntimeError(f"Docker image '{image_name}' not found and no REPO_URL provided for build.")

        if not client.images.get(image_name):
            raise RuntimeError(f"Image '{image_name}' failed to build and is not found.")

    def build_container_streaming(  # noqa: C901
        self,
        client: docker.DockerClient,
        image_name: str,
        build_args: dict[str, str],
        probe: bool = False,
        *,
        force: bool = False,
        delete_img: bool = False,
        timeout_s: float = float("inf"),
        tail_chars: int = 4000,
        pull: bool = False,
    ) -> BuildResult:
        """
        SDK-only build with streamed logs, tail capture, and a wall-clock timeout.
        Returns a BuildResult and does NOT raise for typical failures (so callers can
        report immediately).
        """
        t0 = time.time()
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

            # Streamed build via low-level API for better control
            tar_stream = self.build_tarball_stream(probe=probe)
            stdout_buf: deque[str] = deque(maxlen=2000)  # chunk-tail buffers
            stderr_buf: deque[str] = deque(maxlen=2000)

            # Pretty log line for transparency
            if build_args:
                build_args_str = " --build-arg ".join(f"{k}={v}" for k, v in build_args.items())
                logger.info("$ docker build -t %s . --build-arg %s", image_name, build_args_str)
            else:
                logger.info("$ docker build -t %s .", image_name)

            try:
                stream = client.api.build(
                    fileobj=tar_stream,
                    custom_context=True,
                    tag=image_name,
                    buildargs=build_args,
                    decode=True,
                    rm=True,
                    pull=pull,
                )
            except DockerException:
                logger.exception("Failed to initiate build for '%s'", image_name)
                return BuildResult(
                    ok=False,
                    image_name=image_name,
                    image_id=None,
                    rc=1,
                    duration_s=time.time() - t0,
                    stderr_tail="",
                    stdout_tail="",
                )

            error_seen = None
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
                        # Status lines (pulling base layers, etc.)—treat as stdout
                        s = str(chunk.get("status", ""))
                        if s:
                            stdout_buf.append(s + "\n")
                    if "error" in chunk or "errorDetail" in chunk:
                        error_seen = (chunk.get("error") or str(chunk.get("errorDetail", ""))).strip()
                        if error_seen:
                            # also track in stderr tail
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

            # Failure
            rc = 124 if error_seen == "[TIMEOUT]" else 1
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
            if delete_img:
                try:
                    img = client.images.get(image_name)
                    logger.debug("Deleting image '%s' after build.", image_name)
                    client.images.remove(image=img.id, force=True)
                except ImageNotFound:
                    pass
                except DockerException:
                    logger.exception("Failed to delete image '%s' after build.", image_name)

    def to_dict(self) -> dict[str, str]:
        """Return a JSON-serializable mapping of this context's contents."""
        return {
            "dockerfile_data": self.dockerfile_data,
            "entrypoint_data": self.entrypoint_data,
            "building_data": self.building_data,
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
            building_data=data.get("building_data"),
        )


class ContextRegistry:
    """Registry for Docker contexts to avoid rebuilding the same context multiple times."""

    def __init__(self, registry: dict[Task, DockerContext] | None = None, default_context: DockerContext | None = None):
        if registry is None:
            registry = {}
        self.registry = registry
        self._lock = threading.Lock()

        if default_context is None:
            default_context = DockerContext()

        # ensure a default context for BOTH namespaces
        for k in ("asv", "asvprobe"):
            t = Task(owner="default", repo="default", sha=None, kind=k)
            if t not in self.registry:
                self.registry[t] = default_context
        logger.debug("Default Docker contexts initialized (asv + asvprobe).")

    def get_default(self, kind: str = "asv") -> tuple[Task, DockerContext]:
        task = Task(owner="default", repo="default", sha=None, kind=kind)
        return task, self.registry[task]

    def get_lock(self) -> threading.Lock:
        return self._lock

    def parse_key(self, key: str) -> Task:
        """Parse a string key into a Task object (now preserving 'asv' vs 'asvprobe')."""
        if not (
            key.startswith("asv/")
            or key.startswith("asv/default")
            or key.startswith("asvprobe/")
            or key.startswith("asvprobe/default")
        ):
            raise ValueError("Key must start with 'asv/' or 'asv/default' or 'asvprobe/' or 'asvprobe/default'")

        # Handle defaults like "asv/default-<repo>" and "asvprobe/default-<repo>"
        if key.startswith("asv/default") or key.startswith("asvprobe/default"):
            kind = "asvprobe" if key.startswith("asvprobe/") else "asv"
            parts = key.split("-")
            repo = parts[-1] if len(parts) > 2 else "default"
            return Task(owner="default", repo=repo, sha=None, commit_date=0.0, kind=kind)

        parts = key.split("/")
        if parts[0] not in ("asv", "asvprobe") or not (3 <= len(parts) <= 4):
            raise ValueError("Key must be 'asv/owner/repo[/sha]' or 'asvprobe/owner/repo[/sha]'")

        kind, owner, repo = parts[0], parts[1], parts[2]
        sha = None if len(parts) != 4 else parts[3]

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

        return Task(owner=owner, repo=repo, sha=sha, commit_date=date_unix, kind=kind)

    def register(self, key: str | Task, context: DockerContext) -> None:
        """Register a new Docker context."""
        if isinstance(key, str):
            key = self.parse_key(key)
        if key in self.registry:
            logger.warning(f"Context '{key}' is already registered, overwriting.")
        self.registry[key] = context
        logger.debug(f"Registered Docker context: {key}")

    def get(self, key: str | Task) -> DockerContext:
        """
        Retrieve a Docker context by key using hierarchical matching.
        "asv/astropy/astropy/14134" should query these queries in-order:
            "asv/astropy/astropy/14134"
            "asv/astropy/astropy"
        """
        if isinstance(key, str):
            key = self.parse_key(key)

        # exact match first
        if key.sha is not None and key in self.registry:
            logger.debug(f"Found exact context for key '{key}'.")
            return self.registry[key]

        # owner/repo base (same namespace!)
        base = Task(owner=key.owner, repo=key.repo, sha=None, kind=key.kind)
        if base in self.registry:
            logger.debug(f"Found fallback context '{base}' for key '{key}'.")
            return self.registry[base]

        logger.info(f"No context found for key '{key}'. Using default context for namespace '{key.kind}'.")
        return self.registry[Task(owner="default", repo="default", sha=None, kind=key.kind)]

    def get_similar(self, key: str | Task) -> list[tuple[Task, DockerContext]]:  # noqa: C901
        """
        Retrieve a list of Docker contexts by key using hierarchical matching.
        "asv/astropy/astropy/14134" should return contexts for these queries in-order:
            1) "asv/astropy/astropy/14134" (exact match, if present)
            2) Any others starting with "asv/astropy/astropy/" (e.g., "asv/astropy/astropy/abcdef")
                sorted by abs(key.commit_date / candidate.commit_date) if key.commit_date is not None else alphabetically
            3) "asv/astropy/astropy"      (owner/repo base, if present)
        Keys like "asv/astropy/otherrepo*" or "asv/otherowner/*" must NOT match.
        """
        if isinstance(key, str):
            key = self.parse_key(key)

        results: list[tuple[Task, DockerContext]] = []
        seen: set[Task] = set()

        # 1) Exact match (if present)
        if key in self.registry:
            results.append((key, self.registry[key]))
            seen.add(key)

        # 2) Other SHAs for same owner/repo *in the same namespace*
        candidates: list[tuple[Task, DockerContext]] = []
        for t, ctx in self.registry.items():
            if t in seen:
                continue
            if t.kind == key.kind and t.owner == key.owner and t.repo == key.repo and t.sha is not None:
                candidates.append((t, ctx))

        has_valid_commit_date = getattr(key, "sha", None) is not None and getattr(key, "commit_date", None) is not None
        if has_valid_commit_date:

            def _sort(item: tuple[Task, DockerContext]) -> tuple[float, str]:
                t, _ = item
                cand_cd = getattr(t, "commit_date", None)
                if cand_cd is None:
                    return (float("inf"), str(t.sha))
                try:
                    return (abs(key.commit_date - cand_cd), str(t.sha))
                except Exception:
                    return (float("inf"), str(t.sha))

            candidates.sort(key=_sort)
        else:
            candidates.sort(key=lambda item: str(item[0].sha))

        for t, ctx in candidates:
            if t not in seen:
                results.append((t, ctx))
                seen.add(t)

        # 3) Base owner/repo for the same namespace
        base = Task(owner=key.owner, repo=key.repo, sha=None, kind=key.kind)
        if base in self.registry and base not in seen:
            results.append((base, self.registry[base]))

        return results

    def __getitem__(self, key: str) -> DockerContext:
        return self.get(key)

    def __setitem__(self, key: str, context: DockerContext) -> None:
        self.register(key, context)

    def __contains__(self, key: str | Task) -> bool:
        if isinstance(key, str):
            key = self.parse_key(key)
        return key in self.registry

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
        Serialize the registry (including the 'default' context) to a JSON string.
        The thread lock itself is not serialized; a fresh lock will be created
        when deserializing.
        """
        with self._lock:
            payload = {
                "version": 1,
                "contexts": {repr(k): v.to_dict() for k, v in self.registry.items()},
            }
        return json.dumps(payload, indent=2 if pretty else None, sort_keys=pretty)

    @classmethod
    def deserialize(cls, payload: str) -> ContextRegistry:
        """
        Reconstruct a ContextRegistry from a JSON string produced by `serialize`.
        Ensures a 'default' context exists even if it wasn't present in the payload.
        """
        data = json.loads(payload)
        raw = data.get("contexts", {})
        registry: dict[Task, DockerContext] = {eval(k): DockerContext.from_dict(v) for k, v in raw.items()}  # noqa: S307

        # Ensure 'default' exists:
        if "default" not in registry:
            registry[Task(owner="default", repo="default", sha=None)] = DockerContext()

        return cls(registry=registry)
