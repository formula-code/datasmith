from __future__ import annotations

import contextlib
import datetime
import io
import json
import os
import re
import tarfile
import threading
import time
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

import docker
from docker.errors import APIError, DockerException, ImageNotFound

from datasmith.execution.utils import _get_commit_info
from datasmith.logging_config import get_logger

logger = get_logger("docker.context")


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
    tag: str = "pkg"  # 'pkg' (env + package) or 'env' (env-only)

    @staticmethod
    def _sanitize_component(s: str) -> str:
        """
        Sanitize a component for Docker image/container naming:
        - lowercase
        - keep only [a-z0-9._-]
        - collapse invalid runs to '-'
        - strip leading/trailing separators
        """
        s = s.lower()
        s = re.sub(r"[^a-z0-9._-]+", "-", s)
        s = s.strip("._-")
        return s or "unknown"

    def with_tag(self, tag: str) -> Task:
        """Return a new Task with the given tag."""
        if tag not in {"env", "pkg"}:
            raise ValueError(f"Tag must be either 'env' or 'pkg', got '{tag}'.")
        return Task(owner=self.owner, repo=self.repo, sha=self.sha, commit_date=self.commit_date, tag=tag)

    def get_image_name(self) -> str:
        """Return the Docker image name for this task (repo:tag)."""
        assert self.tag in {"env", "pkg"}, "Tag must be either 'env' or 'pkg'."  # noqa: S101

        owner = self._sanitize_component(self.owner)
        repo = self._sanitize_component(self.repo)
        sha_part = f"-{self._sanitize_component(self.sha)}" if self.sha else ""

        # New scheme: "owner-repo[-sha]:{tag}"
        image_repo = f"{owner}-{repo}{sha_part}"
        return f"{image_repo}:{self.tag}"

    def get_container_name(self) -> str:
        """Return a suitable (deterministic) Docker container name for this task."""
        assert self.tag in {"env", "pkg"}, "Tag must be either 'env' or 'pkg'."  # noqa: S101

        owner = self._sanitize_component(self.owner)
        repo = self._sanitize_component(self.repo)
        sha_part = f"-{self._sanitize_component(self.sha)}" if self.sha else ""
        tag_part = f"-{self._sanitize_component(self.tag)}"

        # Container names cannot contain ':'; allowed: [a-zA-Z0-9][a-zA-Z0-9_.-]
        # We keep it lowercase and deterministic.
        name = f"{owner}-{repo}{sha_part}{tag_part}"

        # Ensure starts with an alphanumeric character
        if not re.match(r"^[a-z0-9]", name):
            name = f"c-{name}"

        # Be conservative on length (Docker allows long names, but trim to 128 chars)
        return name[:128]


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
    default_docker_build_env_loc = Path(__file__).parent / "docker_build_env.sh"
    default_docker_build_pkg_loc = Path(__file__).parent / "docker_build_pkg.sh"
    dockerfile_data: str
    entrypoint_data: str
    env_building_data: str
    base_building_data: str
    building_data: str

    def __init__(
        self,
        building_data: str | None = None,
        dockerfile_data: str | None = None,
        entrypoint_data: str | None = None,
        env_building_data: str | None = None,
        base_building_data: str | None = None,
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

        self.dockerfile_data = dockerfile_data
        self.entrypoint_data = entrypoint_data
        self.env_building_data = env_building_data
        self.base_building_data = base_building_data
        self.building_data = building_data

    @staticmethod
    def add_bytes(tar: tarfile.TarFile, name: str, data: bytes, mode: int = 0o644) -> None:
        info = tarfile.TarInfo(name=name)
        info.size = len(data)
        info.mode = mode
        info.mtime = 0
        info.uid = info.gid = 0
        info.uname = info.gname = ""
        tar.addfile(info, io.BytesIO(data))

    def build_tarball_stream(self, probe: bool = False) -> io.BytesIO:
        tar_stream = io.BytesIO()
        with tarfile.open(fileobj=tar_stream, mode="w") as tar:
            # Add Dockerfile
            DockerContext.add_bytes(tar, "Dockerfile", self.dockerfile_data.encode("utf-8"))
            # Add entrypoint.sh
            DockerContext.add_bytes(tar, "entrypoint.sh", self.entrypoint_data.encode("utf-8"), mode=0o755)
            # Add docker_build_env.sh
            DockerContext.add_bytes(tar, "docker_build_env.sh", self.env_building_data.encode("utf-8"), mode=0o755)

            # Add docker_build_base.sh
            DockerContext.add_bytes(tar, "docker_build_base.sh", self.base_building_data.encode("utf-8"), mode=0o755)

            if not probe:
                # Add docker_build_pkg.sh
                DockerContext.add_bytes(tar, "docker_build_pkg.sh", self.building_data.encode("utf-8"), mode=0o755)

        # Reset the stream position to the beginning
        tar_stream.seek(0)
        return tar_stream

    def process_image_name(self, image_name: str) -> tuple[str, str]:
        """Split image name into (repo, target). Target is required."""
        assert ":" in image_name and image_name.rsplit(":", 1)[1], "Image name must include a ':target' suffix."  # noqa: S101
        repo, target = image_name.rsplit(":", 1)
        return repo, target

    def build_container(
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
            pass  # Image doesn't exist or was removed, proceed to build

        if not image_exists:
            cache_from = None
            if base_image := os.environ.get("DOCKER_CACHE_FROM", None):
                logger.info("Using DOCKER_CACHE_FROM='%s' for build cache.", base_image)
                build_args = {**build_args, "BASE_IMAGE": base_image}
                cache_from = [base_image]

            if len(build_args):
                build_args_str = " --build-arg ".join(f"{k}={v}" for k, v in build_args.items())
                logger.info("$ docker build -t %s src/datasmith/docker/ --build-arg %s", image_name, build_args_str)
                try:
                    client.images.build(
                        fileobj=self.build_tarball_stream(probe=probe),
                        custom_context=True,
                        tag=image_name,
                        buildargs={**build_args, "BUILDKIT_INLINE_CACHE": "1"},
                        target=target,
                        rm=True,
                        labels=run_labels,
                        network_mode=os.environ.get("DOCKER_NETWORK_MODE", None),
                        cache_from=cache_from,
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
        run_labels: dict[str, str] | None = None,
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

            # Streamed build via low-level API for better control
            tar_stream = self.build_tarball_stream(probe=probe)
            stdout_buf: deque[str] = deque(maxlen=2000)  # chunk-tail buffers
            stderr_buf: deque[str] = deque(maxlen=2000)

            cache_from = None
            if base_image := os.environ.get("DOCKER_CACHE_FROM", None):
                logger.info("Using DOCKER_CACHE_FROM='%s' for build cache.", base_image)
                build_args = {**build_args, "BASE_IMAGE": base_image}
                cache_from = [base_image]

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
                    buildargs={**build_args, "BUILDKIT_INLINE_CACHE": "1"},
                    decode=True,
                    rm=True,
                    pull=pull,
                    target=target,
                    labels=run_labels,
                    network_mode=os.environ.get("DOCKER_NETWORK_MODE", None),
                    cache_from=cache_from,
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

            # Failure
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

    def to_dict(self) -> dict[str, str]:
        """Return a JSON-serializable mapping of this context's contents."""
        return {
            "dockerfile_data": self.dockerfile_data,
            "entrypoint_data": self.entrypoint_data,
            "building_data": self.building_data,
            "env_building_data": self.env_building_data,
            "base_building_data": self.base_building_data,
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
        )

    def __hash__(self) -> int:
        return hash((
            self.dockerfile_data,
            self.entrypoint_data,
            self.building_data,
            self.env_building_data,
            self.base_building_data,
        ))


class ContextRegistry:
    """Registry for Docker contexts keyed by owner/repo[/sha], independent of tag.

    Input key format (required): "owner/repo[/sha]:{tag}", where {tag} ∈ {"env","pkg"}.
    The `tag` is validated and preserved on returned `Task`s, but **ignored for storage**;
    all contexts are stored under a canonical key with tag='pkg'.
    """

    VALID_TAGS: ClassVar[set[str]] = {"env", "pkg"}

    def __init__(self, registry: dict[Task, DockerContext] | None = None, default_context: DockerContext | None = None):
        if registry is None:
            registry = {}
        self.registry = registry
        self._lock = threading.Lock()

        if default_context is None:
            default_context = DockerContext()

        # Single default context (canonicalized to tag='pkg')
        default_task_canonical = Task(owner="default", repo="default", sha=None, tag="pkg")
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
        if tag not in self.VALID_TAGS:
            raise ValueError(f"Unknown tag '{tag}'. Valid tags: {sorted(self.VALID_TAGS)}")
        # lookup under canonical default; return Task with requested tag
        user_task = Task(owner="default", repo="default", sha=None, tag=tag)
        canonical = self._canonicalize(user_task)
        return user_task, self.registry[canonical]

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
        # except the building_data (which is pkg-specific)
        if t.tag == "env" and canonical in self.registry:
            existing = self.registry[canonical]
            context = DockerContext(
                dockerfile_data=context.dockerfile_data,
                entrypoint_data=context.entrypoint_data,
                env_building_data=context.env_building_data,
                building_data=existing.building_data,
            )
            logger.warning(
                f"Registering 'env' context for '{canonical}' which already has a 'pkg' version; preserving 'pkg' building_data."
            )
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

    def get_similar(self, key: str | Task) -> list[tuple[Task, DockerContext]]:  # noqa: C901
        """
        Retrieve contexts similar to a key, constrained to SAME owner/repo (tag-insensitive).
        Order:
          1) exact match (if present)  — returned Task uses the caller's tag
          2) other SHAs for owner/repo — returned Tasks use the caller's tag
             sorted by |commit_date diff| if available, else by SHA
          3) base owner/repo           — returned Task uses the caller's tag
        """
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
