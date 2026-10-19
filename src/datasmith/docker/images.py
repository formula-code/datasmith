"""Docker image management via python-on-whales."""

from __future__ import annotations

import hashlib
import os
import posixpath
import re
from pathlib import Path

from python_on_whales import DockerClient

from datasmith.utils import get_logger
from datasmith.utils.core import Settings

logger = get_logger("docker.images")

_TEMPLATES_DIR = Path(__file__).parent / "templates"

# Each tier (base, repo, PR) builds FROM a local image, so use the "default" docker
# driver builder; container builders cannot see locally built images.
_BUILDER = "default"


def _default_context() -> str:
    """Return the path to the built-in templates directory."""
    return str(_TEMPLATES_DIR)


def _docker_namespace() -> str:
    """Return the Docker namespace from settings (DOCKERHUB_USERNAME env var)."""
    return Settings().dockerhub_username


def get_base_image_name() -> str:
    """Return the canonical tag for the base image."""
    return f"{_docker_namespace()}/base:latest"


# Longest slug kept from a package root; the appended digest keeps roots distinct.
DATASMITH_IMAGE_ROOT_SLUG_CHARS: int = int(os.environ.get("DATASMITH_IMAGE_ROOT_SLUG_CHARS", "32"))


def _build_root_slug(build_root: str) -> str:
    """Return the tag component that names a non-default package root.

    The repository root yields "". Other roots become a sanitised slug plus a digest
    of the normalised root, so two roots that sanitise or truncate alike stay distinct.
    """
    root = posixpath.normpath(build_root.strip() or ".").lstrip("/")
    if root in ("", "."):
        return ""
    slug = re.sub(r"[^a-z0-9_.]+", "-", root.lower()).strip("-._")[:DATASMITH_IMAGE_ROOT_SLUG_CHARS].strip("-._")
    digest = hashlib.sha256(root.encode("utf-8")).hexdigest()[:8]
    return f"{slug}-{digest}" if slug else digest


def get_repo_image_name(owner: str, repo: str, py_version: str = "", build_root: str = "") -> str:
    """Return the canonical tag for a repository image.

    The tag includes the Python version and the package root because both are baked
    into the image and vary per commit. An empty ``py_version`` yields ``:latest``.
    """
    owner = owner.lower()
    repo = repo.lower()
    tag = f"py{py_version}" if py_version else "latest"
    slug = _build_root_slug(build_root)
    if slug:
        tag = f"{tag}-{slug}"
    return f"{_docker_namespace()}/{owner}-{repo}:{tag}".lower()


def get_pr_image_name(owner: str, repo: str, issue_number: int) -> str:
    """Return the canonical tag for a PR image."""
    return f"{_docker_namespace()}/{owner}-{repo}:{issue_number}".lower()


class ImageManager:
    def __init__(self, timeout: int = 3600) -> None:
        self._docker = DockerClient()
        self._timeout = timeout

    @staticmethod
    def _default_context() -> str:
        """Return the path to the built-in templates directory."""
        return _default_context()

    def build_base_image(
        self,
        context: str | None = None,
        *,
        py_version: str = "",
    ) -> str:
        ctx = context or _default_context()
        tag = get_base_image_name()
        logger.info("Building base image: %s", tag)
        kwargs: dict[str, object] = {
            "tags": [tag],
            "file": os.path.join(ctx, "Dockerfile.base"),
            "builder": _BUILDER,
        }
        if py_version:
            kwargs["build_args"] = {"PY_VERSION": py_version}
        self._docker.build(ctx, **kwargs)  # type: ignore[arg-type]
        return tag

    def build_repo_image(
        self,
        owner: str,
        repo: str,
        context: str | None = None,
        *,
        repo_url: str | None = None,
        py_version: str = "",
        build_root: str = ".",
    ) -> str:
        ctx = context or _default_context()
        url = repo_url or f"https://github.com/{owner}/{repo}.git"
        tag = get_repo_image_name(owner, repo, py_version, build_root)
        logger.info("Building repo image: %s", tag)
        build_args: dict[str, str] = {
            "BASE_IMAGE": get_base_image_name(),
            "REPO_URL": url,
        }
        # ``packages.primary_root`` may be null; default it here for every caller.
        build_args["BUILD_ROOT"] = build_root or "."
        if py_version:
            build_args["PY_VERSION"] = py_version
        self._docker.build(
            ctx,
            tags=[tag],
            file=os.path.join(ctx, "Dockerfile.repo"),
            build_args=build_args,
            builder=_BUILDER,
        )
        return tag

    def build_pr_image(
        self,
        owner: str,
        repo: str,
        issue_number: int,
        context: str | None = None,
        build_script: str = "",
        *,
        commit_sha: str = "HEAD",
        env_payload: str = "[]",
        py_version: str = "",
        build_root: str = "",
        benchmark_dest: str = "",
    ) -> str:
        ctx = context or _default_context()
        tag = get_pr_image_name(owner, repo, issue_number)
        # ``build_root`` selects the parent image only; its WORKDIR is already in that image.
        repo_image = get_repo_image_name(owner, repo, py_version, build_root)
        logger.info("Building PR image: %s", tag)
        build_args: dict[str, str] = {
            "REPO_IMAGE": repo_image,
            "COMMIT_SHA": commit_sha,
            "ENV_PAYLOAD": env_payload,
        }
        if build_script:
            build_args["BUILD_SCRIPT"] = build_script
        if py_version:
            build_args["PY_VERSION"] = py_version
        if benchmark_dest:
            # Passed only when set, so `docker history` shows tasks with an override.
            build_args["BENCHMARK_DEST"] = benchmark_dest
        self._docker.build(
            ctx,
            tags=[tag],
            file=os.path.join(ctx, "Dockerfile.pr"),
            build_args=build_args,
            builder=_BUILDER,
        )
        return tag

    def image_exists(self, tag: str) -> bool:
        try:
            self._docker.image.inspect(tag)
        except Exception:
            return False
        else:
            return True

    def remove_image(self, tag: str) -> None:
        try:
            self._docker.image.remove(tag, force=True)
        except Exception:
            logger.warning("Failed to remove image: %s", tag)

    def prune_dangling(self) -> None:
        self._docker.image.prune(all=False)
