"""Docker image management via python-on-whales."""

from __future__ import annotations

import os
from pathlib import Path

from python_on_whales import DockerClient

from datasmith.utils import get_logger
from datasmith.utils.core import Settings

logger = get_logger("docker.images")

_TEMPLATES_DIR = Path(__file__).parent / "templates"

# The three-tier hierarchy (base -> repo -> PR) requires each image to be
# available locally for the next FROM.  The built-in "default" builder uses
# the docker driver, which builds directly in the daemon's image store.
# Container-based builders (docker-container) run in isolation and cannot
# resolve locally-built images, so we pin to the docker driver here.
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


def get_repo_image_name(owner: str, repo: str) -> str:
    """Return the canonical tag for a repository image."""
    return f"{_docker_namespace()}/{owner}-{repo}:latest"


def get_pr_image_name(owner: str, repo: str, issue_number: int) -> str:
    """Return the canonical tag for a PR image."""
    return f"{_docker_namespace()}/{owner}-{repo}:{issue_number}"


class ImageManager:
    def __init__(self, timeout: int = 600) -> None:
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
    ) -> str:
        ctx = context or _default_context()
        url = repo_url or f"https://github.com/{owner}/{repo}.git"
        tag = get_repo_image_name(owner, repo)
        logger.info("Building repo image: %s", tag)
        build_args: dict[str, str] = {
            "BASE_IMAGE": get_base_image_name(),
            "REPO_URL": url,
        }
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
        env_payload: str = "{}",
    ) -> str:
        ctx = context or _default_context()
        tag = get_pr_image_name(owner, repo, issue_number)
        repo_image = get_repo_image_name(owner, repo)
        logger.info("Building PR image: %s", tag)
        build_args: dict[str, str] = {
            "REPO_IMAGE": repo_image,
            "COMMIT_SHA": commit_sha,
        }
        if build_script:
            build_args["BUILD_SCRIPT"] = build_script
        if env_payload != "{}":
            build_args["ENV_PAYLOAD"] = env_payload
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
