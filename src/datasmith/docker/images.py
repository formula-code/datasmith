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


# Longest slug kept from a package root before the digest is appended. The
# digest, not the slug, is what makes two roots distinct, so this is a
# readability budget rather than a correctness one.
DATASMITH_IMAGE_ROOT_SLUG_CHARS: int = int(os.environ.get("DATASMITH_IMAGE_ROOT_SLUG_CHARS", "32"))


def _build_root_slug(build_root: str) -> str:
    """Return the tag component that names a non-default package root.

    The repository root — ``""``, ``"."``, ``"./"``, ``"/"`` — yields the empty
    string, so the overwhelming majority of rows keep the tag they already
    have and nothing rebuilds for them. Anything else is sanitised into a
    legal tag component and suffixed with a digest of the *normalised* root,
    because sanitising and truncating both lose information:
    ``python/adbc_driver_bigquery`` and ``python/adbc_driver_flightsql`` are
    two real roots of one repository, and a tag they share is the bug this
    function exists to prevent.
    """
    root = posixpath.normpath(build_root.strip() or ".").lstrip("/")
    if root in ("", "."):
        return ""
    slug = re.sub(r"[^a-z0-9_.]+", "-", root.lower()).strip("-._")[:DATASMITH_IMAGE_ROOT_SLUG_CHARS].strip("-._")
    digest = hashlib.sha256(root.encode("utf-8")).hexdigest()[:8]
    return f"{slug}-{digest}" if slug else digest


def get_repo_image_name(owner: str, repo: str, py_version: str = "", build_root: str = "") -> str:
    """Return the canonical tag for a repository image.

    The interpreter belongs in the tag because it is baked into the image. When
    it was not, one image per repository was built from whichever commit ran
    first, and 88% of repositories have commits that disagree on the
    interpreter — so most containers ran a Python their env_payload was never
    pinned against.

    ``build_root`` belongs in the tag for the same reason: it is the image's
    ``WORKDIR``, it varies per commit, and a repository whose commits disagree
    on it would otherwise get one image whose working directory is decided by
    whichever commit ran first. Qiskit is the worst case — 384 rows whose own
    root is the repository root sharing a tag with rows rooted at
    ``qiskit_pkg``. The repository root adds nothing to the tag, so the common
    case is unchanged.

    An empty ``py_version`` yields ``:latest``, which is what images built
    before this change are tagged with.
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
        # ``packages.primary_root`` is nullable and every legacy row predates
        # this argument, so the fallback lives here -- the one choke point every
        # caller routes through -- rather than at each call site.
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
    ) -> str:
        ctx = context or _default_context()
        tag = get_pr_image_name(owner, repo, issue_number)
        # ``build_root`` names the parent, it is not a build arg here: the
        # WORKDIR it selects is already sealed into the repository image, and
        # a PR image that names the wrong parent fails at FROM.
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
