"""Docker image management via python-on-whales."""

from __future__ import annotations

import os

from python_on_whales import DockerClient

from datasmith.utils import get_logger

logger = get_logger("docker.images")


class ImageManager:
    def __init__(self, timeout: int = 600) -> None:
        self._docker = DockerClient()
        self._timeout = timeout

    def build_base_image(self, context_path: str = ".") -> str:
        tag = "formulacode/base:latest"
        logger.info("Building base image: %s", tag)
        self._docker.build(
            context_path,
            tags=[tag],
            file=os.path.join(context_path, "Dockerfile.base"),
        )
        return tag

    def build_repo_image(self, owner: str, repo: str, context_path: str = ".") -> str:
        tag = f"formulacode/{owner}-{repo}:latest"
        logger.info("Building repo image: %s", tag)
        self._docker.build(
            context_path,
            tags=[tag],
            file=os.path.join(context_path, "Dockerfile.repo"),
            build_args={"BASE_IMAGE": "formulacode/base:latest"},
        )
        return tag

    def build_pr_image(
        self,
        owner: str,
        repo: str,
        issue_number: int,
        context_path: str = ".",
        build_script: str = "",
    ) -> str:
        tag = f"formulacode/{owner}-{repo}:{issue_number}"
        repo_image = f"formulacode/{owner}-{repo}:latest"
        logger.info("Building PR image: %s", tag)
        build_args = {"REPO_IMAGE": repo_image}
        if build_script:
            build_args["BUILD_SCRIPT"] = build_script
        self._docker.build(
            context_path,
            tags=[tag],
            file=os.path.join(context_path, "Dockerfile.pr"),
            build_args=build_args,
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
