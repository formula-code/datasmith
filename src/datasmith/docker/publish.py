"""DockerHub publisher."""

from __future__ import annotations

import os
from datetime import datetime, timezone

import httpx
from python_on_whales import DockerClient

from datasmith.utils import get_logger, with_backoff

logger = get_logger("docker.publish")


class DockerHubPublisher:
    def __init__(self, namespace: str = "formulacode") -> None:
        self._docker = DockerClient()
        self._namespace = namespace
        self._logged_in = False

    def _login(self) -> None:
        if self._logged_in:
            return
        username = os.environ.get("DOCKERHUB_USERNAME", "")
        password = os.environ.get("DOCKERHUB_PASSWORD", "")
        if username and password:
            self._docker.login(username=username, password=password)
            self._logged_in = True

    @with_backoff(max_retries=3, base_delay=2.0)
    def push(self, image_tag: str) -> None:
        self._login()
        logger.info("Pushing image: %s", image_tag)
        self._docker.push(image_tag)

    def tag_with_version(self, image_tag: str) -> str:
        version = datetime.now(tz=timezone.utc).strftime("@%Y-%m")
        new_tag = f"{image_tag}{version}"
        self._docker.tag(image_tag, new_tag)
        return new_tag

    def list_remote_tags(self, repo: str) -> list[str]:
        """List tags for a DockerHub repository."""
        url = f"https://hub.docker.com/v2/repositories/{self._namespace}/{repo}/tags/"
        try:
            resp = httpx.get(url, timeout=10.0)
            if resp.status_code == 200:
                return [t["name"] for t in resp.json().get("results", [])]
        except Exception:
            logger.warning("Failed to list remote tags for %s", repo)
        return []

    def filter_unpublished(self, local_tags: list[str], remote_tags: list[str]) -> list[str]:
        remote_set = set(remote_tags)
        return [t for t in local_tags if t.split(":")[-1] not in remote_set]
