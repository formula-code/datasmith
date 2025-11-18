"""Shared task model used by build and orchestration layers."""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Task:
    """Represents a Docker build task with repository and commit information."""

    owner: str
    repo: str
    sha: str | None = None
    commit_date: float = 0.0
    env_payload: str = ""
    python_version: str = ""
    tag: str = "pkg"  # 'pkg', 'env', 'run', or 'base', or 'final'
    benchmarks: str = ""

    @classmethod
    def default_task(cls) -> Task:
        """Create a default task instance."""
        return cls(owner="default", repo="default", sha=None, tag="pkg")

    @staticmethod
    def _sanitize_component(component: str) -> str:
        """Sanitise components for Docker image/container naming."""
        component = component.lower()
        component = re.sub(r"[^a-z0-9._-]+", "-", component)
        component = component.strip("._-")
        return component or "unknown"

    def with_tag(self, tag: str) -> Task:
        """Return a copy of the task with ``tag`` substituted."""
        if tag not in {"env", "pkg", "run", "base", "final"}:
            raise ValueError(f"Tag must be either 'env', 'pkg', 'run', 'base', or 'final', got '{tag}'.")
        return Task(
            owner=self.owner,
            repo=self.repo,
            sha=self.sha,
            commit_date=self.commit_date,
            tag=tag,
            env_payload=self.env_payload,
            python_version=self.python_version,
            benchmarks=self.benchmarks,
        )

    def with_benchmarks(self, benchmarks: str) -> Task:
        """Return a copy of the task with ``benchmarks`` substituted."""
        return Task(
            owner=self.owner,
            repo=self.repo,
            sha=self.sha,
            commit_date=self.commit_date,
            tag=self.tag,
            env_payload=self.env_payload,
            python_version=self.python_version,
            benchmarks=benchmarks,
        )

    def get_image_name(self) -> str:
        """Return the Docker image name for this task (repo:tag)."""
        if self.tag not in {"env", "pkg", "run", "base", "final"}:
            raise ValueError(f"Tag must be either 'env', 'pkg', 'run', 'base', or 'final', got '{self.tag}'.")

        owner = self._sanitize_component(self.owner)
        repo = self._sanitize_component(self.repo)
        sha_part = f"-{self._sanitize_component(self.sha)}" if self.sha else ""

        image_repo = f"{owner}-{repo}{sha_part}"
        return f"{image_repo}:{self.tag}"

    def get_container_name(self) -> str:
        """Return a deterministic container name for this task."""
        if self.tag not in {"env", "pkg", "run", "base", "final"}:
            raise ValueError("Tag must be either 'env', 'pkg', 'run', 'base', or 'final', got '{self.tag}'.")

        owner = self._sanitize_component(self.owner)
        repo = self._sanitize_component(self.repo)
        sha_part = f"-{self._sanitize_component(self.sha)}" if self.sha else ""
        tag_part = f"-{self._sanitize_component(self.tag)}"

        name = f"{owner}-{repo}{sha_part}{tag_part}"
        if not re.match(r"^[a-z0-9]", name):
            name = f"c-{name}"
        return name[:128]

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Task):
            return NotImplemented
        return self.owner == value.owner and self.repo == value.repo and self.sha == value.sha and self.tag == value.tag

    def __hash__(self) -> int:
        return hash((self.owner, self.repo, self.sha, self.tag))


__all__ = ["Task"]
