"""Resolution package for analyzing commits and resolving dependencies."""

from __future__ import annotations

from typing import Any

__all__ = ["analyze_commit"]


def analyze_commit(sha: str, repo_name: str, bypass_cache: bool = False) -> dict[str, Any] | None:
    """Lazy wrapper around :func:`datasmith.execution.resolution.orchestrator.analyze_commit`."""
    from .orchestrator import analyze_commit as _analyze_commit

    return _analyze_commit(sha=sha, repo_name=repo_name, bypass_cache=bypass_cache)
