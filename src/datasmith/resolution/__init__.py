"""Resolution package for analyzing commits and resolving dependencies."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .orchestrator import RESOLVER_VERSION, ResolutionResult

__all__ = ["RESOLVER_VERSION", "ResolutionResult", "analyze_commit"]


def analyze_commit(sha: str, repo_name: str, bypass_cache: bool = False) -> ResolutionResult | None:
    """Lazy wrapper around :func:`datasmith.resolution.orchestrator.analyze_commit`."""
    from .orchestrator import analyze_commit as _analyze_commit

    return _analyze_commit(sha=sha, repo_name=repo_name, bypass_cache=bypass_cache)


def __getattr__(name: str) -> object:
    """Expose the orchestrator's names without importing git and uv at import time."""
    if name in ("RESOLVER_VERSION", "ResolutionResult"):
        from . import orchestrator

        value = getattr(orchestrator, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
