"""Shared build result model."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class BuildResult:
    """Represents the result of a Docker build operation."""

    ok: bool
    image_name: str
    image_id: str | None
    rc: int  # 0 ok, 124 timeout, 1 generic failure
    duration_s: float
    stderr_tail: str
    stdout_tail: str


__all__ = ["BuildResult"]
