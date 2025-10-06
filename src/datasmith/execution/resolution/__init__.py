"""Resolution package for analyzing commits and resolving dependencies."""

from __future__ import annotations

from .orchestrator import analyze_commit

__all__ = ["analyze_commit"]
