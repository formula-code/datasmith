"""Data models for dependency resolution."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Candidate:
    """Represents a potential package root in a repository."""

    root_relpath: str
    pyproject_path: Path | None = None
    setup_cfg_path: Path | None = None
    setup_py_path: Path | None = None


@dataclass
class CandidateMeta:
    """Metadata extracted from packaging files."""

    name: str | None = None  # PyPI name
    version: str | None = None
    import_name: str | None = None  # importable module (when we can guess)
    requires_python: str | None = None
    classifiers: set[str] = field(default_factory=set)  # trove, rung 2 of the interpreter ladder
    core_deps: set[str] = field(default_factory=set)  # runtime
    extras: dict[str, set[str]] = field(default_factory=dict)
    build_requires: set[str] = field(default_factory=set)  # [build-system].requires


@dataclass
class ASVCfgAggregate:
    """Aggregated configuration from ASV benchmark config files."""

    pythons: set[tuple[int, ...]] = field(default_factory=set)
    build_commands: set[str] = field(default_factory=set)
    install_commands: set[str] = field(default_factory=set)
    matrix: dict[str, set[str]] = field(default_factory=dict)
