"""Choose the Python interpreter for a commit, and record why.

The predecessor assigned ``python_version`` unconditionally before checking
whether the attempt had succeeded (``orchestrator.py:603-607``), tried candidates
newest-first, and broke out of the loop on the first non-ABI error.  The value it
stored was therefore "the newest interpreter that did not crash", and re-running
the same 13 commits changed it on 7 of them.  The project's own
``requires-python`` was parsed and then discarded.

Here the choice is a declared ladder.  Take the newest version that satisfies the
declaration and existed at commit date, and record which rung supplied it.
Measured coverage over 155 cached repositories: rung 1 alone 84%, plus rung 2
91%, plus rung 3 99%.
"""

from __future__ import annotations

import datetime as dt
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass

from packaging.specifiers import InvalidSpecifier, SpecifierSet

from .python_manager import PY_RELEASES

__all__ = [
    "DATASMITH_PYTHON_CEILING",
    "DATASMITH_PYTHON_FLOOR",
    "InterpreterChoice",
    "select_interpreter",
    "trove_versions_from_classifiers",
]

#: Oldest interpreter the container toolchain still supports.
DATASMITH_PYTHON_FLOOR: str = os.environ.get("DATASMITH_PYTHON_FLOOR", "3.8")
#: Newest interpreter the container toolchain is known to build against.  This is
#: a ceiling on purpose: a fresh run must not silently start choosing an
#: interpreter no existing image was built with.
DATASMITH_PYTHON_CEILING: str = os.environ.get("DATASMITH_PYTHON_CEILING", "3.12")

_TROVE_RE = re.compile(r"Programming Language :: Python :: (\d+\.\d+)\s*$")


@dataclass(frozen=True)
class InterpreterChoice:
    """The chosen interpreter, and the ladder rung that supplied it."""

    version: str
    source: str


def _as_tuple(version: str) -> tuple[int, int]:
    major, minor = version.split(".")[:2]
    return int(major), int(minor)


def _supported(commit_date: dt.datetime) -> list[str]:
    """Supported interpreters that existed at ``commit_date``, newest first."""
    floor = _as_tuple(DATASMITH_PYTHON_FLOOR)
    ceiling = _as_tuple(DATASMITH_PYTHON_CEILING)
    out: list[str] = []
    for key, released in PY_RELEASES.items():
        if key < floor or key > ceiling:
            continue
        if released > commit_date:
            continue
        out.append(f"{key[0]}.{key[1]}")
    return sorted(out, key=_as_tuple, reverse=True)


def trove_versions_from_classifiers(classifiers: Iterable[str]) -> list[str]:
    """Extract ``3.x`` versions from trove classifiers, newest first.

    ``Programming Language :: Python :: 3 :: Only`` carries no minor version and
    is skipped rather than parsed as ``3.0``.
    """
    found: set[str] = set()
    for line in classifiers:
        match = _TROVE_RE.search(str(line).strip())
        if match:
            found.add(match.group(1))
    return sorted(found, key=_as_tuple, reverse=True)


def select_interpreter(
    *,
    requires_python: str | None,
    trove_versions: Iterable[str],
    asv_pythons: Iterable[str],
    commit_date: dt.datetime,
) -> InterpreterChoice:
    """Pick the newest supported interpreter the project declares.

    Rungs are tried in order and the first that yields a usable version wins.  A
    declaration nothing can satisfy -- pymc's ``>=3.6,<3.7``, say -- falls through
    to the next rung rather than failing the commit.
    """
    available = _supported(commit_date)
    if not available:
        # Older than every supported interpreter; the floor is the only honest answer.
        return InterpreterChoice(version=DATASMITH_PYTHON_FLOOR, source="commit-date")

    if requires_python:
        spec: SpecifierSet | None
        try:
            spec = SpecifierSet(requires_python)
        except InvalidSpecifier:
            spec = None
        if spec is not None:
            allowed = [v for v in available if spec.contains(v)]
            if allowed:
                return InterpreterChoice(version=allowed[0], source="requires-python")

    for candidates, source in ((trove_versions, "trove"), (asv_pythons, "asv")):
        declared = {str(v).strip() for v in candidates}
        allowed = [v for v in available if v in declared]
        if allowed:
            return InterpreterChoice(version=allowed[0], source=source)

    return InterpreterChoice(version=available[0], source="commit-date")
