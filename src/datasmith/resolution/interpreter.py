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
from packaging.version import InvalidVersion, Version

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

#: Operators that pin a version rather than bound a range.  Only these may be
#: read as naming a minor version at patch level; see :func:`_declares_minor`.
_PINNING_OPERATORS = frozenset({"==", "===", "~="})


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


def _declared_version(value: object) -> str:
    """Coerce one declared version to ``"major.minor"``.

    ``ASVCfgAggregate.pythons`` holds ``set[tuple[int, ...]]``, so a plain
    ``str(value)`` reads ``(3, 10)`` as ``"(3, 10)"`` -- a string that matches no
    supported interpreter, silently emptying the asv rung and dropping the choice
    to the commit-date default.  A three-part ``"3.10.2"`` narrows to ``"3.10"``
    for the same reason: the ladder compares minor versions.
    """
    if isinstance(value, tuple | list):
        text = ".".join(str(part) for part in value)
    else:
        text = str(value)
    parts = text.strip().split(".")
    if len(parts) > 2 and parts[0].isdigit() and parts[1].isdigit():
        return f"{parts[0]}.{parts[1]}"
    return text.strip()


def _declares_minor(spec: SpecifierSet, candidate: str) -> bool:
    """Does ``spec`` allow ``candidate``, a bare ``"major.minor"`` version?

    ``SpecifierSet("==3.12.12").contains("3.12")`` is ``False``: the declaration
    is exact and ``3.12`` is not ``3.12.12``.  PostHog declares exactly that, so
    the rung matched nothing, the choice fell through, and the row recorded
    ``commit-date`` for a project that does declare ``requires-python``.  The
    interpreter was right; only its provenance was wrong, which is the harder
    kind of wrong to notice.

    The ladder compares minor versions, so a declaration that pins a patch level
    of ``3.12`` is a declaration of ``3.12``.  **Only the pinning operators may
    be read that way.**  ``<3.12`` carries the release ``(3, 12)`` just as
    ``==3.12.12`` does, and truncating that to a minor version would make
    ``>=3.9,<3.12`` select the one interpreter it exists to exclude.

    Every specifier in the set must agree, so a bound that really does exclude
    the candidate still excludes it however the others read.
    """
    want = _as_tuple(candidate)
    for specifier in spec:
        if specifier.contains(candidate):
            continue
        if specifier.operator not in _PINNING_OPERATORS:
            return False
        try:
            # ``==3.12.*`` is a prefix match; its release is the prefix.
            pinned = Version(specifier.version.removesuffix(".*"))
        except InvalidVersion:
            # ``===`` takes an arbitrary string, which need not be a version.
            return False
        if pinned.release[:2] != want:
            return False
    return True


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
    asv_pythons: Iterable[str | tuple[int, ...]],
    commit_date: dt.datetime,
) -> InterpreterChoice:
    """Pick the newest supported interpreter the project declares.

    Rungs are tried in order and the first that yields a usable version wins.  A
    declaration nothing can satisfy -- pymc's ``>=3.6,<3.7``, say -- falls through
    to the next rung rather than failing the commit.

    ``asv_pythons`` takes the shape the repository actually holds them in --
    ``ASVCfgAggregate.pythons`` is a set of ``(3, 10)`` tuples -- as well as
    ``"3.10"`` strings.
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
            allowed = [v for v in available if _declares_minor(spec, v)]
            if allowed:
                return InterpreterChoice(version=allowed[0], source="requires-python")

    rungs: tuple[tuple[Iterable[object], str], ...] = ((trove_versions, "trove"), (asv_pythons, "asv"))
    for candidates, source in rungs:
        declared = {_declared_version(v) for v in candidates}
        allowed = [v for v in available if v in declared]
        if allowed:
            return InterpreterChoice(version=allowed[0], source=source)

    # Nothing declared was usable. Distinguish "the project said nothing" from
    # "the project said something this operator does not build", because they
    # call for different actions and the version chosen is the same either way.
    #
    # The first real run made the difference matter: PostHog declares
    # ``==3.13.13`` on 2,775 commits, and with the ceiling at 3.12 every one of
    # them recorded ``commit-date`` -- indistinguishable from a repository that
    # declares nothing at all. 99% of that run's ``commit-date`` rows were this
    # case. ``ceiling-clamped`` says which knob to turn.
    if _clamped_by_ceiling(requires_python, trove_versions, asv_pythons, commit_date):
        return InterpreterChoice(version=available[0], source="ceiling-clamped")
    return InterpreterChoice(version=available[0], source="commit-date")


def _clamped_by_ceiling(
    requires_python: str | None,
    trove_versions: Iterable[str],
    asv_pythons: Iterable[str | tuple[int, ...]],
    commit_date: dt.datetime,
) -> bool:
    """Would some rung have matched if ``DATASMITH_PYTHON_CEILING`` were higher?

    Re-runs the same rungs against the interpreters the ceiling excluded. Only
    the ceiling is lifted -- the floor and the commit date still apply, so a
    declaration that is merely too old, or names an interpreter that did not
    exist yet, is not reported as clamped.
    """
    floor = _as_tuple(DATASMITH_PYTHON_FLOOR)
    ceiling = _as_tuple(DATASMITH_PYTHON_CEILING)
    above = [
        f"{key[0]}.{key[1]}"
        for key, released in PY_RELEASES.items()
        if key >= floor and key > ceiling and released <= commit_date
    ]
    if not above:
        return False

    if requires_python:
        try:
            spec = SpecifierSet(requires_python)
        except InvalidSpecifier:
            spec = None
        if spec is not None and any(_declares_minor(spec, v) for v in above):
            return True

    for candidates in (trove_versions, asv_pythons):
        declared = {_declared_version(v) for v in candidates}
        if any(v in declared for v in above):
            return True
    return False
