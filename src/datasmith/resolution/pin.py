"""Pin a declared dependency set to concrete versions.

Tooling the base image installs (``asv``, ``pytest``, ``setuptools`` ...) is
removed from both the declared and the compiled set, so the payload never
overrides the image's versions.  Extras are opt-in per repository.

The commit-date cutoff is tried first; if it makes the set unsatisfiable the
compile is retried without it and ``cutoff_relaxed`` records that.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable
from dataclasses import dataclass, field

from datasmith.utils import get_logger

from .declare import Declared
from .dependency_resolver import rfc3339, uv_compile
from .requirements import Dropped, parse_many, parse_one, render

logger = get_logger("resolution.pin")

__all__ = ["TOOLING_OWNED_BY_BASE_IMAGE", "Pinned", "pin"]

#: Packages the base image installs.  Naming them in ``env_payload`` does not add
#: coverage, it creates a version conflict with the image.
TOOLING_OWNED_BY_BASE_IMAGE: frozenset[str] = frozenset({
    "asv",
    "hypothesis",
    "pip",
    "pytest",
    "setuptools",
    "versioneer",
    "wheel",
})


@dataclass(frozen=True)
class Pinned:
    """A pinned dependency set and how it was reached."""

    requirements: list[str] = field(default_factory=list)
    cutoff_used: str | None = None
    cutoff_relaxed: bool = False
    dropped: list[Dropped] = field(default_factory=list)


def _owned_by_base_image(line: str) -> bool:
    """True when a requirement line names a package the base image installs."""
    req = parse_one(line)
    return req is not None and req.name.lower() in TOOLING_OWNED_BY_BASE_IMAGE


def _strip_tooling(reqs: Iterable[str]) -> list[str]:
    """Drop packages the base image installs, comparing on the bare package name."""
    parsed, _ = parse_many(reqs)
    return render(r for r in parsed if r.name.lower() not in TOOLING_OWNED_BY_BASE_IMAGE)


def _strip_resolved_tooling(resolved: Iterable[str]) -> list[str]:
    """Drop base-image packages from a *compiled* set, keeping every other line intact.

    Needed because tooling arrives transitively (``setuptools-scm`` pulls in
    ``setuptools``).  Lines the parser does not recognise, such as direct URLs, are kept.
    """
    return [line for line in resolved if not _owned_by_base_image(line)]


def pin(
    declared: Declared,
    *,
    python_version: str,
    commit_date: dt.datetime,
    extras: Iterable[str] = (),
    operator_pins: Iterable[str] = (),
) -> Pinned:
    """Compile a declared set to pinned requirements."""
    wanted: list[str] = [*declared.runtime, *declared.build, *operator_pins]
    for name in extras:
        wanted.extend(declared.extras.get(name, []))

    candidates = _strip_tooling(wanted)
    if not candidates:
        return Pinned()

    cutoff = rfc3339(commit_date)

    try:
        resolved = uv_compile(candidates, python_version=python_version, cutoff_rfc3339=cutoff)
        return Pinned(requirements=_strip_resolved_tooling(resolved), cutoff_used=cutoff)
    except Exception as first:
        logger.debug("Compile with cutoff %s failed, relaxing: %s", cutoff, first)

    try:
        resolved = uv_compile(candidates, python_version=python_version, cutoff_rfc3339=None)
        return Pinned(requirements=_strip_resolved_tooling(resolved), cutoff_used=None, cutoff_relaxed=True)
    except Exception as second:
        return Pinned(
            cutoff_used=None,
            cutoff_relaxed=True,
            dropped=[Dropped(raw=", ".join(candidates), reason=f"compile failed: {second}")],
        )
