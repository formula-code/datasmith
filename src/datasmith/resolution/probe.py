"""Dry-run the pinned seed, advisorily.

The predecessor's ``can_install`` gated stages 5 and 6.  It blocked 3,217
performance PRs that were then never attempted, while passing h5py on a single
dependency and failing apache/arrow on a corrupted marker.  It claimed to mean
"this builds"; it meant "uv could install these wheels into an empty venv".

This keeps the cheap check and drops the claim.  ``status`` orders the stage 5
queue.  It excludes nobody.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .dependency_resolver import uv_dry_run_install
from .pin import Pinned

__all__ = ["PROBE_RANK", "ProbeResult", "ProbeStatus", "probe"]

ProbeStatus = Literal["installable", "unresolved", "failed", "empty"]

#: Queue ordering, best first.  Lower sorts earlier.
PROBE_RANK: dict[str, int] = {"installable": 0, "unresolved": 1, "failed": 2, "empty": 3}


@dataclass(frozen=True)
class ProbeResult:
    """What the dry-run saw."""

    status: ProbeStatus
    log: str


def probe(pinned: Pinned, *, python_version: str) -> ProbeResult:
    """Dry-run a pinned seed.  Never raises."""
    if not pinned.requirements:
        if pinned.dropped:
            reasons = "; ".join(d.reason for d in pinned.dropped)
            return ProbeResult(status="failed", log=reasons)
        return ProbeResult(status="empty", log="nothing declared")

    try:
        ok, log = uv_dry_run_install(pinned.requirements, python_version=python_version)
    except Exception as exc:
        return ProbeResult(status="failed", log=f"{type(exc).__name__}: {exc}")

    if not ok:
        return ProbeResult(status="failed", log=log)
    if pinned.cutoff_relaxed:
        return ProbeResult(status="unresolved", log=log)
    return ProbeResult(status="installable", log=log)
