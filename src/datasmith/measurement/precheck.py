"""Checks before timing, from the fields the image build check writes (template docker/templates/apply_oracle_patch.py).

by_deps: benchmarks LSV selects through a dependency on a file the patch changes; always_affected: benchmarks it selects for
every change; benchmarks_measured_n: benchmarks the build check timed. edited_overlap: files whose edits in
the image touch an oracle hunk. Both are in the measure block of `candidate_containers.build_manifest["verify"]`.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from datasmith.harbor_adapter.utils import patch_files

NON_SOURCE = re.compile(r"(^|/)(tests?|docs?)/|CHANGELOG|\.(md|rst|txt)$")
COMPILED = re.compile(r"\.(pyx|pxd|pxi|c|cc|cpp|cxx|h|hh|hpp|f|f90|rs|cu|cuh)(\.src)?$")
# Measured anyway when asked for: always_affected = no_measurement tasks, compiled = uncovered tasks that change only compiled files.
MEASURE_UNCOVERED_KINDS = ("always_affected", "compiled")


def source(files: list[str] | set[str]) -> set[str]:
    return {f for f in files if not NON_SOURCE.search(f)}


def decide(
    measure: dict[str, Any], changed: list[str] | set[str], measure_uncovered: set[str] | frozenset[str] = frozenset()
) -> str | None:
    """`base_edited`, `patch_failed`, `no_measurement`, `uncovered`, or None when the task should be measured.

    base_edited: the image already has edits that touch an oracle hunk, or the patch failed and the image edited a file
    it changes. no_measurement: LSV selects only always-affected benchmarks and the build check timed none of them.
    uncovered: LSV selects no benchmark. A build check without by_deps (an image built before template-v1.4) is measured.
    """
    failed = measure.get("patch_applied") is False
    edited = source(measure.get("edited_overlap") or [])
    if failed:
        edited |= source(measure.get("edited_before") or []) & set(changed)
    if edited:
        return "base_edited"
    if failed:
        return "patch_failed"
    by_deps = measure.get("by_deps")
    if by_deps is None or by_deps > 0:
        return None
    # LSV times always-affected benchmarks for every change; with none timed in the image, the suite does not run there.
    if measure.get("always_affected"):
        if measure.get("benchmarks_measured_n") == 0 and "always_affected" not in measure_uncovered:
            return "no_measurement"
        return None
    src = source(changed)
    if "compiled" in measure_uncovered and src and all(COMPILED.search(f) for f in src):
        return None
    return "uncovered"


def precheck(
    task_dir: Path, measure_uncovered: set[str] | frozenset[str] = frozenset()
) -> tuple[str | None, dict[str, Any]]:
    """(end state or None, measure block) for a rendered task, from its image's build check in the database."""
    from datasmith.utils.db import fetch_all

    cfg = json.loads((task_dir / "tests" / "config.json").read_text())
    keys = {"owner": cfg["owner"], "repo": cfg["repo"], "sha": cfg["gt_hash"]}
    rows = fetch_all("candidate_containers", "v:build_manifest->verify", filters=keys)
    measure = (rows[0].get("v") or {}) if rows else {}
    return decide(measure, patch_files(task_dir / "solution" / "solve.sh"), measure_uncovered), measure
