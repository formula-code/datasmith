"""Static checks before timing: does the patch apply, did the image already carry part of it, does a benchmark cover it.

The check runs `in_image/coverage_check.py` in the task image without network; its result is kept in
`<survey>/coverage/<task>.json` and reused while it has no error.
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any

from datasmith.verify.survey import Survey

CHECK_SCRIPT = Path(__file__).parent / "in_image" / "coverage_check.py"
NON_SOURCE = re.compile(r"(^|/)(tests?|docs?)/|CHANGELOG|\.(md|rst|txt)$")
COMPILED = re.compile(r"\.(pyx|pxd|pxi|c|cc|cpp|cxx|h|hpp|f|f90|rs|cu)$")
# Kinds of uncovered task that are measured anyway when asked for.
MEASURE_UNCOVERED_KINDS = ("always_affected", "compiled")


def patch_files(solve_sh: Path) -> set[str]:
    """Files the oracle patch in solve.sh changes."""
    return set(re.findall(r"^\+\+\+ b/(\S+)", solve_sh.read_text(), re.M)) if solve_sh.exists() else set()


def source(files: list[str] | set[str]) -> set[str]:
    return {f for f in files if not NON_SOURCE.search(f)}


def run_check(tag: str, solve_sh: Path, timeout: int = 1800) -> dict[str, Any]:
    script = (
        'eval "$(micromamba shell hook --shell=bash)"; micromamba activate "$ENV_NAME"; '
        "cd /workspace/repo && python /tmp/check.py"
    )
    p = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "-v",
            f"{solve_sh}:/tmp/solve.sh:ro",
            "-v",
            f"{CHECK_SCRIPT}:/tmp/check.py:ro",
            "--entrypoint",
            "bash",
            tag,
            "-lc",
            script,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    last = (p.stdout.strip().splitlines() or [""])[-1]
    return json.loads(last) if p.returncode == 0 and last.startswith("{") else {"error": p.stderr[-800:]}


def decide(result: dict[str, Any], changed: set[str], measure_uncovered: set[str] = frozenset()) -> str | None:
    """`base_edited`, `patch_failed`, `uncovered`, or None when the task should be measured.

    base_edited: the image already has edits that touch an oracle hunk, or that make a hunk fail. Results without
    `edited_overlap` count any edit in a file the patch changes. A check that errored returns None.
    """
    if "error" in result:
        return None
    edited = source(result.get("edited_before", [])) & changed
    if "edited_overlap" in result:
        edited = source(result["edited_overlap"]) | (edited & set(result.get("patch_failed", [])))
    if edited:
        return "base_edited"
    if source(result.get("patch_failed", [])):
        return "patch_failed"
    if result.get("by_deps", 0) > 0:
        return None
    benchmarks = result.get("benchmarks")
    if "always_affected" in measure_uncovered and benchmarks and result.get("always_affected") == benchmarks:
        return None
    src = source(changed)
    if "compiled" in measure_uncovered and src and all(COMPILED.search(f) for f in src):
        return None
    return "uncovered"


def precheck(
    survey: Survey, task: str, tag: str, measure_uncovered: set[str] = frozenset(), rerun: bool = False
) -> tuple[str | None, dict[str, Any]]:
    """Run (or reuse) the in-image check for one task and return (end state or None, check result)."""
    path = survey.root / "coverage" / f"{task}.json"
    solve = survey.task_dir(task) / "solution" / "solve.sh"
    result = json.loads(path.read_text()) if path.exists() else {"error": "not run"}
    if rerun or "error" in result:
        result = run_check(tag, solve)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result))
    return decide(result, patch_files(solve), measure_uncovered), result
