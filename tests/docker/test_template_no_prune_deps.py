"""`micromamba remove` in a build template must not prune dependencies.

micromamba prunes by default. Removing the package under test therefore also
removes everything that was pulled in only for it, and that cascade can take
the environment's own interpreter with it.

apache/arrow#1646 is the worked example. The env stage unlinked
libarchive-3.7.7 while removing the package under test, and the build then died
several steps later on:

    error: No virtual environment or system Python installation found for path
    `/opt/conda/envs/asv_3.8/bin/python`

which names the wrong thing entirely. `asv_3.8` is present and healthy in
`formulacode/base:latest`; our own removal broke it. Every command in that loop
ends in `|| true`, so nothing reported the real cause.

The check is textual because the failure is textual: one missing flag.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[2]
_TEMPLATES = _ROOT / "src" / "datasmith" / "docker" / "templates"

# A `micromamba remove` call, capturing the rest of the line.
_REMOVE = re.compile(r"^\s*(?:\[[^\]]*\]\s*&&\s*)?micromamba\s+remove\b(?P<rest>.*)$", re.MULTILINE)


def _pruning_removals(text: str) -> list[str]:
    """Every `micromamba remove` line that would prune dependencies."""
    return [m.group(0).strip() for m in _REMOVE.finditer(text) if "--no-prune-deps" not in m.group("rest")]


def test_detector_flags_the_line_that_broke_arrow() -> None:
    """Red half of the pair: the pre-fix line must be caught.

    Without this, a detector that matches nothing at all would pass the guard
    below and prove nothing.
    """
    before = '    [ -n "$PKG_NAME" ]    && micromamba remove -n "$e" -y "$PKG_NAME"    || true'
    assert _pruning_removals(before) == [before.strip()]

    after = '    [ -n "$PKG_NAME" ] && micromamba remove -n "$e" -y --no-prune-deps "$PKG_NAME" || true'
    assert _pruning_removals(after) == []


@pytest.mark.parametrize(
    "script",
    ["docker_build_env.sh", "docker_build_pkg.sh", "docker_build_run.sh", "docker_build_final.sh"],
)
def test_build_templates_never_prune_on_remove(script: str) -> None:
    path = _TEMPLATES / script
    if not path.exists():
        pytest.skip(f"{script} not present")
    offenders = _pruning_removals(path.read_text(encoding="utf-8"))
    assert not offenders, (
        f"{script} removes a package without --no-prune-deps, so micromamba "
        f"will also remove its orphaned dependencies and can break the "
        f"interpreter:\n  " + "\n  ".join(offenders)
    )


def test_env_stage_checks_the_interpreter_after_removal() -> None:
    """The removal loop must not be able to fail silently.

    Every removal ends in `|| true`. Without an explicit check the damage is
    invisible until an unrelated command fails with an unrelated message.
    """
    text = (_TEMPLATES / "docker_build_env.sh").read_text(encoding="utf-8")
    assert '"$PYTHON_BIN" -c "import sys"' in text, (
        "docker_build_env.sh must verify the interpreter still runs after the package-removal loop"
    )
