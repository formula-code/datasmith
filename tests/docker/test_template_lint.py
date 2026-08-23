"""Template directories are excluded from ruff, so undefined names reach production.

`pyproject.toml:102` lists all three template directories in `extend-exclude`
with `force-exclude = true`. That is deliberate: the files there are stdlib-only
scripts baked into container images, and most ruff rules do not apply to them.

The cost was a real defect. `pytest_runner.py` called `sys.exit()` without
importing `sys`, so every test run in every image ended in a `NameError`. The
agent could not edit the file, so 130 of 134 repositories injected
`builtins.sys = sys` instead, which then ran inside the measured benchmark
process.

This test re-enables only the rules that catch that class: undefined name,
undefined export, redefinition. It uses `--isolated` so the exclusion in
`pyproject.toml` does not apply.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[2]

_TEMPLATE_DIRS = [
    "src/datasmith/docker/templates",
    "src/datasmith/agents/templates",
    "src/datasmith/harbor_adapter/template",
]

# F821 undefined name, F822 undefined name in __all__, F811 redefinition.
_RULES = "F821,F822,F811"


@pytest.mark.parametrize("rel", _TEMPLATE_DIRS)
def test_no_undefined_names_in_templates(rel: str) -> None:
    directory = _ROOT / rel
    assert directory.is_dir(), f"template directory missing: {rel}"

    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "ruff",
            "check",
            "--isolated",
            "--select",
            _RULES,
            "--output-format",
            "concise",
            str(directory),
        ],
        capture_output=True,
        text=True,
    )

    assert proc.returncode == 0, (
        f"undefined or redefined names in {rel}.\n"
        f"These files are excluded from ruff, so nothing else will catch this.\n"
        f"{proc.stdout}\n{proc.stderr}"
    )
