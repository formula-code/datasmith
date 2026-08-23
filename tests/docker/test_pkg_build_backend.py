"""The editable install must know which distribution provides its backend.

`--no-build-isolation` requires the PEP 517 backend to be importable in the
target env already. Nothing put it there, and the retry differed from the first
attempt only in $EXTRAS, so both attempts failed identically:

    BackendUnavailable: Cannot import 'hatchling.build'
    BackendUnavailable: Cannot import 'scikit_build_core.build'

Seven of 22 failures in the 2026-08-23 trial. The mapping is extracted from the
template and executed, so this tests the shipped function rather than a copy.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

_PKG = Path(__file__).parents[2] / "src" / "datasmith" / "docker" / "templates" / "docker_build_pkg.sh"

# Backends observed in the trial, plus the rest of the common long tail.
_EXPECTED = {
    "scikit_build_core.build": "scikit-build-core",
    "hatchling.build": "hatchling",
    "uv_build": "uv-build",
    "flit_core.buildapi": "flit-core",
    "setuptools.build_meta": "setuptools wheel",
    "mesonpy": "meson-python",
    "poetry.core.masonry.api": "poetry-core",
    "maturin": "maturin",
    "pdm.backend": "pdm-backend",
    "pdm.pep517.api": "pdm-pep517",
    "sipbuild.api": "sip",
    "py_build_cmake.build": "py-build-cmake",
}


@pytest.fixture(scope="module")
def mapping() -> dict[str, str]:
    if shutil.which("bash") is None:
        pytest.skip("bash unavailable")
    text = _PKG.read_text(encoding="utf-8")
    match = re.search(r"^backend_distribution\(\) \{.*?^\}", text, re.DOTALL | re.MULTILINE)
    assert match, "backend_distribution is missing from docker_build_pkg.sh"

    script = (
        match.group(0)
        + "\n"
        + "\n".join(f'printf "%s\\n" "$(backend_distribution {b!r})"'.replace("'", '"') for b in _EXPECTED)
    )
    proc = subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stderr
    return dict(zip(_EXPECTED, proc.stdout.strip().splitlines(), strict=True))


@pytest.mark.parametrize(("backend", "distribution"), sorted(_EXPECTED.items()))
def test_backend_maps_to_its_distribution(mapping: dict[str, str], backend: str, distribution: str) -> None:
    assert mapping[backend] == distribution


def test_an_unknown_backend_falls_back_to_the_module_name(mapping: dict[str, str]) -> None:
    """The long tail is mostly `some_backend.build` provided by `some-backend`."""
    assert mapping["py_build_cmake.build"] == "py-build-cmake"


def test_the_isolated_fallback_exists_and_is_recorded() -> None:
    """An isolated build carries a weaker ABI guarantee, so it must be visible.

    It resolves the backend itself but builds against whatever numpy it picks,
    rather than the env's own.
    """
    text = _PKG.read_text(encoding="utf-8")
    assert "build_isolation=" in text, "the isolation mode must reach the manifest"
    assert 'fc_note "build_isolation=$_ISOLATION"' in text
    assert '_ISOLATION="isolated"' in text, "there must be an isolated last resort"


def test_numpy_movement_is_noticed() -> None:
    """Compiling against one numpy and importing another mismeasures later."""
    text = _PKG.read_text(encoding="utf-8")
    assert "numpy_moved_during_install" in text
