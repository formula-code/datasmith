"""Env-only binaries must never be called bare in a build template.

`asv`, `pytest`, `uv` and `pip` live in the micromamba environment at
/opt/conda/envs/$ENV_NAME/bin. PATH in the image carries /opt/conda/bin, which
is the BASE conda. So a bare call exits 127 and fails the build stage.

This was live in `docker_build_run.sh` for the life of the project. All 1856
stored build_run_sh scripts, across all 134 repos, add an activation to that
line, which means the first thing every synthesis agent did was repair our
template. That is the single largest reason the no-agent path never succeeded.

The check is textual and deliberately narrow: it looks for a command at the
start of a line, which is how the broken call was written.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[2]
_TEMPLATES = _ROOT / "src" / "datasmith" / "docker" / "templates"

# Binaries that exist only inside the micromamba environment.
_ENV_ONLY = ("asv", "pytest", "uv", "pip")

# A bare call: start of line, optional indent, then the binary and a space.
_BARE = re.compile(rf"^\s*({'|'.join(_ENV_ONLY)})\s+", re.MULTILINE)

# Scripts that run inside a build stage, where PATH lacks the env.
_BUILD_SCRIPTS = [
    "docker_build_env.sh",
    "docker_build_pkg.sh",
    "docker_build_run.sh",
    "docker_build_final.sh",
]


def _uncovered_bare_calls(text: str) -> list[str]:
    """Bare calls that are not already qualified or activated on the same line."""
    offenders = []
    for match in _BARE.finditer(text):
        line = text[match.start() : text.find("\n", match.start())]
        if "micromamba run" in line or "micromamba activate" in line:
            continue
        offenders.append(line.strip())
    return offenders


@pytest.mark.parametrize("name", _BUILD_SCRIPTS)
def test_no_bare_env_binary_calls(name: str) -> None:
    path = _TEMPLATES / name
    if not path.is_file():
        pytest.skip(f"{name} not present")
    text = path.read_text()

    # A script that activates the env for the whole file is fine.
    if re.search(r"^\s*micromamba activate ", text, re.MULTILINE):
        return

    offenders = _uncovered_bare_calls(text)
    assert not offenders, (
        f"{name} calls an env-only binary without activating the environment.\n"
        f"PATH holds /opt/conda/bin (base conda), not /opt/conda/envs/$ENV_NAME/bin, "
        f"so these exit 127 at build time:\n  " + "\n  ".join(offenders)
    )


def test_the_guard_can_actually_fire() -> None:
    """A guard that cannot fail is worse than no guard."""
    broken = 'cd "$(dirname "$CONF_NAME")"\nasv machine --yes --config foo.json\n'
    assert _uncovered_bare_calls(broken) == ["asv machine --yes --config foo.json"]

    fixed = 'micromamba run -n "$ENV_NAME" asv machine --yes --config foo.json\n'
    assert _uncovered_bare_calls(fixed) == []
