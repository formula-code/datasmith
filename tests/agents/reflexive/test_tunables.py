"""Every knob is DATASMITH_-prefixed, env-overridable, and documented.

CLAUDE.md requires it, and a knob that is not greppable is a knob nobody finds.
"""

from __future__ import annotations

import importlib
import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[3]
_PKG = _ROOT / "src" / "datasmith" / "agents" / "reflexive"

EXPECTED = [
    ("DATASMITH_PV_MAX_ROUNDS", "datasmith.agents.reflexive.loop"),
    ("DATASMITH_PV_ENABLED", "datasmith.agents.reflexive.loop"),
    ("DATASMITH_PV_AGENT_TIMEOUT_S", "datasmith.agents.reflexive.verifier"),
    ("DATASMITH_PV_BATTERY_TIMEOUT_S", "datasmith.agents.reflexive.battery"),
]


@pytest.mark.parametrize(("name", "module"), EXPECTED)
def test_the_constant_exists_and_reads_the_environment(name: str, module: str, monkeypatch) -> None:
    mod = importlib.import_module(module)
    assert hasattr(mod, name), f"{module} must define {name}"
    source = Path(mod.__file__).read_text(encoding="utf-8")
    assert f'os.environ.get("{name}"' in source, f"{name} must read the env at module scope"


def test_every_pv_constant_is_documented_in_claude_md() -> None:
    text = (_ROOT / "CLAUDE.md").read_text(encoding="utf-8")
    for name, _ in EXPECTED:
        assert name in text, f"{name} is not documented in CLAUDE.md"


def test_no_undocumented_pv_constant_exists() -> None:
    """A knob added later must be documented too."""
    documented = (_ROOT / "CLAUDE.md").read_text(encoding="utf-8")
    found: set[str] = set()
    for path in _PKG.glob("*.py"):
        found |= set(re.findall(r'os\.environ\.get\("(DATASMITH_[A-Z0-9_]+)"', path.read_text(encoding="utf-8")))
    missing = sorted(n for n in found if n not in documented)
    assert not missing, f"undocumented tunables: {missing}"
