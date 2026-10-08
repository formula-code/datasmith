"""Every `fc_note` key must be a field the sealer knows.

`emit_manifest.py` reads a FIXED list of keys out of notes.jsonl. A breadcrumb
whose key is not on that list is written by the build and silently discarded,
which produces a manifest block that is indistinguishable from a healthy one.
CLAUDE.md calls this out for measurement facts specifically; it applies to
every breadcrumb.

It happened during this work: `build_isolation` and `numpy_moved_during_install`
were emitted from docker_build_pkg.sh and dropped, because adding them to the
sealer was noted as "a larger change" and not done. Nothing failed. The notes
just went nowhere.

This test walks the shell templates and the sealer rather than naming keys, so
it keeps working as breadcrumbs are added.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_TEMPLATES = Path(__file__).parents[2] / "src" / "datasmith" / "docker" / "templates"
_SEALER = _TEMPLATES / "emit_manifest.py"
_FIELD_LISTS = ("_INT_FIELDS", "_BOOL_FIELDS", "_LIST_FIELDS", "_STR_FIELDS")


def _known_fields() -> set[str]:
    text = _SEALER.read_text(encoding="utf-8")
    known: set[str] = set()
    for name in _FIELD_LISTS:
        match = re.search(rf"{name} = \((.*?)\)", text, re.DOTALL)
        assert match, f"{name} is missing from emit_manifest.py"
        known |= set(re.findall(r'"([a-z_0-9]+)"', match.group(1)))
    return known


def _emitted_keys() -> dict[str, list[str]]:
    found: dict[str, list[str]] = {}
    for script in sorted(_TEMPLATES.glob("*.sh")):
        for key in re.findall(r'fc_note\s+"?([a-zA-Z_0-9]+)=', script.read_text(encoding="utf-8")):
            found.setdefault(key, []).append(script.name)
    return found


def test_the_sealer_declares_field_lists() -> None:
    known = _known_fields()
    assert len(known) > 10, "the sealer's field lists did not parse"


def test_at_least_one_breadcrumb_is_emitted() -> None:
    """Guards the test itself: a regex that matches nothing proves nothing."""
    assert _emitted_keys(), "no fc_note calls found; the detector is broken"


def test_no_breadcrumb_is_silently_dropped() -> None:
    known = _known_fields()
    dropped = {key: files for key, files in _emitted_keys().items() if key not in known}
    assert not dropped, (
        "these fc_note keys are written by the build and discarded by the sealer, "
        "producing a manifest block indistinguishable from a healthy one:\n  "
        + "\n  ".join(f"{key}  (from {', '.join(files)})" for key, files in sorted(dropped.items()))
    )


@pytest.mark.parametrize("key", ["build_isolation", "build_backend", "numpy_moved_during_install"])
def test_the_install_facts_reach_the_sealer(key: str) -> None:
    """These three were the ones that went nowhere."""
    assert key in _known_fields()
