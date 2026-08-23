"""The secret scan must not match its own source.

`docker_build_final.sh` greps three baked scripts for credential literals, and
it is one of the three. The pattern was written as one literal, so it matched
itself. `secrets_scan_clean` was therefore 0 on every build ever made, and the
FATAL `secrets_present` invariant meant the stock template could never pass
verification. It was the second independent reason the no-agent path failed.

A synthesis agent diagnosed this correctly and then replaced `grep` with a
wrapper that reported clean for that exact invocation. Disabling a detector to
silence a false positive is the wrong repair, but the false positive was real.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[2]
_TEMPLATES = _ROOT / "src" / "datasmith" / "docker" / "templates"

# The pattern the scan looks for, written here as the scanner means it.
_SECRET_RE = re.compile(r"sb_secret_|service_role|SUPABASE_[A-Z_]*KEY=[A-Za-z0-9]")

# The files docker_build_final.sh scans.
_SCANNED = ["docker_build_final.sh", "run-tests.sh", "profile.sh"]


@pytest.mark.parametrize("name", _SCANNED)
def test_a_scanned_file_does_not_trip_the_scan(name: str) -> None:
    path = _TEMPLATES / name
    if not path.is_file():
        pytest.skip(f"{name} not present")
    hits = [line.strip() for line in path.read_text().splitlines() if _SECRET_RE.search(line)]
    assert not hits, (
        f"{name} matches the credential scan, so secrets_scan_clean is 0 on every build "
        f"and the FATAL secrets_present invariant always fires:\n  " + "\n  ".join(hits)
    )


def test_the_scan_still_detects_a_real_leak() -> None:
    """A scan that cannot fire is worse than no scan."""
    assert _SECRET_RE.search("export SUPABASE_KEY=sb_secret_abc123")
    assert _SECRET_RE.search("role: service_role")
    assert not _SECRET_RE.search("export SUPABASE_URL=http://127.0.0.1:54321")


def test_the_scanner_assembles_its_pattern_from_fragments() -> None:
    """The mechanism that keeps the pattern out of its own source.

    If someone rewrites it as a single literal, the self-match returns and
    every build fails again.
    """
    text = (_TEMPLATES / "docker_build_final.sh").read_text()
    assert "_SECRET_RE=" in text, "the scan no longer builds its pattern into a variable"
    assert '"sb""_secret_' in text, (
        "the pattern is no longer split across string fragments, so it will match this file again"
    )
