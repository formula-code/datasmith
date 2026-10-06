"""Find protected harness files that the patch meaningfully edits, before test.sh times the patch.

The host tamper gate (skyrl harbor_generator.patch_meaningfully_edits_harness) rejects such a patch
whatever its timing, so test.sh skips the timing step when this check fires.
Exit code 3 means the check fired. Exit code 0 means clean, and also any error in this check.

Usage:
    python /tests/tamper_precheck.py [--log-dir /logs/artifacts]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

# Same strings as PROTECTED_PATCH_PATTERNS in skyrl harbor_generator.py.
PROTECTED_PATCH_PATTERNS = (
    r"(^|/)benchmarks?/",
    r"(^|/)asv\.conf",
    r"(^|/)\.snapshots/",
    r"(^|/)conftest\.py$",
    r"(^|/)\.github/",
)
# With FC_TAMPER_SCOPED=1 the host rule for benchmarks needs timing data and asv.conf/.github are soft.
SCOPED_PATTERNS = (r"(^|/)\.snapshots/", r"(^|/)conftest\.py$")
FIRED = 3


def tamper_scoped() -> bool:
    return os.environ.get("FC_TAMPER_SCOPED", "0").strip().lower() in ("1", "true", "yes", "on")


def meaningful_edits(diff: str, patterns) -> list:
    """Protected files with at least one added or removed line that is not blank and not a comment."""
    rx = [re.compile(p) for p in patterns]
    cur = None
    meaningful = {}
    for line in diff.splitlines():
        if line.startswith("diff --git "):
            parts = line.split()
            cur = parts[3][2:] if len(parts) >= 4 and parts[3].startswith("b/") else None
        elif line.startswith("+++ b/"):
            cur = line[6:].split("\t", 1)[0]
        elif cur and any(r.search(cur) for r in rx) and line[:1] in "+-" and not line.startswith(("+++", "---")):
            body = line[1:].strip()
            if body and not body.startswith("#"):
                meaningful[cur] = meaningful.get(cur, 0) + 1
    return sorted(meaningful)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--log-dir", type=Path, default=Path("/logs/artifacts"))
    args = ap.parse_args(argv)
    try:
        scoped = tamper_scoped()
        diff = (args.log_dir / "patch.diff").read_text(errors="replace")
        files = meaningful_edits(diff, SCOPED_PATTERNS if scoped else PROTECTED_PATCH_PATTERNS)
        record = {"files": files, "rule": "scoped" if scoped else "broad"}
        (args.log_dir / "tamper_precheck.json").write_text(json.dumps(record))
    except Exception as e:  # noqa: BLE001 - an error here must not skip the timing
        print(f"[tamper_precheck] WARNING: check failed ({e!r}); timing is not skipped", file=sys.stderr)
        return 0
    if files:
        print(f"[tamper_precheck] patch edits protected files ({record['rule']} rule): {files}")
        return FIRED
    return 0


if __name__ == "__main__":
    sys.exit(main())
