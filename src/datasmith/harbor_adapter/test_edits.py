"""Where the oracle patch changes the repo's tests, in base line numbers, for the pytest step's regression check.

The verifier runs the base tests against the agent's code. A base test that the oracle patch edits or deletes can
fail for a correct fix (dask#11373), so ``pytest_runner.py`` leaves it out of the check. The record is written to
``tests/oracle_test_edits.json`` at render time; ``/tests`` is removed before the agent starts.
"""

from __future__ import annotations

import re

ORACLE_TEST_EDITS = "oracle_test_edits.json"
_TEST_PATH = re.compile(r"(^|/)tests?/|(^|/)test_[^/]*\.py$|(^|/)[^/]*_test\.py$|(^|/)conftest\.py$")
_HUNK = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+\d+(?:,\d+)? @@")
_NAME = re.compile(r"[A-Za-z_]\w*")


def _file_diffs(patch: str) -> list[tuple[str | None, str | None, list[str], bool]]:
    """``[(old path or None, new path or None, hunk lines, binary)]``; None for /dev/null."""
    out: list[tuple[str | None, str | None, list[str], bool]] = []
    for block in re.split(r"(?m)^diff --git ", patch)[1:]:
        lines = block.split("\n")
        m = re.match(r"a/(\S+) b/(\S+)", lines[0])
        old, new = (m.group(1), m.group(2)) if m else (None, None)
        body: list[str] = []
        binary = False
        for i, line in enumerate(lines[1:], 1):
            if line.startswith("--- "):
                old = None if line[4:].strip() == "/dev/null" else line[4:].strip().removeprefix("a/")
            elif line.startswith("+++ "):
                new = None if line[4:].strip() == "/dev/null" else line[4:].strip().removeprefix("b/")
            elif line.startswith("deleted file mode"):
                new = None
            elif line.startswith("new file mode"):
                old = None
            elif line.startswith(("Binary files ", "GIT binary patch")):
                binary = True
            elif line.startswith("@@"):
                body = lines[i:]
                break
        out.append((old, new, body, binary))
    return out


def _edits(body: list[str]) -> dict:
    removed: list[int] = []
    inserts: list[list] = []
    removed_names: set[str] = set()
    added_names: set[str] = set()
    old = 0
    run: list[str] = []
    after_removal = False

    def close() -> None:
        if run and not after_removal:
            text = [r for r in run if r.strip()]
            if text:
                indent = len(text[0]) - len(text[0].lstrip())
                inserts.append([old - 1, indent, text[-1].lstrip().startswith("@")])
        run.clear()

    for line in body:
        m = _HUNK.match(line)
        if m:
            close()
            old, after_removal = int(m.group(1)), False
            if m.group(2) == "0":
                old += 1
        elif line.startswith("+"):
            run.append(line[1:])
            added_names.update(_NAME.findall(line[1:]))
        else:
            close()
            if line.startswith("-"):
                removed.append(old)
                removed_names.update(_NAME.findall(line[1:]))
                old, after_removal = old + 1, True
            elif line.startswith(" "):
                old, after_removal = old + 1, False
    close()
    return {"removed": removed, "inserts": inserts, "dropped_names": sorted(removed_names - added_names)}


def oracle_test_edits(patch: str) -> dict[str, dict]:
    """{base path: {"deleted": bool, "removed": [base lines], "inserts": [[after base line, indent, ends with a
    decorator]], "dropped_names": [names in removed lines that no added line has]}} for the test files, conftest
    files and other files in tests folders that the patch changes or deletes. New files are left out: they have no
    base tests."""
    out: dict[str, dict] = {}
    for old, new, body, binary in _file_diffs(patch or ""):
        if old is None or not _TEST_PATH.search(old):
            continue
        if new != old or binary:
            out[old] = {"deleted": True, "removed": [], "inserts": [], "dropped_names": []}
        elif body:
            out[old] = {"deleted": False, **_edits(body)}
    return out
