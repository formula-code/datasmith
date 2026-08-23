"""Resolving one repository must not change another's answer."""

import subprocess
from pathlib import Path

# Anchor the grep at the repository, not at the current directory: run from
# anywhere else, a relative path makes grep fail, stdout comes back empty, and
# the guard passes while a live reference is still there.
SRC = Path(__file__).resolve().parents[2] / "src" / "datasmith"

BLOCKLIST_PATTERN = "blocklist|add_to_blocklist|get_blocklist"

# ``-I`` skips binary files. Without it a stale ``__pycache__/*.pyc`` left over
# from before the deletion still carries the name, and grep reports it on
# *stderr* as "binary file matches" -- so the guard would fail on build output
# rather than on a source reference, which is not what it is asking about.
GREP = ["grep", "-rniE", "-I"]


def test_blocklist_module_is_deleted():
    import importlib

    try:
        importlib.import_module("datasmith.resolution.blocklist")
    except ModuleNotFoundError:
        return
    raise AssertionError("datasmith.resolution.blocklist still exists")


def test_no_source_file_references_the_blocklist():
    assert SRC.is_dir(), f"source tree not found at {SRC}"
    cp = subprocess.run(
        [*GREP, BLOCKLIST_PATTERN, str(SRC)],
        capture_output=True,
        text=True,
    )
    # grep exits 0 on a match and 1 on no match; anything else is grep itself
    # failing, which would make an empty stdout meaningless.
    assert cp.returncode in (0, 1), f"grep failed ({cp.returncode}): {cp.stderr}"
    assert cp.stderr == "", f"grep wrote to stderr: {cp.stderr}"
    assert cp.stdout == "", f"stale references remain:\n{cp.stdout}"


def test_the_guard_sees_a_planted_reference(tmp_path):
    # The guard above is only worth having if it can fail. Prove the grep it
    # runs finds a reference, using the same invocation against a decoy tree.
    (tmp_path / "decoy.py").write_text("dynamic = get_blocklist()\n")
    cp = subprocess.run(
        [*GREP, BLOCKLIST_PATTERN, str(tmp_path)],
        capture_output=True,
        text=True,
    )
    assert cp.returncode == 0
    assert "decoy.py" in cp.stdout


def test_declaring_one_project_does_not_change_the_next(tmp_path, monkeypatch):
    # The B2 property, at the unit that survived the redesign. The predecessor
    # read a shared JSON blocklist while filtering and the self-healing loops
    # appended to it, so resolving repository A changed repository B's seed.
    # ``declare`` is a pure function of one project's own metadata: nothing it
    # answers may depend on what ran before it, and nothing it does may write.
    from datasmith.resolution.declare import declare
    from datasmith.resolution.models import CandidateMeta

    monkeypatch.setenv("GIT_CACHE_DIR", str(tmp_path))
    a = CandidateMeta(core_deps={"numpy", "not a requirement at all"})
    b = CandidateMeta(core_deps={"scipy"})

    before = set(tmp_path.rglob("*"))
    b_alone = declare(b, None)
    declare(a, None)
    b_after_a = declare(b, None)
    after = set(tmp_path.rglob("*"))

    assert b_after_a == b_alone
    assert b_alone.runtime == ["scipy"]
    assert after == before, f"declaring wrote {after - before}"
