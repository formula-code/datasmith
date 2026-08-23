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


def test_filter_writes_nothing_to_disk(tmp_path, monkeypatch):
    # filter_requirements_for_pypi used to read a shared JSON file and the
    # self-healing loops used to append to it. Nothing may touch disk now.
    monkeypatch.setenv("GIT_CACHE_DIR", str(tmp_path))
    from datasmith.resolution.package_filters import filter_requirements_for_pypi

    project = tmp_path / "proj"
    project.mkdir()
    before = set(tmp_path.rglob("*"))
    filter_requirements_for_pypi({"numpy", "scipy"}, project_dir=project, own_import_name=None)
    after = set(tmp_path.rglob("*"))
    assert before == after, f"filtering wrote {after - before}"


def test_filtering_is_independent_of_call_order(tmp_path):
    from datasmith.resolution.package_filters import filter_requirements_for_pypi

    project = tmp_path / "proj"
    project.mkdir()

    a_first = filter_requirements_for_pypi({"numpy"}, project_dir=project, own_import_name=None)
    b_after_a = filter_requirements_for_pypi({"scipy"}, project_dir=project, own_import_name=None)
    b_alone = filter_requirements_for_pypi({"scipy"}, project_dir=project, own_import_name=None)

    assert b_after_a == b_alone
    assert a_first == ["numpy"]
