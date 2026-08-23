"""fix_marker_spacing must be gone, and no caller may resurrect its behaviour."""

import subprocess
from pathlib import Path

# Anchor the grep at the repository, not at the current directory: run from
# anywhere else, a relative path makes grep fail, stdout comes back empty, and
# the guard passes while a live reference is still there.
SRC = Path(__file__).resolve().parents[2] / "src" / "datasmith"


def test_fix_marker_spacing_is_deleted():
    import datasmith.resolution.package_filters as pf

    assert not hasattr(pf, "fix_marker_spacing")


def test_no_source_file_references_it():
    assert SRC.is_dir(), f"source tree not found at {SRC}"
    cp = subprocess.run(
        ["grep", "-rn", "fix_marker_spacing", str(SRC)],
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
    (tmp_path / "decoy.py").write_text("fix_marker_spacing(x)\n")
    cp = subprocess.run(
        ["grep", "-rn", "fix_marker_spacing", str(tmp_path)],
        capture_output=True,
        text=True,
    )
    assert cp.returncode == 0
    assert "decoy.py" in cp.stdout


def test_requirements_reach_uv_unmodified():
    from datasmith.resolution.requirements import parse_many, render

    raw = ["numpy; platform_system=='Windows'", "scipy; sys_platform=='linux'"]
    reqs, dropped = parse_many(raw)
    out = render(reqs)
    assert dropped == []
    assert all("platf or m" not in line for line in out)
    assert any("platform_system" in line for line in out)
    assert any("sys_platform" in line for line in out)
