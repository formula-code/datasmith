"""fix_marker_spacing must be gone, and no caller may resurrect its behaviour."""

import subprocess


def test_fix_marker_spacing_is_deleted():
    import datasmith.resolution.package_filters as pf

    assert not hasattr(pf, "fix_marker_spacing")


def test_no_source_file_references_it():
    cp = subprocess.run(
        ["grep", "-rn", "fix_marker_spacing", "src/datasmith"],
        capture_output=True,
        text=True,
    )
    assert cp.stdout == "", f"stale references remain:\n{cp.stdout}"


def test_requirements_reach_uv_unmodified():
    from datasmith.resolution.requirements import parse_many, render

    raw = ["numpy; platform_system=='Windows'", "scipy; sys_platform=='linux'"]
    reqs, dropped = parse_many(raw)
    out = render(reqs)
    assert dropped == []
    assert all("platf or m" not in line for line in out)
    assert any("platform_system" in line for line in out)
    assert any("sys_platform" in line for line in out)
