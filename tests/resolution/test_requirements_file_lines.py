"""A commented requirements-file line must keep its package all the way to uv.

The comment is stripped where the line is read, not later: ``clean_pinned``
collapses whitespace, so a comment that survives that far is glued to the
version (``numpy>=1.25#pinnedfortheABI``), stops looking like a comment, and
takes the package with it when the parser rejects the result.
"""

from __future__ import annotations

from datasmith.resolution import package_filters as pf
from datasmith.resolution.package_filters import clean_pinned, resolve_requirements_file
from datasmith.resolution.requirements import to_requirement_lines

REQUIREMENTS_TXT = """\
# runtime dependencies
numpy>=1.25  # pinned for the ABI
scipy>=1.10,<=1.14   # upper bound for the C API

pandas
"""


def test_a_commented_line_keeps_its_package(monkeypatch):
    monkeypatch.setattr(pf, "read_blob_text", lambda _commit, _path, *a, **k: REQUIREMENTS_TXT)
    found = resolve_requirements_file(object(), "requirements.txt", set())
    assert found == {"numpy>=1.25", "scipy>=1.10,<=1.14", "pandas"}


def test_the_package_survives_clean_pinned_and_reaches_uv(monkeypatch):
    monkeypatch.setattr(pf, "read_blob_text", lambda _commit, _path, *a, **k: REQUIREMENTS_TXT)
    found = resolve_requirements_file(object(), "requirements.txt", set())

    # clean_pinned is the whitespace-collapsing pass that runs on the second
    # resolve attempt; it must not be able to eat a package.
    lines, dropped = to_requirement_lines(clean_pinned(sorted(found)))
    assert dropped == []
    names = {line.split("<")[0].split(">")[0].split("=")[0] for line in lines}
    assert names == {"numpy", "scipy", "pandas"}


def test_clean_pinned_does_not_fuse_a_comment_to_a_version():
    assert clean_pinned(["numpy>=1.25  # pinned for the ABI"]) == ["numpy>=1.25"]
    assert clean_pinned(["# a whole-line comment"]) == []
