"""A commented requirements-file line must keep its package all the way to uv.

The comment is stripped where the line is read, not later: ``clean_pinned``
collapses whitespace, so a comment that survives that far is glued to the
version (``numpy>=1.25#pinnedfortheABI``), stops looking like a comment, and
takes the package with it when the parser rejects the result.

``filter_requirements_for_pypi`` is the other boundary that has to be clean:
its output is the seed stored on the row, and the resolver's pass-through path
(one commit in five, per the audit) stores it without ever handing it to uv.
"""

from __future__ import annotations

from pathlib import Path

from datasmith.resolution import package_filters as pf
from datasmith.resolution.package_filters import clean_pinned, filter_requirements_for_pypi, resolve_requirements_file
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


def test_the_stored_seed_carries_no_comment(tmp_path: Path):
    # The pass-through path stores this list verbatim as env_payload, so a
    # comment left here is a comment in the dataset.
    kept = filter_requirements_for_pypi(
        ["numpy>=1.25  # pinned for the ABI", "# a whole-line comment", "scipy>=1.10"],
        project_dir=tmp_path,
        own_import_name=None,
    )
    assert sorted(kept) == ["numpy>=1.25", "scipy>=1.10"]


def test_stripping_the_comment_does_not_damage_a_url(tmp_path: Path):
    url = "https://files.pythonhosted.org/packages/x/foo-1.0-py3-none-any.whl"
    kept = filter_requirements_for_pypi(
        [url, f"{url}  # the vendored wheel"], project_dir=tmp_path, own_import_name=None
    )
    assert kept == [url]
