"""A requirements file is not a declaration, and a directory holding one is not a package.

Globbing ``requirements*.txt`` and reading ``environment.yml`` is how ``sphinx``,
``towncrier``, ``torch``, ``cupy`` and ``conda-build`` became *runtime*
dependencies (scipy's environment was unsatisfiable because a documentation
requirement pulled in a yanked ``conda-build``), and how conda-only names such as
``boost-cpp`` and ``libprotobuf`` -- which do not exist on PyPI at all -- reached
the resolver.

Discovering those files also had a second effect: a directory that holds one and
no packaging file became a candidate packaging *root*.  That is what scipp's
``binder`` directory is, and picking it means resolving the Binder configuration
instead of the project.
"""

from __future__ import annotations

from dataclasses import fields
from pathlib import Path

import pytest

from datasmith.resolution import metadata_parser
from datasmith.resolution.declare import declare
from datasmith.resolution.metadata_parser import analyze_candidate_meta, discover_candidates
from datasmith.resolution.models import Candidate
from git import Actor, Repo

PYPROJECT = """\
[project]
name = "demo"
version = "1.0"
dependencies = ["numpy>=1.25"]
"""

DOC_REQUIREMENTS = """\
sphinx==7.2.6
towncrier
conda-build
"""

BINDER_REQUIREMENTS = """\
jupyterlab
"""

ENVIRONMENT_YML = """\
name: demo
dependencies:
  - boost-cpp
  - libprotobuf
"""


@pytest.fixture
def repo_commit(tmp_path: Path):
    """A repo shaped like the ones the audit tripped over."""
    repo = Repo.init(tmp_path)
    (tmp_path / "pyproject.toml").write_text(PYPROJECT)
    (tmp_path / "doc").mkdir()
    (tmp_path / "doc" / "requirements.txt").write_text(DOC_REQUIREMENTS)
    (tmp_path / "binder").mkdir()
    (tmp_path / "binder" / "requirements.txt").write_text(BINDER_REQUIREMENTS)
    (tmp_path / "environment.yml").write_text(ENVIRONMENT_YML)
    repo.index.add(["pyproject.toml", "doc/requirements.txt", "binder/requirements.txt", "environment.yml"])
    who = Actor("fc-data tests", "tests@example.invalid")
    return repo.index.commit("initial", author=who, committer=who)


def test_a_requirements_only_directory_is_not_a_packaging_root(repo_commit):
    # 'binder' and 'doc' hold a requirements.txt and nothing else. Neither
    # declares an installable package, so neither may compete to be the root.
    assert set(discover_candidates(repo_commit)) == {"."}


def test_declared_runtime_holds_only_what_the_project_declares(repo_commit):
    candidates = discover_candidates(repo_commit)
    declared = declare(analyze_candidate_meta(candidates["."]), None)

    assert declared.runtime == ["numpy>=1.25"]
    # Nothing from the requirements files or the conda file, and no drop record
    # either -- those lines were never read, so there is nothing to report.
    assert declared.dropped == []


def test_the_harvest_is_deleted():
    for gone in ("parse_requirements_txt", "parse_conda_env_yaml"):
        assert not hasattr(metadata_parser, gone), f"{gone} still exists"
    names = {f.name for f in fields(Candidate)}
    assert "req_files" not in names
    assert "env_yamls" not in names
