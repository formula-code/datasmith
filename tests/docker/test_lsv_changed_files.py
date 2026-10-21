"""Changed files given to LSV: links to folders or out of the repo are left out (dask#11303 SizexLN: `dask/repo ->
/workspace/repo` crashed the timing with "Is a directory")."""

from __future__ import annotations

import importlib.util
import os
import subprocess
from pathlib import Path

import pytest

_TESTS = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests"


def _load(name: str):
    spec = importlib.util.spec_from_file_location("_fc_changed_" + name, _TESTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def git(repo: Path, *a: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True, text=True).stdout


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    r = tmp_path / "repo"
    (r / "pkg" / "tests").mkdir(parents=True)
    (r / "pkg" / "mod.py").write_text("x = 1\n")
    (r / "pkg" / "tests" / "test_mod.py").write_text("def test_x():\n    pass\n")
    (tmp_path / "outside.py").write_text("y = 1\n")
    git(r, "init", "-q")
    git(r, "add", "-A")
    git(r, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "base")
    return r


def test_links_to_folders_or_out_of_the_repo_are_not_timed(repo: Path, monkeypatch) -> None:
    lsv = _load("lsv_measure")
    monkeypatch.setattr(lsv, "REPO_ROOT", repo)
    (repo / "pkg" / "mod.py").write_text("x = 2\n")
    (repo / "prof.out").write_bytes(b"\x00\x01binary\xff")
    os.symlink(repo, repo / "pkg" / "repo")
    os.symlink(repo.parent / "outside.py", repo / "pkg" / "out.py")
    os.symlink(repo / "pkg" / "mod.py", repo / "pkg" / "alias.py")
    got = sorted(
        Path(p).relative_to(repo).as_posix() for p in lsv.get_changed_files(git(repo, "rev-parse", "HEAD").strip())
    )
    assert got == ["pkg/alias.py", "pkg/mod.py", "prof.out"]


def test_a_link_to_the_repo_maps_to_no_tests(repo: Path) -> None:
    runner = _load("pytest_runner")
    os.symlink(repo, repo / "pkg" / "repo")
    assert runner.discover_test_files_from_changed(["pkg/repo", "pkg/mod.py"], str(repo))[0] == [
        "pkg/tests/test_mod.py"
    ]
