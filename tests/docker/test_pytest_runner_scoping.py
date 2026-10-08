"""Collection must be scoped, and one bad module must not hide the suite.

Two defects, both of which made a healthy repository look like a broken one.

`run_pytest_and_collect([])` handed pytest an empty argument list, which means
"collect from rootdir". A repo that sets `--doctest-modules` in addopts and
declares no `testpaths` then imports every .py in the tree. CalebBell/fluids#38
died on `ERROR docs/conf.py` and `ERROR jinja_patch_plugin_pandas.py` -- neither
is a test, and no test ever ran.

Separately, pytest stops at the first collection error by default. One optional
dependency missing in one test module reported zero tests, which is
indistinguishable from a repository with no suite at all.
NCAR/geocat-comp#748 (dask) and AllenCellModeling/aicsimageio#486 (bioformats)
both aborted that way with the rest of the suite never attempted.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_RUNNER = Path(__file__).parents[2] / "src" / "datasmith" / "docker" / "templates" / "pytest_runner.py"


@pytest.fixture(scope="module")
def runner():
    spec = importlib.util.spec_from_file_location("_fc_pytest_runner", _RUNNER)
    module = importlib.util.module_from_spec(spec)
    sys.modules["_fc_pytest_runner"] = module
    spec.loader.exec_module(module)
    return module


def test_conventional_test_dir_is_used(runner, tmp_path: Path) -> None:
    (tmp_path / "tests").mkdir()
    (tmp_path / "docs").mkdir()
    assert runner.default_test_paths(str(tmp_path)) == [str(tmp_path / "tests")]


def test_repo_that_declares_testpaths_is_left_alone(runner, tmp_path: Path) -> None:
    """pytest already scopes itself from testpaths. Overriding it is wrong."""
    (tmp_path / "tests").mkdir()
    (tmp_path / "pyproject.toml").write_text(
        "[tool.pytest.ini_options]\ntestpaths = ['integration']\n", encoding="utf-8"
    )
    assert runner.default_test_paths(str(tmp_path)) == []


def test_package_internal_test_dir_is_found(runner, tmp_path: Path) -> None:
    """src-layout and package-internal suites, e.g. aicsimageio/tests."""
    (tmp_path / "mypkg" / "tests").mkdir(parents=True)
    assert runner.default_test_paths(str(tmp_path)) == [str(tmp_path / "mypkg" / "tests")]


def test_no_test_dir_falls_back_to_rootdir(runner, tmp_path: Path) -> None:
    """Returning [] means "let pytest decide", which is the honest default."""
    (tmp_path / "docs").mkdir()
    assert runner.default_test_paths(str(tmp_path)) == []


def test_the_fluids_layout_excludes_the_files_that_broke_it(runner, tmp_path: Path) -> None:
    """The exact shape of CalebBell/fluids: doctest-modules, no testpaths."""
    (tmp_path / "tests").mkdir()
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "conf.py").write_text("import nonexistent_module\n", encoding="utf-8")
    (tmp_path / "jinja_patch_plugin_pandas.py").write_text("import nonexistent_module\n", encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text(
        '[tool.pytest.ini_options]\naddopts = "--doctest-modules"\n', encoding="utf-8"
    )
    selected = runner.default_test_paths(str(tmp_path))
    assert selected == [str(tmp_path / "tests")]
    joined = " ".join(selected)
    assert "conf.py" not in joined
    assert "jinja_patch_plugin_pandas" not in joined


def test_collection_errors_do_not_hide_passing_tests(runner, tmp_path: Path) -> None:
    """A real pytest run: one unimportable module, one good test file."""
    tests = tmp_path / "tests"
    tests.mkdir()
    (tests / "test_broken.py").write_text("import a_module_that_does_not_exist\n", encoding="utf-8")
    (tests / "test_fine.py").write_text("def test_one():\n    assert True\n", encoding="utf-8")

    results = runner.run_pytest_and_collect([str(tests)], cwd=str(tmp_path))

    assert results["summary"]["passed"] == 1, "the good test must still run"
    assert results["summary"]["error"] >= 1, "the import failure must still be counted"
    assert results["errors"], "and must be reported, not swallowed"
