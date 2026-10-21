"""Changed source file to test files: the package path first, the file name only when the package has none."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_RUNNER = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests" / "pytest_runner.py"


@pytest.fixture
def runner():
    spec = importlib.util.spec_from_file_location("_fc_template_pytest_runner_mapping", _RUNNER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def touch(root: Path, *paths: str) -> None:
    for p in paths:
        (root / p).parent.mkdir(parents=True, exist_ok=True)
        (root / p).write_text("")


def mapped(runner, root: Path, path: str) -> list[str]:
    return runner.discover_test_files_from_changed([path], str(root))[1].get(path, [])


def test_dask_core_files_map_to_their_own_package_tests(runner, tmp_path: Path) -> None:
    touch(
        tmp_path, "dask/core.py", "dask/array/core.py", "dask/tests/test_core.py", "dask/array/tests/test_array_core.py"
    )
    assert mapped(runner, tmp_path, "dask/core.py") == ["dask/tests/test_core.py"]
    assert mapped(runner, tmp_path, "dask/array/core.py") == ["dask/array/tests/test_array_core.py"]


def test_package_name_test_file_when_no_module_test_file(runner, tmp_path: Path) -> None:
    touch(tmp_path, "dask/dataframe/core.py", "dask/tests/test_core.py", "dask/dataframe/tests/test_dataframe.py")
    assert mapped(runner, tmp_path, "dask/dataframe/core.py") == ["dask/dataframe/tests/test_dataframe.py"]


def test_same_name_in_the_package_tests_folder(runner, tmp_path: Path) -> None:
    touch(tmp_path, "dask/array/core.py", "dask/tests/test_core.py", "dask/array/tests/test_core.py")
    assert mapped(runner, tmp_path, "dask/array/core.py") == ["dask/array/tests/test_core.py"]


def test_private_module_and_sibling_test(runner, tmp_path: Path) -> None:
    touch(tmp_path, "pkg/metrics/_ranking.py", "pkg/metrics/tests/test_ranking.py", "pkg/util.py", "pkg/test_util.py")
    assert mapped(runner, tmp_path, "pkg/metrics/_ranking.py") == ["pkg/metrics/tests/test_ranking.py"]
    assert mapped(runner, tmp_path, "pkg/util.py") == ["pkg/test_util.py"]


def test_file_name_is_the_fallback(runner, tmp_path: Path) -> None:
    touch(tmp_path, "src/pkg/x.py", "tests/unit/test_x.py")
    assert mapped(runner, tmp_path, "src/pkg/x.py") == ["tests/unit/test_x.py"]
