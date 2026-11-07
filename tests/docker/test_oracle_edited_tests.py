"""Base tests that the oracle patch edits or deletes are left out of the pytest regression check (dask#11373: three
of the four failing tests were asserts that the oracle patch deletes)."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from datasmith.harbor_adapter.test_edits import oracle_test_edits

_RUNNER = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests" / "pytest_runner.py"

BASE_TESTS = """import pytest

from pkg.seen import normalize, tmp_a, tmp_b


@pytest.fixture
def plain():
    return 1


def test_memoized():
    assert normalize([1, 1]) == ["x", "seen"]


@pytest.mark.parametrize("n", [1, 2])
def test_kept(n):
    assert normalize([n]) == ["x"]


def test_cycle():
    assert normalize([]) == []


class TestGroup:
    def setup_method(self):
        self.v = 1

    def test_a(self):
        assert self.v == 1

    def test_b(self):
        assert normalize([2, 2]) == ["x", "seen"]
"""

ORACLE_TESTS = """import pytest

from pkg.seen import normalize, other, tmp_a, tmp_b


@pytest.fixture
def plain():
    return 1


def test_memoized():
    assert normalize([1, 1])


@pytest.mark.parametrize("n", [1, 2])
def test_kept(n):
    assert normalize([n]) == ["x"]


def test_new():
    assert other()


def test_cycle():
    assert normalize([]) == []


class TestGroup:
    def setup_method(self):
        self.v = 1

    def test_a(self):
        assert self.v == 1

    @pytest.mark.skip
    def test_b(self):
        assert normalize([2, 2]) == ["x", "seen"]

    def test_c(self):
        pass
"""


@pytest.fixture
def runner():
    spec = importlib.util.spec_from_file_location("_fc_template_pytest_runner_edited", _RUNNER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def git(repo: Path, *a: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True, text=True).stdout


def commit(repo: Path, msg: str) -> str:
    git(repo, "add", "-A")
    git(repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", msg)
    return git(repo, "rev-parse", "HEAD").strip()


def oracle_patch(repo: Path, files: dict[str, str | None]) -> str:
    """The patch from HEAD to ``files`` (None deletes), with the repo left at HEAD."""
    for rel, text in files.items():
        if text is None:
            (repo / rel).unlink()
        else:
            (repo / rel).parent.mkdir(parents=True, exist_ok=True)
            (repo / rel).write_text(text)
    git(repo, "add", "-A")
    patch = git(repo, "diff", "--cached", "HEAD")
    git(repo, "reset", "-q", "--hard", "HEAD")
    return patch


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    r = tmp_path / "repo"
    (r / "pkg" / "tests").mkdir(parents=True)
    (r / "pkg" / "__init__.py").write_text("")
    (r / "pkg" / "seen.py").write_text(
        "tmp_a = tmp_b = 0\n\n\ndef other():\n    return 1\n\n\ndef normalize(seq):\n"
        "    out, seen = [], set()\n    for v in seq:\n        out.append('seen' if v in seen else 'x')\n"
        "        seen.add(v)\n    return out\n"
    )
    (r / "pkg" / "tests" / "test_seen.py").write_text(BASE_TESTS)
    (r / "pkg" / "tests" / "test_gone.py").write_text("def test_gone():\n    pass\n")
    git(r, "init", "-q")
    commit(r, "base")
    return r


def test_render_records_base_lines_of_changed_test_files(repo: Path) -> None:
    patch = oracle_patch(
        repo,
        {
            "pkg/tests/test_seen.py": ORACLE_TESTS,
            "pkg/tests/test_gone.py": None,
            "pkg/tests/test_added.py": "def test_added():\n    pass\n",
            "pkg/seen.py": "tmp_a = tmp_b = 1\n",
        },
    )
    edits = oracle_test_edits(patch)
    assert sorted(edits) == ["pkg/tests/test_gone.py", "pkg/tests/test_seen.py"]
    assert edits["pkg/tests/test_gone.py"]["deleted"] is True
    seen = edits["pkg/tests/test_seen.py"]
    assert seen["deleted"] is False and seen["removed"] == [3, 12]
    # test_new after base line 19, the skip mark before test_b (base line 31), test_c after test_b.
    assert seen["inserts"] == [[19, 0, False], [30, 4, True], [32, 4, False]]
    assert "normalize" not in seen["dropped_names"] and "tmp_a" not in seen["dropped_names"]


def test_changed_tests_and_classes_are_found_in_the_base_file(runner, repo: Path) -> None:
    edits = oracle_test_edits(oracle_patch(repo, {"pkg/tests/test_seen.py": ORACLE_TESTS}))["pkg/tests/test_seen.py"]
    p = "pkg/tests/test_seen.py"
    assert runner.edited_test_prefixes(p, BASE_TESTS, edits) == [p + "::TestGroup::test_b", p + "::test_memoized"]


def test_a_changed_fixture_or_a_dropped_import_name_is_the_whole_file(runner, repo: Path) -> None:
    p = "pkg/tests/test_seen.py"
    fixture = BASE_TESTS.replace("    return 1\n", "    return 2\n", 1)
    edits = oracle_test_edits(oracle_patch(repo, {p: fixture}))[p]
    assert runner.edited_test_prefixes(p, BASE_TESTS, edits) == [p]
    no_tmp_b = BASE_TESTS.replace("tmp_a, tmp_b", "tmp_a")
    edits = oracle_test_edits(oracle_patch(repo, {p: no_tmp_b}))[p]
    assert runner.edited_test_prefixes(p, BASE_TESTS, edits) == [p]
    in_class = BASE_TESTS.replace("self.v = 1", "self.v = 2")
    edits = oracle_test_edits(oracle_patch(repo, {p: in_class}))[p]
    assert runner.edited_test_prefixes(p, BASE_TESTS, edits) == [p + "::TestGroup"]


def test_conftest_and_deleted_files(runner, repo: Path) -> None:
    (repo / "pkg" / "tests" / "conftest.py").write_text("X = 1\n")
    base = commit(repo, "conftest")
    patch = oracle_patch(repo, {"pkg/tests/conftest.py": "X = 2\n", "pkg/tests/test_gone.py": None})
    path = repo.parent / "edits.json"
    path.write_text(json.dumps(oracle_test_edits(patch)))
    prefixes = runner.load_oracle_edited(str(path), str(repo), base)
    assert prefixes == ["pkg/tests/", "pkg/tests/test_gone.py"]
    assert runner.oracle_edited("pkg/tests/test_seen.py::test_cycle", prefixes)
    assert not runner.oracle_edited("other/test_x.py::test_y", prefixes)


def test_node_id_matching(runner) -> None:
    prefixes = ["a/test_x.py::test_m", "a/test_x.py::TestG", "b/test_y.py"]
    assert runner.oracle_edited("a/test_x.py::test_m[1-2]", prefixes)
    assert runner.oracle_edited("a/test_x.py::TestG::test_a", prefixes)
    assert runner.oracle_edited("b/test_y.py", prefixes) and runner.oracle_edited("b/test_y.py::test_z", prefixes)
    assert not runner.oracle_edited("a/test_x.py::test_m2", prefixes)
    assert not runner.oracle_edited("a/test_x.py", prefixes)


def test_a_fix_like_the_oracle_fails_only_tests_the_oracle_edits(repo: Path, tmp_path: Path) -> None:
    base = git(repo, "rev-parse", "HEAD").strip()
    patch = oracle_patch(repo, {"pkg/tests/test_seen.py": ORACLE_TESTS})
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    shutil.copy(_RUNNER, tests_dir / "pytest_runner.py")
    (tests_dir / "oracle_test_edits.json").write_text(json.dumps(oracle_test_edits(patch)))
    # No memoization (as the oracle) and a broken empty case (a real regression in a test the oracle keeps).
    (repo / "pkg" / "seen.py").write_text(
        "tmp_a = tmp_b = 0\n\n\ndef other():\n    return 1\n\n\ndef normalize(seq):\n    return ['x' for v in seq] or None\n"
    )
    logs = tmp_path / "logs"
    env = dict(os.environ, T_BENCH_TASK_LOGS_PATH=str(logs), FC_PYTEST_TIMEOUT="600", FC_WIDE_TESTS_SEC="0")
    subprocess.run(
        [sys.executable, str(tests_dir / "pytest_runner.py"), "--base", base],
        cwd=repo,
        env=env,
        check=True,
        capture_output=True,
    )
    reg = json.loads((logs / "test_results.json").read_text())["regression"]
    assert reg["regressed"] == ["pkg/tests/test_seen.py::test_cycle"]
    assert reg["oracle_edited"] == [
        "pkg/tests/test_seen.py::TestGroup::test_b",
        "pkg/tests/test_seen.py::test_memoized",
    ]
