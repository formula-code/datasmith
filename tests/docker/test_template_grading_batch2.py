"""Template grading rules: staged and new files count, skipped tests are regressions, the base pytest config, broken
benchmarks and the pytest time-out record."""

from __future__ import annotations

import importlib.util
import json
import subprocess
from pathlib import Path

import pytest

_TESTS = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests"


def _load(name: str):
    spec = importlib.util.spec_from_file_location("_fc_" + name, _TESTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def runner():
    return _load("pytest_runner")


def git(repo: Path, *a: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *a], check=True, capture_output=True, text=True).stdout


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    git(tmp_path, "init", "-q")
    (tmp_path / "m.py").write_text("x = 1\n")
    (tmp_path / "setup.cfg").write_text("[tool:pytest]\naddopts = -q\n")
    git(tmp_path, "add", "-A")
    git(tmp_path, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "base")
    return tmp_path


def test_staged_and_new_files_are_changed_files(runner, repo: Path) -> None:
    (repo / "m.py").write_text("x = 2\n")
    git(repo, "add", "m.py")
    (repo / "new.py").write_text("y = 1\n")
    (repo / ".fc_base_pytest.cfg").write_text("")
    kw = {"base_ref": "HEAD", "repo_root": str(repo)}
    assert runner.get_changed_files(**kw) == []
    assert runner.get_changed_files(include_staged=True, include_untracked=True, **kw) == ["m.py", "new.py"]


def test_skipped_or_missing_with_the_patch_is_a_regression(runner) -> None:
    base = {"t.py::a": "passed", "t.py::b": "passed", "t.py::c[1]": "passed", "t.py::d": "passed", "u.py::e": "passed"}
    agent = {"t.py::a": "passed", "t.py::b": "skipped", "t.py::c[one]": "passed", "u.py": "error"}
    assert runner.skipped_or_missing(base, agent) == (["t.py::b"], ["t.py::d"])


def test_base_pytest_config_is_found_in_pytest_order(runner, repo: Path) -> None:
    path = runner.write_base_pytest_config("HEAD", str(repo))
    assert Path(path).name == ".fc_base_pytest.cfg" and "[tool:pytest]" in Path(path).read_text()
    (repo / "pytest.ini").write_text("[pytest]\naddopts = -q\n")
    git(repo, "add", "-A")
    git(repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "ini")
    assert Path(runner.write_base_pytest_config("HEAD", str(repo))).name == ".fc_base_pytest.ini"


def test_an_agent_pytest_ini_that_deselects_a_failing_test_is_not_read(runner, repo: Path, monkeypatch) -> None:
    (repo / "test_fc_deselect_case.py").write_text("def test_a():\n    assert False\n")
    monkeypatch.setenv("FC_PYTEST_CONFIG", runner.write_base_pytest_config("HEAD", str(repo)))
    (repo / "pytest.ini").write_text("[pytest]\naddopts = --deselect test_fc_deselect_case.py::test_a\n")
    res = runner.run_pytest_and_collect(["test_fc_deselect_case.py"], extra_args="-p no:cacheprovider", cwd=str(repo))
    assert {t["nodeid"]: t["outcome"] for t in res["tests"] if t.get("when") == "call"} == {
        "test_fc_deselect_case.py::test_a": "failed"
    }


def test_benchmarks_timed_only_at_base_are_listed() -> None:
    lsv = _load("lsv_measure")
    rounds = [{"base": {"a": 1.0, "b": 1.0}, "patched": {"a": 0.9}} for _ in range(5)]
    rounds[0]["patched"]["b"] = None
    assert lsv.broke_by_patch(rounds, 3) == ["b"]


def test_parser_records_the_pytest_time_out(tmp_path: Path, monkeypatch) -> None:
    parser = _load("parser")
    monkeypatch.setattr(parser, "LOG_DIR", tmp_path)
    assert parser._pytest_status() == {
        "pytest_runner_exit_code": None,
        "pytest_timed_out": False,
        "pytest_base_also_timed_out": False,
    }
    (tmp_path / "pytest_status.json").write_text(
        json.dumps({"runner_exit_code": 124, "timed_out": True, "base_exit_code": 124, "base_also_timed_out": True})
    )
    assert parser._pytest_status()["pytest_base_also_timed_out"] is True
