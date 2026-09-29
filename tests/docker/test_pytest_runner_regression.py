"""run_base_and_diff: `ran` only when the base side produced results."""

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

_RUNNER = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests" / "pytest_runner.py"


@pytest.fixture
def runner():
    spec = importlib.util.spec_from_file_location("_fc_template_pytest_runner", _RUNNER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    def git(*args):
        subprocess.run(["git", *args], cwd=tmp_path, check=True, capture_output=True)

    git("init", "-q")
    (tmp_path / "test_m.py").write_text("def test_a():\n    assert True\n")
    git("add", "-A")
    git("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "base")
    (tmp_path / "test_m.py").write_text("def test_a():\n    assert False\n")
    return tmp_path


AGENT = {"tests": [{"nodeid": "test_m.py::test_a", "when": "call", "outcome": "failed"}]}


def test_base_results_mean_ran_and_regressions_count(runner, repo: Path) -> None:
    out = runner.run_base_and_diff(["test_m.py"], "", str(repo), AGENT)
    assert out["ran"] is True
    assert out["regressed"] == ["test_m.py::test_a"]
    assert "assert False" in (repo / "test_m.py").read_text()


def test_no_base_results_is_not_ran(runner, repo: Path, monkeypatch) -> None:
    real = runner._run
    monkeypatch.setattr(runner, "_run", lambda cmd, cwd=None: (1, "", "boom") if cmd[0] == sys.executable else real(cmd, cwd))
    out = runner.run_base_and_diff(["test_m.py"], "", str(repo), AGENT)
    assert out["ran"] is False
    assert out["stashed"] is True
    assert "no results" in out["base_error"]
    assert "assert False" in (repo / "test_m.py").read_text()
