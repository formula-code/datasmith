"""run_base_and_diff: `ran` only when the base side produced results."""

import importlib.util
import json
import os
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


def test_base_side_imports_plugins_next_to_the_runner(runner, repo: Path, tmp_path_factory) -> None:
    tests_dir = tmp_path_factory.mktemp("tests")
    (tests_dir / "pytest_runner.py").write_text(_RUNNER.read_text())
    (tests_dir / "fc_plugin.py").write_text("")
    spec = importlib.util.spec_from_file_location("_fc_copied_runner", tests_dir / "pytest_runner.py")
    copied = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(copied)
    out = copied.run_base_and_diff(["test_m.py"], "-p fc_plugin", str(repo), AGENT)
    assert out["ran"] is True, out.get("base_error")


def test_rebuild_runs_at_base_and_again_after_restore(runner, repo: Path, monkeypatch) -> None:
    log = repo.parent / "rebuilds.log"
    monkeypatch.setenv("FC_REBUILD_CMD", f"grep -h assert test_m.py >> {log}")
    out = runner.run_base_and_diff(["test_m.py"], "", str(repo), AGENT)
    assert out["ran"] is True
    assert log.read_text().split() == ["assert", "True", "assert", "False"]
    monkeypatch.setenv("FC_REBUILD_CMD", "false")
    out = runner.run_base_and_diff(["test_m.py"], "", str(repo), AGENT)
    assert out["ran"] is False and "base rebuild failed" in out["base_error"]


def test_repo_package_wins_over_a_copy_in_site_packages(tmp_path_factory) -> None:
    # numpy#21464: a stale wheel of the package in site-packages, imported by a plugin, broke conftest collection.
    repo, site, tests_dir = (tmp_path_factory.mktemp(n) for n in ("repo", "site", "tests"))
    for root in (repo, site):
        (root / "pkg").mkdir()
        (root / "pkg" / "__init__.py").write_text("")
        (root / "pkg" / "conftest.py").write_text("")
    (repo / "pkg" / "tests").mkdir()
    (repo / "pkg" / "tests" / "__init__.py").write_text("")
    (repo / "pkg" / "tests" / "test_x.py").write_text(
        f"import pkg\n\ndef test_x():\n    assert pkg.__file__.startswith({str(repo)!r})\n"
    )
    (tests_dir / "pytest_runner.py").write_text(_RUNNER.read_text())
    (tests_dir / "fc_plugin.py").write_text("import pkg  # noqa: F401\n")
    script = (
        f"import sys, json, importlib.util; sys.path[0] = {str(tests_dir)!r}; "
        f"spec = importlib.util.spec_from_file_location('r', {str(tests_dir / 'pytest_runner.py')!r}); "
        "P = importlib.util.module_from_spec(spec); spec.loader.exec_module(P); "
        f"r = P.run_pytest_and_collect(['pkg/tests/test_x.py'], extra_args='-p fc_plugin -p no:cacheprovider', cwd={str(repo)!r}); "
        "print(json.dumps(r['summary']))"
    )
    env = {**os.environ, "PYTHONPATH": str(site)}
    proc = subprocess.run([sys.executable, "-c", script], cwd=repo, env=env, capture_output=True, text=True)
    summary = json.loads(proc.stdout.strip().splitlines()[-1])
    assert summary["passed"] == 1, proc.stdout + proc.stderr
