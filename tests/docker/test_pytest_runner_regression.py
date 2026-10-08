"""run_base_and_diff and restore_test_edits: the base side runs at the base commit and the agent cannot weaken tests."""

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
    monkeypatch.setattr(
        runner, "_run", lambda cmd, cwd=None: (1, "", "boom") if cmd[0] == sys.executable else real(cmd, cwd)
    )
    out = runner.run_base_and_diff(["test_m.py"], "", str(repo), AGENT)
    assert out["ran"] is False
    assert out["reverted"] is True
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


def _git(cwd: Path, *args) -> str:
    return subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout


@pytest.fixture
def src_repo(tmp_path: Path) -> tuple[Path, str]:
    root = tmp_path / "repo"
    (root / "pkg").mkdir(parents=True)
    _git(root, "init", "-q")
    (root / "pkg" / "m.py").write_text("def f():\n    return 1\n")
    (root / "pkg" / "test_m.py").write_text("from m import f\n\ndef test_f():\n    assert f() == 1\n")
    (root / "old.py").write_text("x = 1\n")
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "fc-baseline")
    return root, _git(root, "rev-parse", "HEAD").strip()


def _run_runner(root: Path, base: str, logs: Path) -> tuple[dict, dict]:
    env = dict(os.environ, T_BENCH_TASK_LOGS_PATH=str(logs))
    subprocess.run(
        [sys.executable, str(_RUNNER), "--base", base, "--root", str(root)],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
    )
    return json.loads((logs / "test_results.json").read_text()), json.loads((logs / "test_edits.json").read_text())


def test_weakened_test_is_restored_and_regression_reported(src_repo, tmp_path: Path) -> None:
    root, base = src_repo
    (root / "pkg" / "m.py").write_text("def f():\n    return 2\n")
    (root / "pkg" / "test_m.py").write_text("from m import f\n\ndef test_f():\n    assert f() == 2\n")
    (root / "pkg" / "test_new.py").write_text("def test_n():\n    assert True\n")
    results, edits = _run_runner(root, base, tmp_path / "logs")
    assert edits["modified"] == ["pkg/test_m.py"] and edits["added"] == ["pkg/test_new.py"]
    assert not (root / "pkg" / "test_new.py").exists()
    assert results["regression"]["regressed"] == ["pkg/test_m.py::test_f"]


def test_committed_agent_change_is_reverted_for_base_run(src_repo, tmp_path: Path) -> None:
    root, base = src_repo
    (root / "pkg" / "m.py").write_text("def f():\n    return 2\n")
    _git(root, "commit", "-qam", "agent")
    results, _ = _run_runner(root, base, tmp_path / "logs")
    assert results["regression"]["ran"] is True
    assert results["regression"]["regressed"] == ["pkg/test_m.py::test_f"]


def test_agent_tree_is_restored_byte_for_byte(runner, src_repo, monkeypatch) -> None:
    root, base = src_repo
    (root / "pkg" / "m.py").write_text("def f():\n    return 2\n")
    _git(root, "commit", "-qam", "agent")
    (root / "pkg" / "test_m.py").write_text("from m import f\n\ndef test_f():\n    assert f() == 3\n")
    (root / "new").mkdir()
    (root / "new" / "new.py").write_bytes(b"y = 2\r\n")
    (root / "old.py").unlink()
    _git(root, "add", "pkg/test_m.py")

    def snapshot():
        files = {
            p.relative_to(root): p.read_bytes()
            for p in root.rglob("*")
            if p.is_file() and not str(p.relative_to(root)).startswith(".") and "__pycache__" not in p.parts
        }
        return files, _git(root, "status", "--porcelain"), _git(root, "rev-parse", "HEAD")

    before = snapshot()
    real = runner._run
    monkeypatch.setattr(
        runner,
        "_run",
        lambda cmd, cwd=None: (_ for _ in ()).throw(KeyboardInterrupt) if cmd[0] == sys.executable else real(cmd, cwd),
    )
    with pytest.raises(KeyboardInterrupt):
        runner.run_base_and_diff(["pkg/test_m.py"], "", str(root), AGENT, base_ref=base)
    assert snapshot() == before
    monkeypatch.setattr(runner, "_run", real)
    assert runner.run_base_and_diff(["pkg/test_m.py"], "", str(root), AGENT, base_ref=base)["ran"] is True
    assert snapshot() == before


def test_failed_restore_sets_restore_ok_false_and_exits_nonzero(src_repo, tmp_path: Path) -> None:
    root, base = src_repo
    (root / "pkg" / "m.py").write_text("def f():\n    return 2\n")
    logs = tmp_path / "logs"
    env = dict(os.environ, T_BENCH_TASK_LOGS_PATH=str(logs), FC_REBUILD_CMD="grep -q 'return 1' pkg/m.py")
    proc = subprocess.run(
        [sys.executable, str(_RUNNER), "--base", base, "--root", str(root)], cwd=root, env=env, capture_output=True
    )
    regression = json.loads((logs / "test_results.json").read_text())["regression"]
    assert proc.returncode == 3
    assert regression["restore_ok"] is False and "patched rebuild after restore failed" in regression["base_error"]


def test_base_pass_test_skipped_by_agent_code_is_a_regression(src_repo, tmp_path: Path) -> None:
    root, base = src_repo
    (root / "pkg" / "m.py").write_text("import pytest\n\ndef f():\n    pytest.skip('no')\n")
    results, _ = _run_runner(root, base, tmp_path / "logs")
    assert results["regression"]["regressed"] == ["pkg/test_m.py::test_f"]


def test_base_pass_test_not_collected_is_a_regression(src_repo, tmp_path: Path) -> None:
    root, base = src_repo
    (root / "pkg" / "m.py").write_text("raise ImportError('broken')\n")
    results, _ = _run_runner(root, base, tmp_path / "logs")
    assert results["regression"]["regressed"] == ["pkg/test_m.py::test_f"]


def test_weakened_testing_helper_is_restored(src_repo, tmp_path: Path) -> None:
    root, _ = src_repo
    (root / "pkg" / "testing").mkdir()
    (root / "pkg" / "testing" / "__init__.py").write_text("")
    (root / "pkg" / "testing" / "helpers.py").write_text("def check(x):\n    assert x == 1\n")
    (root / "pkg" / "test_m.py").write_text(
        "from m import f\nfrom testing.helpers import check\n\ndef test_f():\n    check(f())\n"
    )
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "helpers")
    base = _git(root, "rev-parse", "HEAD").strip()
    (root / "pkg" / "m.py").write_text("def f():\n    return 2\n")
    (root / "pkg" / "testing" / "helpers.py").write_text("def check(x):\n    pass\n")
    results, edits = _run_runner(root, base, tmp_path / "logs")
    assert edits["modified"] == ["pkg/testing/helpers.py"]
    assert results["regression"]["regressed"] == ["pkg/test_m.py::test_f"]


def _base_config_in_setup_cfg(root: Path) -> str:
    (root / "setup.cfg").write_text("[tool:pytest]\naddopts = -ra\n")
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", "config")
    return _git(root, "rev-parse", "HEAD").strip()


def test_agent_pyproject_dotted_key_config_is_not_read(src_repo, tmp_path: Path) -> None:
    root, _ = src_repo
    base = _base_config_in_setup_cfg(root)
    (root / "pyproject.toml").write_text('[tool]\npytest.ini_options.addopts = "--deselect pkg/test_m.py::test_f"\n')
    (root / "pkg" / "m.py").write_text("def f():\n    return 2\n")
    results, _ = _run_runner(root, base, tmp_path / "logs")
    cfg = results["results"]["config_file"]
    assert cfg.endswith("setup.cfg") and "fc_pytest_cfg_" in cfg
    assert results["regression"]["regressed"] == ["pkg/test_m.py::test_f"]


def test_added_dot_pytest_ini_is_not_read(src_repo, tmp_path: Path) -> None:
    root, _ = src_repo
    base = _base_config_in_setup_cfg(root)
    (root / ".pytest.ini").write_text("[pytest]\naddopts = --deselect pkg/test_m.py::test_f\n")
    (root / "pkg" / "m.py").write_text("def f():\n    return 2\n")
    results, _ = _run_runner(root, base, tmp_path / "logs")
    assert "fc_pytest_cfg_" in results["results"]["config_file"]
    assert results["regression"]["regressed"] == ["pkg/test_m.py::test_f"]


def test_plugin_loaded_only_in_the_agent_run_is_a_regression(src_repo, tmp_path: Path) -> None:
    root, base = src_repo
    dist = root / "fcevil-0.dist-info"
    dist.mkdir()
    (dist / "METADATA").write_text("Metadata-Version: 2.1\nName: fcevil\nVersion: 0\n")
    (dist / "entry_points.txt").write_text("[pytest11]\nfcevil = fcevil_mod\n")
    (root / "fcevil_mod.py").write_text("")
    (root / "pkg" / "m.py").write_text("def f():\n    return 1  # agent\n")
    results, _ = _run_runner(root, base, tmp_path / "logs")
    assert results["regression"]["agent_only_plugins"] == ["fcevil"]
    assert "plugin:fcevil" in results["regression"]["regressed"]
