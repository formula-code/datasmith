"""test.sh times the patch after pytest and snapshots, and skips the timing when they already decide the reward."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from datasmith.harbor_adapter.utils import render_template

_TESTS = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"fc_{name}", _TESTS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir(parents=True)
    git = ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@t"]
    (repo / "core.py").write_text("x = 1\n")
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run([*git, "add", "-A"], check=True)
    subprocess.run([*git, "commit", "-q", "-m", "base"], check=True)
    (repo / "core.py").write_text("x = 2\n")
    (repo / "new.py").write_text("y = 1\n")
    return repo


def _run(
    tmp_path: Path,
    *,
    regressed: list[str] | None = None,
    runner: str = "",
    snapshot: dict | None = None,
    patch_files: int = 2,
    agent: str = "qwen-coder",
    owner: str = "o",
    env: dict[str, str] | None = None,
    base: str | None = None,
    snapshot_cmd: str | None = None,
) -> tuple[list[str], Path]:
    repo, log = _repo(tmp_path), tmp_path / "logs"
    log.mkdir()
    (log / ".snapshots").mkdir()
    (log / ".snapshots" / "baseline.json").write_text("{}")
    text = render_template(
        "tests/test.sh",
        base_commit="BASE",
        run_pytest=True,
        rounds=None,
        owner=owner,
        repo="r",
        issue_number=1,
        task_id=f"{owner}__r__1",
    )
    head = text[: text.index("\nset -u\n", text.index("set +u")) + len("\nset -u\n")]
    head = head.replace("cd /workspace/repo || exit 1", f"cd {repo}").replace(
        "/etc/profile.d/", f"{tmp_path}/profile.d/"
    )
    (tmp_path / "profile.d").mkdir()
    for f in ("asv_utils.sh", "asv_build_vars.sh"):
        (tmp_path / "profile.d" / f).write_text("")
    body = text[text.index("# The tree the timing must see") : text.index("# Per-step timings")]
    body = body.replace("/tests/", f"{_TESTS}/").replace("/workspace/.fc_base", str(tmp_path / "base"))
    body = body.replace("/opt/fc_base_digest", str(tmp_path / "fc_base_digest"))
    results = {"results": {"exit_code": 0}, "regression": {"ran": True, "regressed": regressed or []}}
    calls = tmp_path / "calls"
    summary = f"echo '{json.dumps(snapshot)}' > {log}/summary_{agent}.json" if snapshot else ":"
    script = f"""micromamba() {{ :; }}
{head}
LOG_DIR={log} OWNER={owner} AGENT_KEY={agent} BENCHMARK_DIR={tmp_path} SUPABASE_URL= FC_REBUILD_CMD=
FC_BASE=$(git rev-parse HEAD)
patch_files={patch_files}
ts() {{ echo T; }}
mg_acquire() {{ echo "acquire $1" >> {calls}; }}
mg_release() {{ :; }}
snapshot-tool() {{ echo "snapshot $1" >> {calls}; {summary}; {snapshot_cmd or ":"}; }}
timeout() {{ shift 2; "$@"; }}
python() {{
  case "$1" in
    */timing_decision.py) command python3 "$@";;
    */pytest_runner.py) echo pytest >> {calls}; echo '{json.dumps(results)}' > {log}/test_results.json; {runner or ":"};;
    */lsv_measure.py) echo lsv_measure >> {calls};;
    *) command python3 "$@";;
  esac
}}
{body}
echo REACHED_REWARD
"""
    if base is not None:
        (tmp_path / "base").mkdir()
        (tmp_path / "base" / "core.py").write_text("x = 1\n")
        fn = (_TESTS / "base_digest.sh").read_text()
        digest = subprocess.run(
            ["bash", "-c", fn + "fc_base_digest"],
            capture_output=True,
            text=True,
            check=True,
            env={**os.environ, "FC_BASE_COPY": str(tmp_path / "base")},
        ).stdout.strip()
        (tmp_path / "fc_base_digest").write_text(digest + "\n")
        if base:
            (tmp_path / "base" / "core.py").write_text(base)
    clean = {k: v for k, v in os.environ.items() if not k.startswith(("FC_", "FORMULACODE_"))}
    clean["FC_BASE_COPY"] = str(tmp_path / "base")
    out = subprocess.run(
        ["bash", "-c", script], capture_output=True, text=True, timeout=60, env={**clean, **(env or {})}
    )
    assert "REACHED_REWARD" in out.stdout, out.stderr
    return calls.read_text().splitlines(), log


def _skipped(log: Path) -> dict | None:
    p = log / "lsv_skipped.json"
    return json.loads(p.read_text()) if p.exists() else None


def test_clean_patch_is_timed_after_the_correctness_steps(tmp_path: Path) -> None:
    calls, log = _run(tmp_path)
    assert calls.index("pytest") < calls.index("lsv_measure")
    assert "snapshot verify" in calls and _skipped(log) is None


def test_pytest_regression_skips_the_timing(tmp_path: Path) -> None:
    calls, log = _run(tmp_path, regressed=["t.py::a"])
    assert "lsv_measure" not in calls
    assert _skipped(log) == {"lsv_skipped": "pytest_regression"}


@pytest.mark.parametrize(
    "kwargs",
    [
        {"env": {"FC_TIMING_SHORT_CIRCUIT": "0"}},
        {"env": {"FC_ORACLE_FAILING_TESTS": '["t.py::a"]'}},
        {"agent": "oracle"},
        {"env": {"FC_SKIP_SNAPSHOT": "1"}},
    ],
)
def test_regression_is_timed_when_the_reward_stays_open_or_timing_is_asked(tmp_path: Path, kwargs: dict) -> None:
    calls, log = _run(tmp_path, regressed=["t.py::a"], **kwargs)
    assert "lsv_measure" in calls and _skipped(log) is None


def test_snapshot_regression_skips_the_timing_except_for_skipped_libraries(tmp_path: Path) -> None:
    snap = {"pass-to-fail": 1, "pass-to-pass": 3}
    calls, log = _run(tmp_path / "a", snapshot=snap)
    assert "lsv_measure" not in calls and _skipped(log) == {"lsv_skipped": "snapshots_regressed"}
    calls, log = _run(tmp_path / "b", snapshot=snap, owner="dask")
    assert "lsv_measure" in calls


HANG_WITH_PATCH = 'grep -q "x = 2" core.py && return 124; return 0'


def _status(log: Path) -> dict:
    return json.loads((log / "snapshot_status.json").read_text())


def test_snapshot_verify_over_its_limit_only_with_the_patch_fails(tmp_path: Path) -> None:
    calls, log = _run(tmp_path, snapshot_cmd=HANG_WITH_PATCH)
    assert calls.count("snapshot verify") == 2 and "lsv_measure" not in calls
    assert _status(log) == {
        "verify_exit_code": 124,
        "timed_out": True,
        "base_exit_code": 0,
        "base_also_timed_out": False,
    }
    assert _skipped(log) == {"lsv_skipped": "snapshots_regressed"}
    assert (tmp_path / "repo" / "core.py").read_text() == "x = 2\n" and (tmp_path / "repo" / "new.py").exists()


def test_snapshot_verify_over_its_limit_also_at_base_is_not_a_failure(tmp_path: Path) -> None:
    calls, log = _run(tmp_path, snapshot_cmd="return 124")
    assert calls.count("snapshot verify") == 2 and "lsv_measure" in calls
    assert _status(log)["base_also_timed_out"] is True and _skipped(log) is None


def test_finished_snapshot_verify_runs_once(tmp_path: Path) -> None:
    calls, log = _run(tmp_path)
    assert calls.count("snapshot verify") == 1 and _status(log)["timed_out"] is False


def test_empty_patch_skips_the_timing(tmp_path: Path) -> None:
    calls, log = _run(tmp_path, patch_files=0)
    assert "lsv_measure" not in calls and _skipped(log) == {"lsv_skipped": "no_patch"}


def test_edits_left_stashed_by_a_killed_runner_are_restored_and_timed(tmp_path: Path) -> None:
    calls, log = _run(tmp_path, runner="git stash push -q -m fc-regression; return 137")
    assert "lsv_measure" in calls and _skipped(log) is None
    assert (tmp_path / "repo" / "core.py").read_text() == "x = 2\n"


def test_tree_changed_by_the_correctness_steps_is_not_timed(tmp_path: Path) -> None:
    calls, log = _run(tmp_path, runner="echo 'x = 3' > core.py")
    assert "lsv_measure" not in calls
    assert _skipped(log) == {"lsv_skipped": "tree_not_restored", "error": "tree_not_restored"}


def test_changed_base_copy_is_not_timed(tmp_path: Path) -> None:
    calls, _ = _run(tmp_path / "a", base="")
    assert "lsv_measure" in calls
    calls, log = _run(tmp_path / "b", base="import time\ntime.sleep(1)\nx = 1\n")
    assert "lsv_measure" not in calls
    assert _skipped(log) == {"lsv_skipped": "base_changed", "error": "base_changed"}


def test_digest_from_the_host_wins_over_the_container_file(tmp_path: Path) -> None:
    calls, log = _run(tmp_path, base="", env={"FC_BASE_DIGEST": "0" * 64})
    assert "lsv_measure" not in calls and _skipped(log)["lsv_skipped"] == "base_changed"


def test_prepare_and_test_hash_the_base_copy_with_the_same_helper() -> None:
    setup, test = ((_TESTS / f).read_text() for f in ("prepare.sh", "test.sh"))
    assert "source /tests/base_digest.sh" in setup and "source /tests/base_digest.sh" in test


def test_decision_matches_the_host_rules(tmp_path: Path) -> None:
    m = _load("timing_decision")
    assert m.pytest_regressed({"regression": {"regressed": ["a", "b"]}}, ["a"])
    assert not m.pytest_regressed({"regression": {"regressed": ["a"]}}, ["a"])
    assert m.pytest_regressed({"regression": {"n_regressed": 2}})
    assert not m.pytest_regressed(None)
    (tmp_path / "summary_x.json").write_text(json.dumps({"pass-to-pass": 4}))
    assert not m.snapshot_regressed(tmp_path, "o", {"dask"})
    (tmp_path / "summary_y.json").write_text(json.dumps({"skip-to-fail": 1}))
    assert m.snapshot_regressed(tmp_path, "o", {"dask"}) and not m.snapshot_regressed(tmp_path, "dask", {"dask"})


def _parser(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, skip: dict) -> dict:
    mod = _load("parser")
    log = tmp_path / "artifacts"
    (log / "lsv").mkdir(parents=True)
    (log / "lsv" / "lsv_results.json").write_text(json.dumps({"init": {"baseline_sha": "s"}}))
    (log / "lsv_skipped.json").write_text(json.dumps(skip))
    (log / "test_results.json").write_text(json.dumps({"results": {"exit_code": 0}}))
    monkeypatch.setattr(mod, "LOG_DIR", log)
    monkeypatch.setattr(mod, "LSV_DIR", log / "lsv")
    monkeypatch.setattr(mod, "REWARD_DIR", tmp_path / "verifier")
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.setattr(sys, "argv", ["parser.py", "--owner", "o", "--repo", "r", "--issue-number", "1"])
    mod.main()
    return json.loads((tmp_path / "verifier" / "reward.json").read_text())


def test_parser_records_why_the_timing_was_skipped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reward = _parser(tmp_path / "a", monkeypatch, {"lsv_skipped": "pytest_regression"})
    assert reward["lsv_skipped"] == "pytest_regression" and reward["lsv_error"] is None
    reward = _parser(tmp_path / "b", monkeypatch, {"lsv_skipped": "base_changed", "error": "base_changed"})
    assert reward["lsv_error"] == "base_changed"
