"""test.sh skips the timing of a patch that the host tamper gate rejects anyway (tamper_precheck.py)."""

from __future__ import annotations

import importlib.util
import json
import subprocess
from pathlib import Path

import pytest

_TESTS = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(f"fc_{name}_test", _TESTS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _diff(path: str, *lines: str) -> str:
    body = "\n".join(lines)
    return f"diff --git a/{path} b/{path}\n--- a/{path}\n+++ b/{path}\n@@ -1,1 +1,1 @@\n{body}\n"


CONFTEST = _diff("tests/conftest.py", "-def fixture(): return 1", "+def fixture(): return 2")
SNAPSHOT = _diff("pkg/.snapshots/baseline.json", '-{"a": 1}', '+{"a": 2}')
BENCH = _diff("benchmarks/bench_io.py", "-N = 1000", "+N = 10")
ASV_CONF = _diff("asv.conf.json", '-"repo": ".",', '+"repo": "..",')
COMMENT_ONLY = _diff("tests/conftest.py", "-# old note", "+# new note", "+")
LIBRARY = _diff("pkg/core.py", "-x = slow()", "+x = fast()")


@pytest.mark.parametrize(
    ("diff", "scoped", "flagged"),
    [
        (CONFTEST, False, ["tests/conftest.py"]),
        (CONFTEST, True, ["tests/conftest.py"]),
        (SNAPSHOT, False, ["pkg/.snapshots/baseline.json"]),
        (SNAPSHOT, True, ["pkg/.snapshots/baseline.json"]),
        (BENCH, False, ["benchmarks/bench_io.py"]),
        (BENCH, True, []),
        (ASV_CONF, False, ["asv.conf.json"]),
        (ASV_CONF, True, []),
        (COMMENT_ONLY, False, []),
        (LIBRARY, False, []),
        (LIBRARY + BENCH + CONFTEST, True, ["tests/conftest.py"]),
    ],
)
def test_precheck_rule(tmp_path, monkeypatch, diff, scoped, flagged):
    mod = _load("tamper_precheck")
    monkeypatch.setenv("FC_TAMPER_SCOPED", "1" if scoped else "0")
    (tmp_path / "patch.diff").write_text(diff)
    rc = mod.main(["--log-dir", str(tmp_path)])
    record = json.loads((tmp_path / "tamper_precheck.json").read_text())
    assert record == {"files": flagged, "rule": "scoped" if scoped else "broad"}
    assert rc == (mod.FIRED if flagged else 0)


def test_precheck_does_not_skip_on_its_own_error(tmp_path, monkeypatch):
    monkeypatch.delenv("FC_TAMPER_SCOPED", raising=False)
    assert _load("tamper_precheck").main(["--log-dir", str(tmp_path / "missing")]) == 0


def _test_sh_steps(agent_key: str, precheck_rc: int, tmp_path: Path) -> tuple[subprocess.CompletedProcess, list[str]]:
    """Run test.sh from the report function to the rebuild step with python stubbed."""
    text = (_TESTS / "test.sh").read_text()
    report = text[text.index('FC_TIMING_SKIPPED=""') : text.index("test_start=$(date +%s)")]
    precheck = text[text.index("# ── Tamper precheck") : text.index("# ── Rebuild compiled")]
    log = tmp_path / "calls.txt"
    script = f"""set -euo pipefail
LOG_DIR={tmp_path} OWNER=o REPO=r ISSUE_NUMBER=1 FC_BASE=BASE AGENT_KEY={agent_key}
test_start=$(date +%s)
ts() {{ date +%s; }}
python() {{ echo "python $*" >> {log}; case "$*" in *tamper_precheck*) return {precheck_rc};; esac; }}
{report}
{precheck}
echo REACHED_REBUILD
"""
    out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=30)
    return out, (log.read_text().splitlines() if log.exists() else [])


def test_fired_precheck_skips_timing_and_still_writes_reward(tmp_path):
    out, calls = _test_sh_steps("qwen-coder", 3, tmp_path)
    assert out.returncode == 0, out.stderr
    assert "REACHED_REBUILD" not in out.stdout
    assert any("parser.py" in c and "--timing-skipped tamper" in c for c in calls)
    assert "test_total_s" in json.loads((tmp_path / "test_timings.json").read_text())


@pytest.mark.parametrize(("agent_key", "precheck_rc"), [("qwen-coder", 0), ("qwen-coder", 1), ("oracle", 3)])
def test_timing_runs_when_precheck_is_clean_failed_or_oracle(tmp_path, agent_key, precheck_rc):
    out, calls = _test_sh_steps(agent_key, precheck_rc, tmp_path)
    assert "REACHED_REBUILD" in out.stdout, out.stderr
    assert not any("parser.py" in c for c in calls)
    assert any("tamper_precheck" in c for c in calls) == (agent_key != "oracle")


def test_parser_records_skipped_timing(tmp_path, monkeypatch):
    parser = _load("parser")
    (tmp_path / "tamper_precheck.json").write_text(json.dumps({"files": ["tests/conftest.py"], "rule": "broad"}))
    monkeypatch.setattr(parser, "LOG_DIR", tmp_path)
    monkeypatch.setattr(parser, "LSV_DIR", tmp_path / "lsv")
    monkeypatch.setattr(parser, "REWARD_DIR", tmp_path / "verifier")
    argv = ["parser.py", "--owner", "o", "--repo", "r", "--issue-number", "1", "--agent-key", "qwen-coder"]
    monkeypatch.setattr("sys.argv", [*argv, "--timing-skipped", "tamper"])
    with pytest.raises(SystemExit) as exc:
        parser.main()
    assert exc.value.code == 0
    reward = json.loads((tmp_path / "verifier" / "reward.json").read_text())
    assert reward["timing_skipped"] == "tamper"
    assert reward["tamper_precheck"] == {"files": ["tests/conftest.py"], "rule": "broad"}
    assert reward["lsv_error"] is None and reward["per_benchmark_speedups"] == {}
    monkeypatch.setattr("sys.argv", argv)
    with pytest.raises(SystemExit) as exc:
        parser.main()
    assert exc.value.code == 1
