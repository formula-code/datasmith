"""test.sh skips the timing of an empty patch and of a patch with a pytest regression; pytest runs before LSV."""

from __future__ import annotations

import importlib.util
import json
import subprocess
from pathlib import Path

import pytest

from datasmith.harbor_adapter.utils import render_template


def _run(tmp_path: Path, patch_files: int, regressed: int) -> tuple[subprocess.CompletedProcess, list[str]]:
    """Run test.sh from the empty-patch check to the end, with the gate, the runners and parser.py stubbed."""
    text = render_template(
        "tests/test.sh", base_commit="BASE", run_pytest=True, rounds=None, task_id="t", owner="o", repo="r", issue_number=1
    )
    report = text[text.index('FC_TIMING_SKIPPED=""') : text.index("test_start=$(date +%s)")]
    body = text[text.index("# ── Empty patch") :]
    results = json.dumps({"results": {"exit_code": 0}, "regression": {"ran": True, "n_regressed": regressed}})
    log = tmp_path / "calls.txt"
    script = f"""set -euo pipefail
LOG_DIR={tmp_path} OWNER=o REPO=r ISSUE_NUMBER=1 FC_BASE=BASE AGENT_KEY=qwen-coder BENCHMARK_DIR=
patch_files={patch_files}
test_start=$(date +%s)
ts() {{ date +%s; }}
fc_diff() {{ :; }}
mg_acquire() {{ echo "acquire $1" >> {log}; }}
mg_release() {{ echo "release" >> {log}; }}
timeout() {{ shift 2; "$@"; }}
python() {{
  case "$1" in -) command python3 "$@"; return;; esac
  echo "python $*" >> {log}
  case "$*" in *pytest_runner*) echo '{results}' > {tmp_path}/test_results.json;; esac
}}
{report}
{body}
"""
    out = subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=60)
    return out, (log.read_text().splitlines() if log.exists() else [])


def test_empty_patch_takes_no_gate_slot_and_runs_nothing(tmp_path):
    out, calls = _run(tmp_path, patch_files=0, regressed=0)
    assert out.returncode == 0, out.stderr
    assert not any(c.startswith("acquire") or "lsv_measure" in c or "pytest_runner" in c for c in calls)
    assert any("parser.py" in c and "--timing-skipped no_patch" in c for c in calls)


def test_pytest_regression_skips_lsv(tmp_path):
    out, calls = _run(tmp_path, patch_files=2, regressed=1)
    assert out.returncode == 0, out.stderr
    assert "acquire measure" not in calls and not any("lsv_measure" in c for c in calls)
    assert any("parser.py" in c and "--timing-skipped pytest_regression" in c for c in calls)
    assert "lsv_measure_s" not in json.loads((tmp_path / "test_timings.json").read_text())


def test_clean_pytest_runs_lsv_after_pytest(tmp_path):
    out, calls = _run(tmp_path, patch_files=2, regressed=0)
    assert out.returncode == 0, out.stderr
    pytest_at = next(i for i, c in enumerate(calls) if "pytest_runner" in c)
    assert calls.index("acquire tests") < pytest_at < calls.index("acquire measure")
    assert any("lsv_measure" in c for c in calls[pytest_at:])
    assert not any("--timing-skipped" in c for c in calls)
    assert "lsv_measure_s" in json.loads((tmp_path / "test_timings.json").read_text())


def test_parser_writes_reward_without_lsv_results_when_timing_was_skipped(tmp_path, monkeypatch):
    path = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests" / "parser.py"
    spec = importlib.util.spec_from_file_location("fc_parser_order_test", path)
    parser = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(parser)
    (tmp_path / "patch_info.json").write_text(json.dumps({"applied": False, "files": 0}))
    monkeypatch.setattr(parser, "LOG_DIR", tmp_path)
    monkeypatch.setattr(parser, "LSV_DIR", tmp_path / "lsv")
    monkeypatch.setattr(parser, "REWARD_DIR", tmp_path / "verifier")
    argv = ["parser.py", "--owner", "o", "--repo", "r", "--issue-number", "1", "--agent-key", "qwen-coder"]
    monkeypatch.setattr("sys.argv", [*argv, "--timing-skipped", "no_patch"])
    with pytest.raises(SystemExit) as exc:
        parser.main()
    assert exc.value.code == 0
    reward = json.loads((tmp_path / "verifier" / "reward.json").read_text())
    assert reward["timing_skipped"] == "no_patch" and reward["patch"]["applied"] is False
    assert reward["tests_passed"] is None and float((tmp_path / "verifier" / "reward.txt").read_text()) < 0
