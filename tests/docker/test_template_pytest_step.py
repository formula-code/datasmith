"""A killed or timed-out pytest runner must not stop test.sh before the reward step.

bluesky/tiled#1283 (2026-09-30): the runner was killed after its tests passed, test.sh exited with its status, no
reward file was written, and all six trials were discarded although their timing data was complete.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

_TEST_SH = Path(__file__).parents[2] / "src" / "datasmith" / "harbor_adapter" / "template" / "tests" / "test.sh"


def _pytest_block() -> str:
    text = _TEST_SH.read_text()
    start = text.index('echo "[$(ts)] [test] Running pytest..."')
    end = text.index("# Per-step timings")
    block = re.sub(r"\{%-? else %\}.*?\{%-? endif %\}", "", text[start:end], flags=re.DOTALL)
    return re.sub(r"\{\{[^}]*\}\}", "BASE", block)


def _run(tmp_path: Path, runner: str) -> subprocess.CompletedProcess:
    (tmp_path / "test_results.json").write_text("{}")
    script = f"""set -euo pipefail
LOG_DIR={tmp_path}
FC_BASE=BASE
ts() {{ date +%s; }}
mg_release() {{ :; }}
python() {{ {runner}; }}
timeout() {{ shift 2; "$@"; }}
{_pytest_block()}
echo REACHED_REWARD
"""
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=30)


def test_killed_runner_still_reaches_reward(tmp_path: Path) -> None:
    out = _run(tmp_path, "return 137")
    assert "REACHED_REWARD" in out.stdout, out.stderr
    assert "continuing without test results" in out.stderr
    assert not (tmp_path / "test_results.json").exists()


def test_passing_runner_keeps_results(tmp_path: Path) -> None:
    out = _run(tmp_path, "return 0")
    assert "REACHED_REWARD" in out.stdout, out.stderr
    assert (tmp_path / "test_results.json").exists()
