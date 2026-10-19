import csv
import json
import math
from pathlib import Path

import pytest

RULE = {"form": "per_benchmark", "k": 2, "n0": 10, "theta_min": 0.0296, "theta_max": 0.3, "bias_check": "aggregate"}
FIELDS = ["task", "decision", "reviewer", "date", "data", "note"]


def run(role: str, n: int, logs: dict[str, float]) -> dict:
    return {"role": role, "run": n, "log_speedups": logs, "baseline_reused": True, "tests": {}}


def record(task: str, oracle: list[dict[str, float]], nop: list[dict[str, float]]) -> dict:
    runs = [run("oracle_baseline", i + 1, x) for i, x in enumerate(oracle)]
    runs += [run("nop", i + 1, x) for i, x in enumerate(nop)]
    return {"task": task, "n_oracle": len(oracle), "n_nop": len(nop), "runs": runs}


@pytest.fixture
def survey(tmp_path: Path) -> Path:
    """Three tasks: one with a clear gain, one measured with no gain, one uncovered; all pending."""
    for d in ("state", "oracle", "pinned"):
        (tmp_path / d).mkdir()
    (tmp_path / "threshold_rule.json").write_text(json.dumps(RULE))
    quiet = [
        {"a.time_x": 0.001, "a.time_y": -0.002},
        {"a.time_x": -0.001, "a.time_y": 0.0},
        {"a.time_x": 0.0, "a.time_y": 0.001},
    ]
    fast = record("o__r__1", [{"a.time_x": math.log(1.5), "a.time_y": 0.0}] * 3, quiet)
    flat = record("o__r__2", [{"a.time_x": 0.001, "a.time_y": -0.001}] * 3, quiet)
    for rec in (fast, flat):
        (tmp_path / "oracle" / f"{rec['task']}.json").write_text(json.dumps(rec))
    for task, phase in (("o__r__1", "measured"), ("o__r__2", "measured"), ("o__r__3", "uncovered")):
        (tmp_path / "state" / f"{task}.json").write_text(json.dumps({"task": task, "phase": phase}))
    td = tmp_path / "pinned" / "o__r__1"
    (td / "solution").mkdir(parents=True)
    (td / "solution" / "solve.sh").write_text(
        "cat > p.diff << '__SOLUTION__'\n--- a/pkg/a/core.py\n+++ b/pkg/a/core.py\n@@ -1 +1 @@\n-x\n+y\n__SOLUTION__\n"
    )
    (td / "instruction.md").write_text("**Task Description**\n\nThe code is slow.\n\n## Task Requirements\n- x\n")
    with (tmp_path / "manual_review.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for t in ("o/r#1", "o/r#2", "o/r#3"):
            w.writerow({"task": t, "decision": "pending"})
    return tmp_path
