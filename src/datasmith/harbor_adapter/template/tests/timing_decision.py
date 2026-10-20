"""Whether the correctness results already decide the reward, so test.sh can skip the timing.
Same rules as fcrl.grading gates.pytest_regression and gates.snapshot_verdict; runs on Python 3.8."""

import json
import sys
from pathlib import Path


def pytest_regressed(test_results, oracle_failing=()):
    reg = (test_results or {}).get("regression") or {}
    if "regressed" not in reg:
        return int(reg.get("n_regressed") or 0) > 0
    return bool(set(reg["regressed"]) - set(oracle_failing or ()))


def snapshot_regressed(log_dir, owner, skip_libs):
    if owner in skip_libs:
        return False
    compared = regressions = 0
    for p in Path(log_dir).glob("summary_*.json"):
        try:
            s = json.loads(p.read_text())
        except Exception:  # noqa: BLE001
            continue
        compared += sum(v for k, v in s.items() if isinstance(v, int) and "-to-" in str(k))
        regressions += int(s.get("pass-to-fail", 0) or 0) + int(s.get("skip-to-fail", 0) or 0)
    return compared > 0 and regressions > 0


def decide(log_dir, patch_files, owner, oracle_failing=(), skip_libs=("dask",)):
    """"no_patch", "pytest_regression", "snapshots_regressed" or None (time the patch)."""
    if int(patch_files) == 0:
        return "no_patch"
    try:
        results = json.loads((Path(log_dir) / "test_results.json").read_text())
    except Exception:  # noqa: BLE001
        results = None
    if pytest_regressed(results, oracle_failing):
        return "pytest_regression"
    if snapshot_regressed(log_dir, owner, set(skip_libs)):
        return "snapshots_regressed"
    return None


if __name__ == "__main__":
    log_dir, patch_files, owner, failing_json, skip_libs = sys.argv[1:6]
    try:
        failing = json.loads(failing_json or "[]")
    except ValueError:
        failing = []
    reason = decide(log_dir, patch_files, owner, failing, [x for x in skip_libs.split(",") if x])
    print(reason or "")
