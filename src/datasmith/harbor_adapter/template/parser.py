"""Reward computation + harbor_runs persistence.

Reads lsv_results.json (produced by lsv_measure.py), computes per-benchmark
speedups (baseline/current from LSV), fetches oracle benchmark timings from
Supabase to compute advantage ((oracle_time - agent_time) / agent_time),
writes reward.json + reward.txt, and then performs all post-trial Supabase
persistence end-to-end:

  * Classifies trial status (setup/patch/lsv/tests) from the reward dossier.
  * Inserts a ``harbor_runs`` row keyed on a sandbox-generated ``run_id``.
  * Uploads ``/logs/verifier/run_artifacts.tar.gz`` to the ``runs`` bucket
    and PATCHes ``harbor_runs.artifacts_storage_url`` with the object key.
  * For oracle daytona success: uploads ``oracle_snapshots.tar.gz`` to the
    ``snapshots`` bucket and PATCHes ``pull_requests`` with the new
    ``baseline_run_id`` + ``snapshot_storage_url``.
  * Writes ``/logs/verifier/run_id.txt`` last as a sentinel so the runner's
    failure-path sweep can tell which trials it must NOT re-write rows for.

Was previously a runner-side responsibility (datasmith.runners.harbor_healthcheck)
— moved here so ``harbor trials start`` and SkyRL get parity with ``fc-data``
without going through the datasmith runner.

Usage:
    python /tests/parser.py --owner OWNER --repo REPO --issue-number N [--agent-key KEY]
"""

from __future__ import annotations

import argparse
import glob as glob_mod
import json
import math
import os
import sys
import urllib.error
import urllib.request
import uuid
from pathlib import Path

LOG_DIR = Path(os.environ.get("T_BENCH_TASK_LOGS_PATH", "/logs/artifacts"))
LSV_DIR = LOG_DIR / "lsv"
REWARD_DIR = Path("/logs/verifier")


# ── LSV results ──────────────────────────────────────────────────────────────


def load_lsv_results(lsv_dir: Path) -> dict:
    """Read lsv_results.json."""
    path = lsv_dir / "lsv_results.json"
    if not path.exists():
        print(f"WARNING: {path} not found")
        return {}
    return json.loads(path.read_text())


def compute_per_benchmark_speedups(benchmarks: dict) -> dict[str, float]:
    """Compute speedup = baseline / current for each benchmark.

    Values >1 mean the agent made it faster.
    Filters out benchmarks with None/zero baseline or current.
    """
    speedups = {}
    for name, data in benchmarks.items():
        baseline = data.get("baseline")
        current = data.get("current")
        if baseline is None or current is None:
            continue
        if current <= 0 or baseline <= 0:
            continue
        speedups[name] = baseline / current
    return speedups


# ── Oracle advantage via Supabase ────────────────────────────────────────────


def _supabase_get(url: str, headers: dict) -> dict | list | None:
    """GET request to Supabase REST API. Returns parsed JSON or None."""
    req = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError) as e:
        print(f"WARNING: Supabase request failed: {e}")
        return None


def _datasmith_headers() -> dict[str, str] | None:
    """Auth headers for datasmith's Supabase. Mirrors lsv_cache_writeback.py.

    Returns None if the service key is missing — caller should treat that as
    "skip oracle advantage computation" rather than fail the trial.
    """
    key = os.environ.get("DATASMITH_SUPABASE_SERVICE_KEY", "")
    if not key:
        return None
    headers = {
        "Authorization": f"Bearer {key}",
        "apikey": key,
        "Accept": "application/json",
        "User-Agent": "datasmith-parser/1.0",
    }
    cf_id = os.environ.get("DATASMITH_CF_ACCESS_CLIENT_ID", "")
    cf_secret = os.environ.get("DATASMITH_CF_ACCESS_CLIENT_SECRET", "")
    if cf_id and cf_secret:
        headers["CF-Access-Client-Id"] = cf_id
        headers["CF-Access-Client-Secret"] = cf_secret
    return headers



def _parse_task_id(task_id: str) -> tuple[str, str, int] | None:
    """``owner_repo_<issue>`` — same shape as ``records.py`` task_id minting.

    Splits from the right so repo names with underscores still parse.
    Returns None if the trailing component isn't an integer.
    """
    try:
        *rest, issue_str = task_id.rsplit("_", 1)
        issue = int(issue_str)
        head = "_".join(rest)
        owner, _, repo = head.partition("_")
        if not owner or not repo:
            return None
        return owner, repo, issue
    except (ValueError, IndexError):
        return None


def fetch_oracle_benchmarks(base_url: str, task_id: str) -> dict[str, float] | None:
    """Fetch the oracle's per-benchmark timings from datasmith Supabase.

    1. Parse task_id → (owner, repo, issue_number).
    2. GET pull_requests?select=baseline_run_id&owner=eq.X&repo=eq.Y&issue_number=eq.Z
       to find the promoted oracle run.
    3. GET harbor_runs?select=reward_payload&run_id=eq.<baseline_run_id>
       to pull the oracle's reward_payload.
    4. Extract reward_payload.lsv_results.measure.benchmarks → {name: current_seconds}.

    Returns None if any step lacks data — caller logs and proceeds without
    advantage computation.
    """
    headers = _datasmith_headers()
    if not headers or not base_url:
        return None

    parsed = _parse_task_id(task_id)
    if parsed is None:
        print(f"[parser] Could not parse task_id {task_id!r} as owner_repo_issue")
        return None
    owner, repo, issue_number = parsed

    pr_url = (
        f"{base_url}/rest/v1/pull_requests"
        f"?select=baseline_run_id"
        f"&owner=eq.{owner}&repo=eq.{repo}&issue_number=eq.{issue_number}"
    )
    pr_rows = _supabase_get(pr_url, headers)
    if not pr_rows or not isinstance(pr_rows, list) or len(pr_rows) == 0:
        print(f"[parser] No pull_requests row for {owner}/{repo}#{issue_number}")
        return None

    baseline_run_id = pr_rows[0].get("baseline_run_id")
    if not baseline_run_id:
        print(f"[parser] No baseline_run_id set for {owner}/{repo}#{issue_number}")
        return None

    runs_url = (
        f"{base_url}/rest/v1/harbor_runs"
        f"?select=reward_payload&run_id=eq.{baseline_run_id}"
    )
    runs = _supabase_get(runs_url, headers)
    if not runs or not isinstance(runs, list) or len(runs) == 0:
        print(f"[parser] harbor_runs row {baseline_run_id} not found")
        return None

    payload = runs[0].get("reward_payload") or {}
    if isinstance(payload, str):
        payload = json.loads(payload)

    oracle_benchmarks_raw = (
        payload.get("lsv_results", {}).get("measure", {}).get("benchmarks", {})
    )
    if not oracle_benchmarks_raw:
        print("[parser] oracle harbor_runs row has no benchmark data in reward_payload")
        return None

    oracle_times = {}
    for name, data in oracle_benchmarks_raw.items():
        current = data.get("current")
        if current is not None and current > 0:
            oracle_times[name] = current

    print(f"[parser] Fetched {len(oracle_times)} oracle benchmark timings")
    return oracle_times


def compute_per_benchmark_advantages(
    agent_benchmarks: dict,
    oracle_benchmarks: dict[str, float],
) -> dict[str, float]:
    """Compute advantage = (oracle_time - agent_time) / agent_time.

    Values >0 mean the agent is faster than oracle.
    Only computes for benchmarks present in both sets.
    """
    advantages = {}
    for name, data in agent_benchmarks.items():
        agent_time = data.get("current")
        oracle_time = oracle_benchmarks.get(name)
        if agent_time is None or oracle_time is None:
            continue
        if agent_time <= 0:
            continue
        advantages[name] = (oracle_time - agent_time) / agent_time
    return advantages


# ── Aggregation ──────────────────────────────────────────────────────────────


def geometric_mean(values: list[float]) -> float:
    """Geometric mean, handling edge cases."""
    if not values:
        return 0.0
    # For speedup ratios (always positive), use direct geometric mean
    # For advantage values (can be negative), use arithmetic mean instead
    if any(v <= 0 for v in values):
        return sum(values) / len(values)
    log_sum = sum(math.log(v) for v in values)
    return math.exp(log_sum / len(values))


def aggregate_by_hierarchy(per_benchmark: dict[str, float]) -> dict:
    """Group benchmarks by ASV naming hierarchy and aggregate.

    Benchmark names follow: module.Class.method or module.method
    Level 1: per-function (identity — each benchmark)
    Level 2: per-class (group by 'module.Class')
    Level 3: per-module (group by 'module')
    Level 4: overall
    """
    if not per_benchmark:
        return {"level1": {}, "level2": {}, "level3": {}, "level4": 0.0}

    # Level 1: identity
    level1 = dict(per_benchmark)

    # Level 2: group by class (first two dotted segments)
    class_groups: dict[str, list[float]] = {}
    for name, val in per_benchmark.items():
        parts = name.split(".")
        class_key = ".".join(parts[:2]) if len(parts) >= 2 else name
        class_groups.setdefault(class_key, []).append(val)
    level2 = {k: geometric_mean(v) for k, v in class_groups.items()}

    # Level 3: group by module (first dotted segment)
    module_groups: dict[str, list[float]] = {}
    for name, val in per_benchmark.items():
        module_key = name.split(".")[0] if "." in name else name
        module_groups.setdefault(module_key, []).append(val)
    level3 = {k: geometric_mean(v) for k, v in module_groups.items()}

    # Level 4: overall
    all_values = list(per_benchmark.values())
    level4 = geometric_mean(all_values)

    return {"level1": level1, "level2": level2, "level3": level3, "level4": level4}


# ── Test and snapshot results ────────────────────────────────────────────────


def load_test_results(log_dir: Path) -> tuple[bool, float, dict]:
    """Read test_results.json. Returns (tests_passed, success_ratio, raw).

    Missing file returns ``(False, 0.0, {})`` so the caller can distinguish
    'no pytest data' from 'pytest passed'. This is the inversion of the
    previous behavior, which defaulted to ``True`` and silently masked
    every failure mode upstream of pytest.
    """
    path = log_dir / "test_results.json"
    if not path.exists():
        return False, 0.0, {}

    raw = json.loads(path.read_text())
    results = raw.get("results", raw)
    summary = results.get("summary", {})
    exit_code = results.get("exit_code", 0)

    failed = summary.get("failed", 0) + summary.get("error", 0)
    total_tests = sum(
        summary.get(k, 0) for k in ("passed", "failed", "error", "skipped")
    )

    tests_passed = exit_code == 0 and failed == 0
    success_ratio = (total_tests - failed) / total_tests if total_tests > 0 else 1.0

    return tests_passed, success_ratio, raw


def summarize_pytest(raw: dict) -> dict:
    """Extract structured pytest counters from test_results.json for the
    reward dossier. Returns an empty dict if there's no usable content."""
    if not raw:
        return {}
    results = raw.get("results", raw) or {}
    summary = results.get("summary", {}) or {}
    tests = results.get("tests", []) or []
    return {
        "exit_code": results.get("exit_code"),
        "total": summary.get("total", 0),
        "passed": summary.get("passed", 0),
        "failed": summary.get("failed", 0),
        "skipped": summary.get("skipped", 0),
        "error": summary.get("error", 0),
        "duration_s": results.get("duration"),
        "selected_nodeids": [t.get("nodeid") for t in tests if t.get("nodeid")],
        "strategy": raw.get("strategy"),
        "note": raw.get("note"),
    }


def summarize_snapshots(log_dir: Path) -> dict:
    """Collect snapshot verify summaries into a single structured block."""
    out: dict = {"summaries": {}, "passed": True}
    for match_path in glob_mod.glob(str(log_dir / "summary_*.json")):
        name = Path(match_path).stem.removeprefix("summary_")
        try:
            data = json.loads(Path(match_path).read_text())
        except (json.JSONDecodeError, OSError):
            continue
        out["summaries"][name] = data
        if data.get("passed") is False:
            out["passed"] = False
    return out


def load_patch_info(log_dir: Path) -> dict:
    path = log_dir / "patch_info.json"
    if not path.exists():
        return {"applied": False, "files": 0, "added_lines": 0, "removed_lines": 0}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {"applied": False, "files": 0, "added_lines": 0, "removed_lines": 0}


def load_setup_status(log_dir: Path) -> dict:
    """Read setup_status.json written by setup.sh's EXIT trap.

    Missing file means setup.sh never ran the trap (earlier crash, or an
    old image without the trap baked in). Return a conservative default:
    exit_code=None so downstream code can treat it as 'unknown' rather
    than 'succeeded'.
    """
    path = log_dir / "setup_status.json"
    if not path.exists():
        return {"exit_code": None, "failed_phase": None, "succeeded": None}
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {"exit_code": None, "failed_phase": None, "succeeded": None}


def load_timings(log_dir: Path) -> dict:
    """Merge setup_timings.json (from setup.sh) and test_timings.json
    (from test.sh). Missing files contribute no keys."""
    merged: dict = {}
    for name in ("setup_timings.json", "test_timings.json"):
        path = log_dir / name
        if not path.exists():
            continue
        try:
            merged.update(json.loads(path.read_text()))
        except (json.JSONDecodeError, OSError):
            continue
    return merged


def summarize_lsv_init(lsv_results: dict) -> dict:
    init = (lsv_results or {}).get("init", {}) or {}
    if not init:
        return {}
    return {
        "benchmarks_discovered": init.get("benchmarks_discovered") or [],
        "benchmarks_impactable": init.get("benchmarks_impactable") or [],
        "source_files_covered": init.get("source_files_covered"),
        "init_time_s": (init.get("timing") or {}).get("total_s"),
    }


def log_snapshot_results(log_dir: Path) -> None:
    """Log snapshot verification results. Regressions zero out reward.txt for agent runs."""
    regressions = []
    for match_path in glob_mod.glob(str(log_dir / "summary_*.json")):
        try:
            data = json.loads(Path(match_path).read_text())
            if data.get("passed") is False:
                regressions.append(Path(match_path).name)
        except (json.JSONDecodeError, OSError):
            continue
    if regressions:
        print(f"[parser] Snapshot regressions detected (reward zeroed for agent runs): {regressions}")
    else:
        print("[parser] Snapshot verification passed (or not run).")


# ── Reward output ────────────────────────────────────────────────────────────


def write_reward(
    reward_dir: Path,
    advantages: dict[str, float] | None,
    advantage_levels: dict | None,
    speedups: dict[str, float],
    speedup_levels: dict,
    tests_passed: bool,
    snapshots: dict,
    lsv_error: str | None = None,
    *,
    patch: dict | None = None,
    lsv_init_summary: dict | None = None,
    lsv_measure_raw: dict | None = None,
    pytest_summary: dict | None = None,
    timings: dict | None = None,
    setup_status: dict | None = None,
    is_oracle: bool = False,
) -> dict:
    """Write reward.json + reward.txt with a structured dossier.

    The top-level gating fields (``num_valid_benchmarks``, ``max_speedup``,
    ``lsv_mean_speedup``, ``tests_passed``, ``snapshots_passed``,
    ``lsv_error``) are preserved for back-compat with the publish filter
    and harbor_healthcheck row builder. Everything else lives in nested
    sub-blocks so post-mortem triage has the full picture without
    round-tripping through the trial container."""
    reward_dir.mkdir(parents=True, exist_ok=True)

    max_speedup = max(speedups.values()) if speedups else None
    snapshots_passed = bool((snapshots or {}).get("passed", True))

    reward_data = {
        # ── legacy scalar fields (publish filter, old downstream tools) ──
        "agent_advantage": (advantage_levels["level4"] if advantage_levels else None),
        "agent_advantage_level1": (
            advantage_levels["level1"] if advantage_levels else None
        ),
        "agent_advantage_level2": (
            advantage_levels["level2"] if advantage_levels else None
        ),
        "agent_advantage_level3": (
            advantage_levels["level3"] if advantage_levels else None
        ),
        "agent_advantage_level4": (
            advantage_levels["level4"] if advantage_levels else None
        ),
        "num_valid_benchmarks": len(speedups),
        "per_benchmark_speedups": speedups,
        "lsv_mean_speedup": speedup_levels["level4"],
        "max_speedup": max_speedup,
        "tests_passed": tests_passed,
        "snapshots_passed": snapshots_passed,
        "lsv_error": lsv_error,
        # ── structured dossier ────────────────────────────────────────────
        "patch": patch or {},
        "lsv": {
            "init": lsv_init_summary or {},
            "measure": {
                "selected_count": (lsv_measure_raw or {}).get("selected_count"),
                "total_count": (lsv_measure_raw or {}).get("total_count"),
                "skipped_count": (lsv_measure_raw or {}).get("skipped_count"),
                "time_s": ((lsv_measure_raw or {}).get("timing") or {}).get("total_s"),
                "measured_count": len(speedups),
                "error": lsv_error,
            },
        },
        "pytest": pytest_summary or {},
        "snapshots": snapshots or {},
        "timings": timings or {},
        "setup": setup_status or {},
    }

    (reward_dir / "reward.json").write_text(json.dumps(reward_data, indent=2))

    # reward.txt — LSV geomean speedup, zeroed out on correctness regressions.
    # Oracle runs are exempt: they capture baselines and aren't being graded.
    speedup_value = speedup_levels["level4"] if speedup_levels else 0.0
    if not is_oracle and (not tests_passed or not snapshots_passed):
        speedup_value = 0.0
    (reward_dir / "reward.txt").write_text(str(speedup_value))

    print(f"[parser] reward.txt = {speedup_value}")
    print(f"[parser] max_speedup = {max_speedup}")
    print(f"[parser] num_valid_benchmarks = {len(speedups)}")
    print(f"[parser] tests_passed = {tests_passed}")
    print(f"[parser] patch_applied = {(patch or {}).get('applied')}")
    return reward_data


# ── Status classification ────────────────────────────────────────────────────
# Verbatim translation of harbor_healthcheck:_row_from_trial's classification
# block. Same priority order — setup failures cascade so they win over every
# downstream symptom; patch_failed before lsv_init_empty because no patch
# means no measurement; tests_failed degrades a successful row.


def classify_status(reward: dict) -> tuple[str, str | None]:
    """Return ``(status, error_message)`` from the reward dossier.

    Inputs come straight out of ``write_reward()``'s dict, so this
    re-uses the in-process payload — no JSON round-trip.
    """
    setup = reward.get("setup") or {}
    setup_exit = setup.get("exit_code")
    setup_phase = setup.get("failed_phase")
    if setup_exit not in (None, 0):
        if setup_phase == "lsv_init":
            return (
                "lsv_init_failed",
                f"lsv_init.py crashed in setup.sh (exit={setup_exit}). "
                "Benchmark discovery never completed, so no dep DB was "
                "written. Check setup.txt in the trial dir for the "
                "underlying ASV/LSV traceback.",
            )
        return (
            "setup_failed",
            f"setup.sh failed in phase '{setup_phase}' (exit={setup_exit}).",
        )

    patch = reward.get("patch") or {}
    if patch.get("applied") is False:
        return "patch_failed", "solve.sh produced no diff vs base_commit"

    lsv_init = (reward.get("lsv") or {}).get("init") or {}
    impactable = len(lsv_init.get("benchmarks_impactable") or [])
    source_files_covered = lsv_init.get("source_files_covered")
    if lsv_init and impactable == 0 and source_files_covered == 0:
        return (
            "lsv_init_empty",
            "LSV init mapped 0 source files and 0 impactable benchmarks — "
            "ASV/LSV could not trace imports into this repo's benchmark suite.",
        )

    max_speedup = reward.get("max_speedup")
    if max_speedup is not None:
        if reward.get("tests_passed") is False:
            return "tests_failed", "tests_passed=False in reward.json"
        return "success", None

    lsv_error = reward.get("lsv_error")
    if lsv_error:
        return "lsv_measure_failed", str(lsv_error)

    if not reward.get("per_benchmark_speedups"):
        return "no_benchmarks", None

    return "failed", None


def pytest_success_ratio(reward: dict) -> float | None:
    pytest_block = reward.get("pytest") or {}
    try:
        total = int(pytest_block.get("total", 0) or 0)
        failed = int(pytest_block.get("failed", 0) or 0)
        err = int(pytest_block.get("error", 0) or 0)
        if total > 0:
            return (total - failed - err) / total
    except (TypeError, ValueError):
        pass
    return None


# ── Supabase persistence ─────────────────────────────────────────────────────
# Stdlib-only PostgREST + Storage. Each helper returns silently on auth/config
# misses so a forgotten env var degrades to "no row written" rather than
# crashing the whole verifier — the runner's failure-path sweep then notices
# the missing run_id.txt sentinel and writes a row from result.json.


def _supabase_request(
    url: str,
    method: str,
    headers: dict[str, str],
    *,
    data: bytes | None = None,
    content_type: str | None = None,
    timeout: float = 60.0,
) -> tuple[int, bytes]:
    """One-shot HTTP for PostgREST/Storage. Returns ``(status, body)`` and
    never raises on HTTP error — caller decides how to handle 4xx/5xx so
    we can swallow FK violations (PostgREST 23503 on candidate_containers
    orphans) the same way the legacy runner did."""
    if content_type:
        headers = {**headers, "Content-Type": content_type}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)  # noqa: S310
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        body = b""
        try:
            body = exc.read()
        except Exception:
            pass
        return exc.code, body
    except urllib.error.URLError as exc:
        print(f"[parser] Supabase request transport error: {exc}")
        return 0, b""


def _service_headers() -> dict[str, str] | None:
    """Service-role headers for datasmith Supabase. Returns None if creds
    aren't set (e.g. FORMULACODE_NO_UPLOAD path); caller skips persistence."""
    key = os.environ.get("DATASMITH_SUPABASE_SERVICE_KEY", "")
    if not key:
        return None
    headers = {
        "Authorization": f"Bearer {key}",
        "apikey": key,
        "User-Agent": "datasmith-parser/1.0",
    }
    cf_id = os.environ.get("DATASMITH_CF_ACCESS_CLIENT_ID", "")
    cf_secret = os.environ.get("DATASMITH_CF_ACCESS_CLIENT_SECRET", "")
    if cf_id and cf_secret:
        headers["CF-Access-Client-Id"] = cf_id
        headers["CF-Access-Client-Secret"] = cf_secret
    return headers


def _build_harbor_runs_row(
    *,
    run_id: str,
    owner: str,
    repo: str,
    issue_number: int,
    reward: dict,
) -> dict:
    """Map reward payload + env vars into a harbor_runs row dict."""
    status, error_message = classify_status(reward)
    speedups = reward.get("per_benchmark_speedups") or {}
    n_benchmarks = reward.get("num_valid_benchmarks") or len(speedups) or None
    timings = reward.get("timings") or {}
    wallclock = timings.get("test_total_s")

    agent_name = os.environ.get("HARBOR_AGENT_NAME") or os.environ.get("LSV_AGENT_NAME") or "oracle"
    model_name = os.environ.get("LSV_MODEL_NAME") or agent_name

    return {
        "run_id": run_id,
        "owner": owner,
        "repo": repo,
        "sha": os.environ.get("LSV_SHA", ""),
        "issue_number": issue_number,
        "container_name": os.environ.get("LSV_CONTAINER_NAME", "") or None,
        "environment": os.environ.get("LSV_ENV", ""),
        "agent_name": agent_name,
        "model_name": model_name,
        "model_agent_signature": f"{agent_name}:{model_name}",
        "status": status,
        "max_speedup": reward.get("max_speedup"),
        "geomean_speedup": reward.get("lsv_mean_speedup"),
        "n_benchmarks": n_benchmarks,
        "wallclock_sec": wallclock,
        "reward_payload": reward,
        "pytest_success_ratio": pytest_success_ratio(reward),
        "error_message": error_message,
    }


def _insert_harbor_run(base_url: str, headers: dict[str, str], row: dict) -> bool:
    """POST one row to harbor_runs. Idempotent via on_conflict=run_id (the
    sandbox-generated UUID is the PK). Returns True on success."""
    on_conflict = "run_id"
    url = f"{base_url}/rest/v1/harbor_runs?on_conflict={on_conflict}"
    post_headers = {
        **headers,
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    status, body = _supabase_request(
        url, "POST", post_headers,
        data=json.dumps(row).encode(),
        content_type="application/json",
    )
    if 200 <= status < 300:
        print(f"[parser] inserted harbor_runs row run_id={row['run_id']} status={row['status']}")
        return True
    # PostgREST returns 409 with code 23503 for FK violations against
    # candidate_containers — orphan PR, no candidate_containers row. Same
    # graceful skip the legacy runner did at harbor_healthcheck:825.
    text = body.decode(errors="replace")
    if status == 409 and "23503" in text:
        print(
            f"[parser] harbor_runs orphan row skipped: {row['owner']}/{row['repo']}@"
            f"{(row.get('sha') or '')[:12]} status={row['status']} (no candidate_containers row)"
        )
        return False
    print(f"[parser] harbor_runs insert failed: HTTP {status} body={text[:300]!r}")
    return False


def _patch_harbor_run(
    base_url: str,
    headers: dict[str, str],
    *,
    run_id: str,
    fields: dict,
) -> bool:
    """PATCH one column on the just-inserted harbor_runs row (typically
    ``artifacts_storage_url`` after the tarball uploads)."""
    url = f"{base_url}/rest/v1/harbor_runs?run_id=eq.{run_id}"
    patch_headers = {**headers, "Prefer": "return=minimal"}
    status, body = _supabase_request(
        url, "PATCH", patch_headers,
        data=json.dumps(fields).encode(),
        content_type="application/json",
    )
    if 200 <= status < 300:
        return True
    print(f"[parser] harbor_runs PATCH failed: HTTP {status} body={body.decode(errors='replace')[:300]!r}")
    return False


def _upload_storage(
    base_url: str,
    headers: dict[str, str],
    *,
    bucket: str,
    object_key: str,
    blob: bytes,
    content_type: str = "application/gzip",
) -> bool:
    """Upload to Supabase Storage with upsert. Returns True on success."""
    url = f"{base_url}/storage/v1/object/{bucket}/{object_key}"
    upload_headers = {**headers, "x-upsert": "true"}
    status, body = _supabase_request(
        url, "POST", upload_headers,
        data=blob,
        content_type=content_type,
        timeout=300.0,
    )
    if 200 <= status < 300:
        print(f"[parser] uploaded {len(blob)} bytes → {bucket}/{object_key}")
        return True
    print(f"[parser] storage upload {bucket}/{object_key} failed: HTTP {status} body={body.decode(errors='replace')[:300]!r}")
    return False


def _patch_pull_request_baseline(
    base_url: str,
    headers: dict[str, str],
    *,
    owner: str,
    repo: str,
    issue_number: int,
    baseline_run_id: str,
    snapshot_storage_url: str | None,
) -> bool:
    """Promote this run as the PR's oracle baseline pointer. PATCHes
    ``pull_requests.baseline_run_id`` and (when present) ``snapshot_storage_url``
    so stage 8 publish + future agent runs use this oracle as the comparison
    point. Daytona-success-only — see ``_persist_supabase`` for the gate."""
    payload: dict = {"baseline_run_id": baseline_run_id}
    if snapshot_storage_url:
        payload["snapshot_storage_url"] = snapshot_storage_url
    url = (
        f"{base_url}/rest/v1/pull_requests"
        f"?owner=eq.{owner}&repo=eq.{repo}&issue_number=eq.{issue_number}"
    )
    patch_headers = {**headers, "Prefer": "return=minimal"}
    status, body = _supabase_request(
        url, "PATCH", patch_headers,
        data=json.dumps(payload).encode(),
        content_type="application/json",
    )
    if 200 <= status < 300:
        print(
            f"[parser] PATCH pull_requests {owner}/{repo}#{issue_number}: "
            f"baseline_run_id={baseline_run_id[:8]}…"
            + (f" + snapshot_storage_url" if snapshot_storage_url else "")
        )
        return True
    print(f"[parser] pull_requests PATCH failed: HTTP {status} body={body.decode(errors='replace')[:300]!r}")
    return False


def persist_supabase(
    *,
    run_id: str,
    task_id: str,
    reward: dict,
) -> bool:
    """End-to-end Supabase persistence for one trial.

    Returns True iff the harbor_runs row landed; the run_id.txt sentinel
    (written by ``main()`` after this returns) gates whether the runner's
    sweep treats the trial as already-persisted.
    """
    base_url = os.environ.get("DATASMITH_SUPABASE_URL", "").rstrip("/")
    headers = _service_headers()
    if not base_url or not headers:
        print("[parser] persist: SKIP (DATASMITH_SUPABASE_* not set)")
        return False
    if os.environ.get("FORMULACODE_NO_UPLOAD"):
        print("[parser] persist: SKIP (FORMULACODE_NO_UPLOAD set)")
        return False

    parsed = _parse_task_id(task_id)
    if parsed is None:
        print(f"[parser] persist: SKIP (could not parse task_id {task_id!r})")
        return False
    owner, repo, issue_number = parsed

    row = _build_harbor_runs_row(
        run_id=run_id, owner=owner, repo=repo, issue_number=issue_number, reward=reward,
    )

    if not _insert_harbor_run(base_url, headers, row):
        return False

    # Run-artifacts tarball — upload + PATCH the row's URL column. Best-effort:
    # a missing / empty tarball just means we leave artifacts_storage_url null.
    runs_bucket = os.environ.get("DATASMITH_RUNS_BUCKET", "runs")
    artifacts_path = Path("/logs/verifier/run_artifacts.tar.gz")
    if artifacts_path.exists() and artifacts_path.stat().st_size > 0:
        artifacts_key = f"{owner}__{repo}__{issue_number}/{run_id}.tar.gz"
        try:
            blob = artifacts_path.read_bytes()
        except OSError as exc:
            print(f"[parser] run_artifacts read failed: {exc}")
        else:
            if _upload_storage(
                base_url, headers,
                bucket=runs_bucket, object_key=artifacts_key, blob=blob,
            ):
                _patch_harbor_run(
                    base_url, headers,
                    run_id=run_id, fields={"artifacts_storage_url": artifacts_key},
                )

    # Snapshot tarball + pull_requests baseline pointer — oracle daytona
    # success only. Mirrors the runner's _update_baseline_pointers gate
    # (harbor_healthcheck:756). Docker rows are intentionally not eligible:
    # publish (stage 8) gates against daytona only, and we don't want a flaky
    # local docker iteration to overwrite a leaderboard pointer.
    is_oracle = row["agent_name"] == "oracle"
    is_daytona = row["environment"] == "daytona"
    is_success = row["status"] == "success"
    if is_oracle and is_daytona and is_success:
        snapshots_bucket = os.environ.get("DATASMITH_SNAPSHOTS_BUCKET", "snapshots")
        snapshot_path = Path("/logs/verifier/oracle_snapshots.tar.gz")
        snapshot_key: str | None = None
        if snapshot_path.exists() and snapshot_path.stat().st_size > 0:
            try:
                blob = snapshot_path.read_bytes()
            except OSError as exc:
                print(f"[parser] oracle_snapshots read failed: {exc}")
                blob = b""
            if blob and _upload_storage(
                base_url, headers,
                bucket=snapshots_bucket,
                object_key=f"{owner}__{repo}__{issue_number}/oracle.tar.gz",
                blob=blob,
            ):
                snapshot_key = f"{owner}__{repo}__{issue_number}/oracle.tar.gz"
        _patch_pull_request_baseline(
            base_url, headers,
            owner=owner, repo=repo, issue_number=issue_number,
            baseline_run_id=run_id,
            snapshot_storage_url=snapshot_key,
        )
    return True


# ── Main ─────────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute reward from LSV results")
    parser.add_argument("--owner", required=True, help="Repository owner")
    parser.add_argument("--repo", required=True, help="Repository name")
    parser.add_argument("--issue-number", required=True, type=int, help="PR number")
    parser.add_argument(
        "--agent-key", default="agent", help="Agent key (e.g., oracle, terminus-2)"
    )
    args = parser.parse_args()

    # Load all sidecars written by setup.sh / test.sh. Each helper returns
    # an empty/default payload if its file is missing, so a partially-run
    # trial still produces a well-formed reward.json.
    patch_info = load_patch_info(LOG_DIR)
    timings = load_timings(LOG_DIR)
    snapshot_block = summarize_snapshots(LOG_DIR)
    setup_status = load_setup_status(LOG_DIR)

    # Load LSV results
    lsv_results = load_lsv_results(LSV_DIR)
    lsv_init_summary = summarize_lsv_init(lsv_results)

    # Generate run_id ONCE here — persist_supabase uses it as the harbor_runs
    # PK, the sentinel file at /logs/verifier/run_id.txt uses it to tell the
    # runner's failure-path sweep "this trial already wrote a row."
    run_id = str(uuid.uuid4())

    is_oracle = args.agent_key == "oracle"

    if not lsv_results:
        print("[parser] No LSV results found. Writing zero reward.")
        reward = write_reward(
            REWARD_DIR,
            None,
            None,
            {},
            {"level4": 0.0},
            False,
            snapshot_block,
            patch=patch_info,
            lsv_init_summary=lsv_init_summary,
            lsv_measure_raw=None,
            pytest_summary={},
            timings=timings,
            setup_status=setup_status,
            is_oracle=is_oracle,
        )
        # Even on the no-results path we still try to land a harbor_runs
        # row (status will classify as setup_failed / lsv_init_empty / etc.)
        # so the trial appears in dashboards. If persistence lands, write
        # the sentinel before exiting non-zero.
        if persist_supabase(run_id=run_id, task_id=args.task_id, reward=reward):
            (REWARD_DIR / "run_id.txt").write_text(run_id)
        sys.exit(1)

    measure_results = lsv_results.get("measure", {}) or {}
    benchmarks = measure_results.get("benchmarks", {}) or {}
    lsv_error = measure_results.get("error")

    # Compute speedups (baseline/current from LSV)
    speedups = compute_per_benchmark_speedups(benchmarks)
    speedup_levels = aggregate_by_hierarchy(speedups)

    # Compute advantages (oracle-relative)
    advantages = None
    advantage_levels = None

    if args.agent_key == "oracle":
        # Oracle advantage is 0 by definition
        advantages = {name: 0.0 for name in speedups}
        advantage_levels = aggregate_by_hierarchy(advantages)
    else:
        # Fetch oracle data from datasmith Supabase
        base_url = os.environ.get("DATASMITH_SUPABASE_URL", "")
        if base_url:
            oracle_benchmarks = fetch_oracle_benchmarks(
                base_url, args.owner, args.repo, args.issue_number
            )
            if oracle_benchmarks:
                advantages = compute_per_benchmark_advantages(
                    benchmarks, oracle_benchmarks
                )
                advantage_levels = aggregate_by_hierarchy(advantages)
            else:
                print("[parser] Could not fetch oracle data; advantage will be null")
        else:
            print("[parser] DATASMITH_SUPABASE_URL not set; skipping advantage computation")

    # Load test results + summarize pytest into a dossier block.
    tests_passed, _, test_raw = load_test_results(LOG_DIR)
    pytest_summary = summarize_pytest(test_raw)
    log_snapshot_results(LOG_DIR)

    # Write reward files
    reward = write_reward(
        REWARD_DIR,
        advantages,
        advantage_levels,
        speedups,
        speedup_levels,
        tests_passed,
        snapshot_block,
        lsv_error=lsv_error,
        patch=patch_info,
        lsv_init_summary=lsv_init_summary,
        lsv_measure_raw=measure_results,
        pytest_summary=pytest_summary,
        timings=timings,
        setup_status=setup_status,
        is_oracle=is_oracle,
    )
    if lsv_error:
        print(f"[parser] lsv_error = {lsv_error}")
    if setup_status.get("exit_code") not in (None, 0):
        print(
            f"[parser] setup_status = failed (exit_code={setup_status.get('exit_code')}, "
            f"phase={setup_status.get('failed_phase')})"
        )

    # End-to-end Supabase persistence: harbor_runs INSERT, run_artifacts
    # upload, snapshot upload + pull_requests pointer update (oracle daytona
    # success only). Sentinel last so a partial failure doesn't poison the
    # runner sweep's "already wrote" check.
    if persist_supabase(run_id=run_id, task_id=args.task_id, reward=reward):
        (REWARD_DIR / "run_id.txt").write_text(run_id)
        print(f"[parser] wrote run_id.txt sentinel (run_id={run_id})")
    else:
        print("[parser] persistence skipped/failed — runner sweep will handle this trial")

    print("[parser] Complete.")


if __name__ == "__main__":
    main()
