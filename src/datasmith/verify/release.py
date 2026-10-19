"""Release readiness of reviewed tasks: one row of facts per task, written to a CSV; no verdict.

Repo licenses and PR authors come from the GitHub REST API (token in GITHUB_TOKEN) and are cached in
`<survey>/release/provenance.json`.
"""

from __future__ import annotations

import csv
import json
import os
import urllib.request
from pathlib import Path
from typing import Any

from datasmith.verify.measure import read_json, trial_run
from datasmith.verify.pin import read_pin
from datasmith.verify.prompt_check import check_task
from datasmith.verify.survey import Survey, task_id

FIELDS = [
    "task",
    "pinned_image",
    "in_registry",
    "render_commit",
    "test_refresh_commit",
    "oracle_runs",
    "nop_runs",
    "runs_with_speedups",
    "tests_ran",
    "cpu_models",
    "license",
    "pr_author",
    "pr_url",
    "merged_at",
    "prompt_giveaway",
    "note",
]


def registry_tag(image: str) -> str:
    """`fc-task/<task>:<id>` -> the registry tag `<task>__<id>`."""
    name, ident = image.split(":", 1)
    return f"{name.split('/')[-1]}__{ident}"


def _get(url: str, token: str | None) -> Any:
    headers = {"Accept": "application/vnd.github+json", **({"Authorization": f"Bearer {token}"} if token else {})}
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=30) as r:  # noqa: S310
        return json.load(r)


def provenance(owner: str, repo: str, number: str, token: str | None = None) -> dict[str, Any]:
    api = "https://api.github.com"
    pr = _get(f"{api}/repos/{owner}/{repo}/pulls/{number}", token)
    meta = _get(f"{api}/repos/{owner}/{repo}", token)
    return {
        "license": ((meta.get("license") or {}).get("spdx_id")),
        "pr_author": (pr.get("user") or {}).get("login"),
        "pr_url": pr.get("html_url"),
        "merged_at": pr.get("merged_at"),
    }


def cached_provenance(survey: Survey, tasks: list[str], fetch: bool) -> dict[str, dict]:
    path = survey.root / "release" / "provenance.json"
    cache = read_json(path) or {}
    if fetch:
        token = os.environ.get("GITHUB_TOKEN")
        for t in tasks:
            if t in cache:
                continue
            owner, repo, num = t.split("__")
            try:
                cache[t] = provenance(owner, repo, num, token)
            except (OSError, ValueError) as e:
                cache[t] = {"error": str(e)[:200]}
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(cache, indent=1, sort_keys=True))
    return cache


def task_row(
    survey: Survey, task: str, record: dict | None, registry: set[str] | None, prov: dict, giveaway: dict
) -> dict:
    td = survey.task_dir(task)
    toml = td / "task.toml"
    image = read_pin(toml.read_text()) if toml.exists() else None
    stamp = read_json(td / "render_stamp.json") or {}
    refresh = read_json(td / "TESTS_REFRESHED.json") or {}
    runs = (record or {}).get("runs") or []
    cpus = set()
    for r in runs:
        cpu = r.get("cpu_model")
        if cpu is None and r.get("trial_dir") and (Path(r["trial_dir"]) / "result.json").exists():
            cpu = trial_run(Path(r["trial_dir"]), [])["cpu_model"]
        cpus.add(cpu or "unknown")
    return {
        "task": task,
        "pinned_image": image or "",
        "in_registry": "" if registry is None or not image else registry_tag(image) in registry,
        "render_commit": (stamp.get("datasmith") or {}).get("commit", "")[:12],
        "test_refresh_commit": str(refresh.get("datasmith_commit") or refresh.get("commit") or "")[:12],
        "oracle_runs": sum(r["role"] != "nop" for r in runs),
        "nop_runs": sum(r["role"] == "nop" for r in runs),
        "runs_with_speedups": sum(bool(r.get("log_speedups")) for r in runs),
        "tests_ran": sum(bool((r.get("tests") or {}).get("ran")) for r in runs),
        "cpu_models": ";".join(sorted(cpus)),
        "license": prov.get("license") or "",
        "pr_author": prov.get("pr_author") or "",
        "pr_url": prov.get("pr_url") or "",
        "merged_at": prov.get("merged_at") or "",
        "prompt_giveaway": giveaway.get(task, ""),
        "prompt_findings": "; ".join(check_task(td)) if (td / "instruction.md").exists() else "",
        "note": prov.get("error", ""),
    }


def readiness(
    survey: Survey,
    decision: str = "approved",
    registry: set[str] | None = None,
    fetch: bool = False,
    giveaway: dict | None = None,
) -> list[dict]:
    tasks = [task_id(r["task"]) for r in survey.reviews() if r["decision"] == decision]
    records = {p.stem: read_json(p) for p in (survey.root / "oracle").glob("*.json")}
    prov = cached_provenance(survey, tasks, fetch)
    return [task_row(survey, t, records.get(t), registry, prov.get(t) or {}, giveaway or {}) for t in tasks]


def summary(rows: list[dict]) -> dict[str, Any]:
    """Counts across rows: how many distinct render commits and CPU types, and how many rows miss each item."""
    return {
        "tasks": len(rows),
        "render_commits": len({r["render_commit"] for r in rows}),
        "cpu_types": sorted({c for r in rows for c in r["cpu_models"].split(";")}),
        "not_in_registry": sum(r["in_registry"] is False for r in rows),
        "runs_missing_speedups": sum(r["runs_with_speedups"] < r["oracle_runs"] + r["nop_runs"] for r in rows),
        "tests_not_run_everywhere": sum(r["tests_ran"] < r["oracle_runs"] + r["nop_runs"] for r in rows),
        "no_license": sum(not r["license"] for r in rows),
        "no_author": sum(not r["pr_author"] for r in rows),
        "prompt_not_symptom_only": sum(bool(r["prompt_findings"]) for r in rows),
    }


def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
