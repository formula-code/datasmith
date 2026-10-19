"""Select tasks to build: render records for chosen PRs, or a ranked sample of database candidates.

Candidates must be performance PRs of an ingest-enabled repo with a patch, a base commit, a rendered problem and an
environment image on Docker Hub that was built at the base commit. Rules and sampling are as in docs/TASK_PIPELINE.md
(A1-A4, H4): oracle evidence first, then tier 1 images, then tier 2 round-robin over difficulty strata, large suites last.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import statistics
import sys
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any

PR_FIELDS = "owner,repo,issue_number,merge_commit_sha,base_sha,container_name,patch,rendered_problem,classification,difficulty,merged_at"
HARNESS = re.compile(r"^diff --git a/\S*(benchmarks?/|asv_bench/|asv\.conf|conftest\.py|\.github/)", re.M)
BAD_WARNINGS = {
    "oracle_patch_touches_benchmarks",
    "speedup_direction",
    "base_tests_failing",
    "pytest_collect_failed",
    "measure_partial",
    "dilution_ratio",
}
QUOTA = {"easy": 0.3, "medium": 0.4, "hard": 0.3}
LARGE = {"pandas-dev-pandas", "apache-arrow", "scikit-learn-scikit-learn", "scipy-scipy", "qiskit-qiskit"}
REGISTRY = "https://registry-1.docker.io/v2/formulacode"
UA = {"User-Agent": "curl/8.0"}


def get_json(url: str, headers: dict | None = None) -> Any:
    for i in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers or UA), timeout=120) as r:  # noqa: S310
                return json.load(r)
        except urllib.error.HTTPError as e:
            if e.code != 429 or i == 4:
                raise
            time.sleep(10 * (i + 1))
    return None


def image_name(owner: str, repo: str) -> str:
    return f"{owner}-{repo}".lower()


def record(p: dict) -> dict:
    """A render record (harbor_adapter FormulaCodeRecord fields) from a pull_requests row."""
    owner, repo = p["owner"], p["repo"]
    return {
        "container_name": p.get("container_name") or f"formulacode/{image_name(owner, repo)}:{p['issue_number']}",
        "patch": p.get("patch") or "",
        "owner": owner,
        "repo": repo,
        "issue_number": int(p["issue_number"]),
        "gt_hash": p.get("merge_commit_sha") or "",
        "base_commit": p.get("base_sha") or "",
        "instructions": p.get("rendered_problem") or "",
        "date": p.get("merged_at"),
        "classification": p.get("classification") or "optimization",
        "difficulty": p.get("difficulty") if p.get("difficulty") in QUOTA else "unknown",
        "repo_name": f"{owner}/{repo}",
    }


def _rows(client: Any, table: str, select: str, **eq: Any) -> list[dict]:
    rows: list[dict] = []
    while True:
        q = client.table(table).select(select)
        for k, v in eq.items():
            q = q.eq(k, v)
        page = q.range(len(rows), len(rows) + 999).execute().data
        rows += page
        if len(page) < 1000:
            return rows


def records_for(client: Any, ids: list[str]) -> tuple[list[dict], list[str]]:
    """Render records for `owner/repo#N` ids, and the ids with no complete row."""
    out, missing = [], []
    for t in ids:
        owner, repo, num = re.fullmatch(r"([\w.-]+)/([\w.-]+)#(\d+)", t).groups()
        rows = (
            client.table("pull_requests")
            .select(PR_FIELDS)
            .eq("owner", owner)
            .eq("repo", repo)
            .eq("issue_number", int(num))
            .execute()
            .data
        )
        p = rows[0] if rows else None
        if not p or not all(p.get(k) for k in ("patch", "base_sha", "merge_commit_sha", "rendered_problem")):
            missing.append(t)
        else:
            out.append(record(p))
    return out, missing


def db_pool(client: Any, counts: bool = False) -> dict[str, dict]:
    """Candidates from the database after rules A1 (repo, patch), A3 (4-300 measured benchmarks) and A4 (warnings, time, size)."""
    allowed = {(r["owner"], r["repo"]) for r in _rows(client, "repositories", "owner,repo", ingest_enabled=True)}
    prs = [
        p
        for p in _rows(client, "pull_requests", PR_FIELDS, is_performance_commit=True)
        if p.get("container_name") and p.get("base_sha") and p.get("rendered_problem")
    ]
    cc = {
        (c["owner"], c["repo"], c["sha"]): c
        for c in _rows(
            client,
            "candidate_containers",
            "owner,repo,sha,verification_state,manifest_warnings,v:build_manifest->verify,rm:resource_metrics",
        )
    }
    runs = defaultdict(list)
    for h in _rows(
        client, "harbor_runs", "owner,repo,sha,max_speedup,n_benchmarks", agent_name="oracle", status="success"
    ):
        runs[(h["owner"], h["repo"], h["sha"])].append(h)
    bench: dict = defaultdict(dict)
    for b in _rows(
        client,
        "benchmark_information",
        "owner,repo,issue_number,benchmark_name,oracle_speedup_vs_nop",
        agent_name="oracle",
    ):
        d = bench[(b["owner"], b["repo"], b["issue_number"])]
        d[b["benchmark_name"]] = max(d.get(b["benchmark_name"], 0), b["oracle_speedup_vs_nop"] or 0)
    pool, drop = {}, Counter()
    for p in prs:
        key3, pr = (p["owner"], p["repo"], p["merge_commit_sha"]), (p["owner"], p["repo"], p["issue_number"])
        c, orc, b = cc.get(key3) or {}, runs.get(key3, []), bench.get(pr, {})
        verify, rm = c.get("v") or {}, c.get("rm") or {}
        n_measured = verify.get("benchmarks_measured_n") or (
            statistics.median(r["n_benchmarks"] or 0 for r in orc) if orc else None
        )
        best = max((r["max_speedup"] or 0 for r in orc), default=None)
        n_strong = sum(v >= 1.2 for v in b.values())
        if pr[:2] not in allowed or not p.get("patch"):
            drop["A1 repo not ingest_enabled or no patch"] += 1
        elif n_measured is not None and not 4 <= n_measured <= 300:
            drop["A3 measured benchmark count outside 4-300"] += 1
        elif (
            set(c.get("manifest_warnings") or []) & BAD_WARNINGS
            or (verify.get("measure_duration_s") or 0) > 2700
            or (rm.get("image_size_bytes") or 0) > 20e9
        ):
            drop["A4 manifest, measure time, image size"] += 1
        else:
            pool["{}/{}#{}".format(*pr)] = {
                "record": record(p),
                "source": "db",
                "verification_state": c.get("verification_state"),
                "n_measured": n_measured,
                "best_max": best,
                "n_strong": n_strong if b else None,
                "evidence": bool(best and best >= 1.05) or n_strong >= 2,
            }
    for why, n in drop.items() if counts else ():
        print(f"  db rule {why}: -{n}")
    return pool


def hub_tags(path: Path) -> dict[tuple[str, int], dict]:
    """Docker Hub tags from a tsv written by refresh_hub: (image, tag) -> bytes, pushed."""
    lines = path.read_text().splitlines()[1:]
    return {(i, int(t)): {"bytes": int(b), "pushed": p} for i, t, b, p in (line.split("\t") for line in lines)}


def refresh_hub(path: Path) -> None:
    """List formulacode's Docker Hub repositories and numeric tags into a tsv."""
    names: set[str] = set()
    for order in ("name", "-name", "last_updated", "-last_updated", "pull_count", "-pull_count"):
        for page in (1, 2):
            try:
                url = f"https://hub.docker.com/v2/repositories/formulacode/?page_size=50&page={page}&ordering={order}"
                names |= {r["name"] for r in get_json(url)["results"]}
            except urllib.error.HTTPError:
                continue
    rows = []
    for name in sorted(names):
        url = f"https://hub.docker.com/v2/repositories/formulacode/{name}/tags/?page_size=50"
        while url:
            try:
                page = get_json(url)
            except urllib.error.HTTPError:
                break
            rows += [
                (name, int(t["name"]), t.get("full_size") or 0, t.get("tag_last_pushed") or "")
                for t in page["results"]
                if t["name"].isdigit()
            ]
            url = page.get("next")
    path.write_text("image\ttag\tbytes\tpushed\n" + "".join("\t".join(map(str, r)) + "\n" for r in sorted(rows)))


def image_ref(image: str, tag: int, cache: Path) -> str:
    """The vcs.ref label of a Hub image (the commit its repo checkout is at); cached in a tsv."""
    known = dict(line.split("\t") for line in cache.read_text().splitlines()) if cache.is_file() else {}
    key = f"{image}:{tag}"
    if key not in known:
        token = get_json(
            f"https://auth.docker.io/token?service=registry.docker.io&scope=repository:formulacode/{image}:pull"
        )["token"]
        headers = {
            **UA,
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.docker.distribution.manifest.v2+json,application/vnd.oci.image.manifest.v1+json",
        }
        req = urllib.request.Request(f"{REGISTRY}/{image}/manifests/{tag}", headers=headers)  # noqa: S310
        with urllib.request.urlopen(req, timeout=60) as r:  # noqa: S310
            config, left = (
                json.load(r)["config"]["digest"],
                int(r.headers.get("ratelimit-remaining", "100").split(";")[0]),
            )
        known[key] = (get_json(f"{REGISTRY}/{image}/blobs/{config}", headers)["config"].get("Labels") or {}).get(
            "vcs.ref", ""
        )
        with cache.open("a") as f:
            f.write(f"{key}\t{known[key]}\n")
        if left < int(os.environ.get("FC_HUB_RESERVE", "40")):
            print(f"Docker Hub pulls left this hour: {left}; waiting 10 min", file=sys.stderr)
            time.sleep(600)
    return known[key]


def filter_pool(
    pool: dict[str, dict],
    hub: dict,
    known: set[str],
    exclude: set[str],
    tier1: tuple[str, str],
    built_from: str,
    counts: bool = False,
) -> list[dict]:
    """Rules H4 (image on Hub, at most 6 GB), A1 (complete record, repo not excluded), A2 (patch edits no harness file)."""
    out = []
    for task, c in pool.items():
        m = re.fullmatch(r"formulacode/([\w.-]+):(\d+)", c["record"]["container_name"])
        c.update(
            task=task,
            repo_key=image_name(c["record"]["owner"], c["record"]["repo"]),
            hub=hub.get((m[1], int(m[2]))) if m else None,
        )
        out.append(c)
    rules = [
        ("H4 image on Docker Hub", lambda c: c["hub"] is not None),
        ("not already chosen", lambda c: c["task"] not in known),
        (
            "A1 record complete",
            lambda c: all(c["record"][k] for k in ("patch", "base_commit", "gt_hash", "instructions")),
        ),
        ("A1 repo not excluded", lambda c: c["repo_key"] not in exclude),
        ("A2 oracle patch does not edit harness files", lambda c: not HARNESS.search(c["record"]["patch"])),
        ("A4 Hub size <= 6 GB compressed", lambda c: c["hub"]["bytes"] <= 6e9),
        ("image pushed after the merge-commit builds", lambda c: c["hub"]["pushed"] >= built_from),
    ]
    for name, keep in rules:
        out = [c for c in out if keep(c)]
        if counts:
            print(f"  {name}: {len(out)}")
    for c in out:
        n = c.get("n_measured")
        c["tier"] = 1 if tier1[0] <= c["hub"]["pushed"][:10] < tier1[1] else 2
        c["stratum"] = (
            c["record"]["difficulty"]
            + "/"
            + ("na" if n is None else "4-20" if n <= 20 else "21-80" if n <= 80 else "81-300")
        )
        c["large"] = c["repo_key"] in LARGE
    return out


def _round_robin(main: list[dict], n: int, rows: list[dict], take: Callable, free: Callable) -> None:
    """Tier 2 rows, one stratum at a time, until each difficulty's quota of the remaining rows is used."""
    strata = defaultdict(list)
    for c in main:
        if c["tier"] == 2:
            strata[c["stratum"]].append(c)
    quota = {d: round(q * (n - len(rows))) for d, q in QUOTA.items()}
    moved = True
    while moved and len(rows) < n:
        moved = False
        for s in sorted(strata):
            d = s.split("/")[0]
            c = next((c for c in strata[s] if free(c)), None) if quota.get(d, 0) > 0 else None
            if c:
                take(c, "tier2")
                quota[d] -= 1
                moved = True


def sample(  # noqa: C901
    seeds: list[dict],
    pool: list[dict],
    n: int,
    seed: str,
    cap: Callable[[str], int],
    accept: Callable[[dict], bool] = lambda c: True,
) -> list[dict]:
    """Seeds, then evidence, tier 1, tier 2 round-robin over strata by difficulty quota, the rest, large suites last.
    `accept(c)` runs only for a row that would be taken next."""

    def prio(c: dict) -> str:
        return hashlib.sha256(f"{seed}:{c['task']}".encode()).hexdigest()

    rows: list[dict] = []
    per_repo: Counter = Counter()
    seen: set[str] = set()

    def take(c: dict, group: str) -> None:
        rows.append({**c, "group": group, "prio": prio(c)[:12]})
        per_repo[c["repo_key"]] += 1
        seen.add(c["task"])

    def free(c: dict) -> bool:
        if len(rows) >= n or c["task"] in seen or per_repo[c["repo_key"]] >= cap(c["repo_key"]):
            return False
        seen.add(c["task"])
        return accept(c)

    for c in sorted(seeds, key=prio):
        take(c, "seed")
    main = sorted((c for c in pool if not c["large"]), key=prio)
    for group, wanted in (("evidence", lambda c: c.get("evidence")), ("tier1", lambda c: c["tier"] == 1)):
        for c in main:
            if wanted(c) and free(c):
                take(c, group)
    _round_robin(main, n, rows, take, free)
    for c in main + sorted((c for c in pool if c["large"]), key=prio):
        if free(c):
            take(c, "large" if c["large"] else "fill")
    return rows


def write_selection(rows: list[dict], out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    (out / "records.jsonl").write_text("".join(json.dumps(r["record"], sort_keys=True) + "\n" for r in rows))
    (out / "ids.md").write_text("".join(f"- {r['task']}\n" for r in rows))
    cols = [
        "rank",
        "task",
        "group",
        "tier",
        "stratum",
        "source",
        "verification_state",
        "n_measured",
        "best_max",
        "n_strong",
    ]
    lines = ["\t".join(cols)] + [
        "\t".join(str({**r, "rank": i}.get(k, "") or "") for k in cols) for i, r in enumerate(rows, 1)
    ]
    (out / "candidates.tsv").write_text("\n".join(lines) + "\n")
