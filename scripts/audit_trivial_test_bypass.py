"""Audit candidate_containers for the trivial-test-bypass failure mode.

Heuristics for "would flip under stricter check":
  H1: build_pkg_sh / build_run_sh / dockerfile / run_tests_sh contains a heredoc
      or echo creating a test_*.py file at the workspace root (not under tests/).
  H2: harbor_runs reward_payload reports < MIN_TESTS distinct tests collected,
      where the upstream repo normally collects orders of magnitude more.
  H3: The collected node IDs in test_results.json are all rooted at "test_*.py"
      (no path component), i.e. discovery fell through to cwd.

Cross-reference with harbor_runs to count how many "currently successful"
containers would flip to failing.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict

from dotenv import load_dotenv

from datasmith.utils.db import fetch_all

load_dotenv("tokens.env")

# Heredoc / write of test_*.py to a path that is the repo root, $REPO_ROOT,
# /workspace/repo, or a quoted variant. We also accept any test_*.py that
# isn't clearly under a tests/ subdirectory.
ROOT_TEST_HEREDOC = re.compile(
    r"""(?xm)
    (?:cat|tee|printf|echo)
    [^\n]*?
    >\s*
    ["']?
    (?:\$\{?REPO_ROOT\}?/|\$\{?ROOT_PATH\}?/|/workspace/repo/|\./|\$PWD/)?
    (test_[\w./-]*\.py)
    ["']?
    """
)

DOCKERFILE_ADD_ROOT_TEST = re.compile(r"^\s*(?:ADD|COPY)\s+test_[\w]+\.py\s", re.MULTILINE)

# Pre-fabricated fake test summary that bypasses the validator
FAKE_SUMMARY_JSON = re.compile(
    r"""(?xs)
    /logs/summary_agent\.json
    [^\n]*\n
    [^{]*\{
    [^}]*"passed"\s*:\s*[1-9]
    """
)

# Pre-fabricated fake test_results.json
FAKE_TEST_RESULTS = re.compile(
    r"""(?xs)
    /logs/test_results\.json
    [^\n]*\n
    [^{]*\{
    """
)

# Trivial assert-True body indicates a no-op test
TRIVIAL_ASSERT = re.compile(r"def\s+test_\w+\s*\([^)]*\)\s*:\s*\n\s*(pass|assert\s+True)\b")


def script_blob(row: dict) -> str:
    return "\n".join(
        row.get(k) or ""
        for k in (
            "dockerfile",
            "build_base_sh",
            "build_env_sh",
            "build_pkg_sh",
            "build_run_sh",
            "build_final_sh",
            "run_tests_sh",
            "entrypoint_sh",
        )
    )


def classify(row: dict) -> set[str]:
    """Return a set of failure-mode tags this container exhibits."""
    tags: set[str] = set()
    blob = script_blob(row)
    for m in ROOT_TEST_HEREDOC.finditer(blob):
        path = m.group(1)
        # Anything not under a tests/ subdir is suspicious
        if "/tests/" not in path and not path.startswith("tests/"):
            tags.add("root_test_heredoc")
            break
    if DOCKERFILE_ADD_ROOT_TEST.search(row.get("dockerfile") or ""):
        tags.add("dockerfile_root_test")
    if FAKE_SUMMARY_JSON.search(blob):
        tags.add("fake_summary_agent_json")
    if FAKE_TEST_RESULTS.search(blob):
        tags.add("fake_test_results_json")
    if TRIVIAL_ASSERT.search(blob):
        tags.add("trivial_assert_inline")
    return tags


def main() -> None:  # noqa: C901
    print("Fetching candidate_containers...")
    containers = fetch_all(
        "candidate_containers",
        select="owner,repo,sha,issue_number,dockerfile,build_base_sh,build_env_sh,build_pkg_sh,build_run_sh,build_final_sh,run_tests_sh,entrypoint_sh",
    )
    print(f"  {len(containers)} candidate_containers")

    print("Fetching harbor_runs...")
    runs = fetch_all(
        "harbor_runs",
        select="owner,repo,sha,environment,status,max_speedup,n_benchmarks,reward_payload",
    )
    print(f"  {len(runs)} harbor_runs")

    # Index harbor runs by (owner, repo, sha)
    runs_by_sha: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    for r in runs:
        runs_by_sha[(r["owner"], r["repo"], r["sha"])].append(r)

    # Classify every container
    tag_counts: Counter[str] = Counter()
    flagged: list[tuple[dict, set[str]]] = []
    by_repo: Counter[str] = Counter()
    for c in containers:
        tags = classify(c)
        if tags:
            flagged.append((c, tags))
            for t in tags:
                tag_counts[t] += 1
            by_repo[f"{c['owner']}/{c['repo']}"] += 1

    print(
        f"\nFlagged containers: {len(flagged)} / {len(containers)} ({100 * len(flagged) / max(1, len(containers)):.1f}%)"
    )
    print("\nTag breakdown:")
    for tag, count in tag_counts.most_common():
        print(f"  {tag:>30}: {count}")
    print("\nTop repos by flagged-container count:")
    for repo, count in by_repo.most_common(15):
        print(f"  {repo:>40}: {count}")
    print("\nFirst 20 flagged containers:")
    for c, tags in flagged[:20]:
        print(f"  - {c['owner']}/{c['repo']}@{c['sha'][:8]} (PR #{c.get('issue_number')}) tags={sorted(tags)}")

    # H2: harbor_runs reward_payload n_benchmarks - but harbor measures benchmarks not tests.
    # The pytest_runner test summary lives in reward_payload under different keys depending
    # on harbor version. Best-effort scan.
    n_test_dist: Counter[int] = Counter()
    low_test_runs: list[dict] = []
    for r in runs:
        rp = r.get("reward_payload") or {}
        # Try common locations
        total = None
        for path in (
            ("test_summary", "total"),
            ("tests", "total"),
            ("pytest", "total"),
            ("results", "summary", "total"),
        ):
            cur = rp
            for k in path:
                if isinstance(cur, dict) and k in cur:
                    cur = cur[k]
                else:
                    cur = None
                    break
            if isinstance(cur, int):
                total = cur
                break
        if total is not None:
            n_test_dist[total] += 1
            if total <= 3:
                low_test_runs.append(r)

    print(f"\nReward payloads with test totals found: {sum(n_test_dist.values())}")
    if n_test_dist:
        print("  test-total distribution (top 15):")
        for total, count in n_test_dist.most_common(15):
            print(f"    {total:>6} tests: {count} runs")
    print(f"\nH2 (runs with <=3 tests collected): {len(low_test_runs)}")
    for r in low_test_runs[:20]:
        print(
            f"  - {r['owner']}/{r['repo']}@{r['sha'][:8]} env={r['environment']} status={r['status']} max_speedup={r['max_speedup']}"
        )

    # Currently-publishable: success on Daytona with max_speedup >= 1.05
    publishable: set[tuple[str, str, str]] = set()
    for r in runs:
        if r["environment"] == "daytona" and r["status"] == "success" and (r.get("max_speedup") or 0) >= 1.05:
            publishable.add((r["owner"], r["repo"], r["sha"]))
    print(f"\nCurrently publishable (Daytona success, max_speedup>=1.05): {len(publishable)}")

    flagged_keys = {(c["owner"], c["repo"], c["sha"]) for c, _ in flagged}
    flip = publishable & flagged_keys
    print(f"\nFlip count (publishable AND flagged): {len(flip)}")
    for k in sorted(flip)[:20]:
        print(f"  - {k[0]}/{k[1]}@{k[2][:8]}")


if __name__ == "__main__":
    main()
