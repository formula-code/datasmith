"""Backfill: identify and remove tainted candidate_containers.

A container is "tainted" if any of its build scripts:
  - reference /logs/summary_*.json or /logs/test_results.json (forged validator output)
  - inject a test_*.py file at the repo root (heredoc / git commit)
  - contain an inline `def test_x(): assert True` body

Removing the row makes the PR eligible for re-synthesis on the next stage-6 run.
The harbor_runs FK is ON DELETE CASCADE, so any (currently nonexistent) downstream
rows are removed too.

Usage:
  uv run python scripts/backfill_tainted_containers.py            # dry run
  uv run python scripts/backfill_tainted_containers.py --apply    # actually delete

Always writes a backup of the deleted rows to backups/tainted_containers_<ts>.json.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv

from datasmith.utils.db import fetch_all, get_client

load_dotenv("tokens.env")

SCRIPT_COLUMNS = (
    "dockerfile",
    "build_base_sh",
    "build_env_sh",
    "build_pkg_sh",
    "build_run_sh",
    "build_final_sh",
    "run_tests_sh",
    "entrypoint_sh",
    "profile_sh",
)

FORGED_LOGS_RE = re.compile(r"/logs/(summary_[\w${}]*\.json|test_results\.json|postrun_agent[\w.\-:*${}]*\.tar\.gz)")
ROOT_TEST_HEREDOC_RE = re.compile(
    r"""(?xm)
    (?:cat|tee|printf|echo)
    [^\n]*?>\s*["']?
    (?:\$\{?REPO_ROOT\}?/|\$\{?ROOT_PATH\}?/|/workspace/repo/|\./|\$PWD/)?
    test_[\w./-]*\.py
    """
)
# Test file write ANYWHERE in the tree (not just root)
ANY_TEST_FILE_WRITE_RE = re.compile(
    r"""(?xm)
    (?:cat|tee|printf|echo)
    [^\n]*?>\s*["']?
    [\w./$@{}\\-]*?(?:test_[\w.-]+|_test)\.py
    """
)
PY_TEST_FILE_WRITE_RE = re.compile(
    r"""open\(\s*["'][^"']*(?:test_[\w.-]+|_test)\.py["']\s*,\s*["']w""",
)
DOCKERFILE_ADD_ROOT_TEST_RE = re.compile(r"^\s*(?:ADD|COPY)\s+test_[\w]+\.py\s", re.MULTILINE)
TRIVIAL_ASSERT_RE = re.compile(r"def\s+test_\w+\s*\([^)]*\)\s*:\s*\n\s*(pass|assert\s+True)\b")
# Override of T_BENCH_AGENT_MODEL_NAME to anything other than the literal "agent"
T_BENCH_OVERRIDE_RE = re.compile(
    r"""(?m)
    ^\s*(?:export\s+)?T_BENCH_AGENT_MODEL_NAME\s*=\s*
    ["']?(?!agent["'\s]|\$\{?T_BENCH)
    ([\w:.\-]{1,40})
    """,
    re.VERBOSE,
)
# tmux binary shim — agent renames real tmux and writes a wrapper script
TMUX_SHIM_RE = re.compile(
    r"""(?xs)
    (?:
        mv\s+["']?[^"'\s]*tmux["']?\s+["']?[^"'\s]*tmux\.real
      | cat\s*>\s*["']?[^"'\s]*tmux["']?\s*<<
      | tee\s+["']?[^"'\s]*/tmux["']?
    )
    """
)
# Hardcoded passing-summary JSON literals
HARDCODED_PASSED_RE = re.compile(r'"passed"\s*:\s*[1-9]')
# Append (>>) to a tracked test file: the `# codex-smoke-anchor` pattern
APPEND_TO_TEST_FILE_RE = re.compile(
    r""">>[^\n]*(?:test_[\w.-]+|_test)\.py""",
)
# Pre-baked ASV result tarballs
PREBAKED_TARBALL_RE = re.compile(r"postrun_agent[\w.\-:${}*]*\.tar\.gz")


def script_blob(row: dict) -> str:
    return "\n".join((row.get(k) or "") for k in SCRIPT_COLUMNS)


def classify(row: dict) -> set[str]:  # noqa: C901
    tags: set[str] = set()
    blob = script_blob(row)
    if FORGED_LOGS_RE.search(blob):
        tags.add("forged_logs_json")
    if ROOT_TEST_HEREDOC_RE.search(blob):
        tags.add("root_test_heredoc")
    if ANY_TEST_FILE_WRITE_RE.search(blob):
        tags.add("any_test_file_write")
    if PY_TEST_FILE_WRITE_RE.search(blob):
        tags.add("py_test_file_write")
    if DOCKERFILE_ADD_ROOT_TEST_RE.search(row.get("dockerfile") or ""):
        tags.add("dockerfile_root_test")
    if TRIVIAL_ASSERT_RE.search(blob):
        tags.add("trivial_assert_inline")
    if T_BENCH_OVERRIDE_RE.search(blob):
        tags.add("t_bench_override")
    if TMUX_SHIM_RE.search(blob):
        tags.add("tmux_shim")
    if HARDCODED_PASSED_RE.search(blob):
        tags.add("hardcoded_passed")
    if APPEND_TO_TEST_FILE_RE.search(blob):
        tags.add("append_to_test_file")
    if PREBAKED_TARBALL_RE.search(blob):
        tags.add("prebaked_postrun_tarball")
    return tags


def main() -> None:  # noqa: C901
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Actually delete (default: dry run)")
    args = parser.parse_args()

    print("Fetching candidate_containers...")
    select_cols = ",".join(("owner", "repo", "sha", "issue_number", *SCRIPT_COLUMNS))
    rows = fetch_all("candidate_containers", select=select_cols)
    print(f"  {len(rows)} candidate_containers")

    tainted: list[tuple[dict, set[str]]] = []
    tag_counts: Counter[str] = Counter()
    by_repo: Counter[str] = Counter()
    for r in rows:
        tags = classify(r)
        if tags:
            tainted.append((r, tags))
            for t in tags:
                tag_counts[t] += 1
            by_repo[f"{r['owner']}/{r['repo']}"] += 1

    print(f"\nTainted: {len(tainted)} / {len(rows)} ({100 * len(tainted) / max(1, len(rows)):.1f}%)")
    print("Tag breakdown:")
    for tag, count in tag_counts.most_common():
        print(f"  {tag:>30}: {count}")
    print("Top repos:")
    for repo, count in by_repo.most_common(15):
        print(f"  {repo:>40}: {count}")

    if not tainted:
        print("Nothing to do.")
        return

    # Always write a backup, even on dry run
    Path("backups").mkdir(exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    backup_path = Path("backups") / f"tainted_containers_{ts}.json"
    backup_data = [{**r, "_tamper_tags": sorted(tags)} for r, tags in tainted]
    backup_path.write_text(json.dumps(backup_data, indent=2, default=str))
    print(f"\nBackup written: {backup_path} ({backup_path.stat().st_size // 1024} KB)")

    if not args.apply:
        print("\nDRY RUN — no rows deleted. Re-run with --apply to actually delete.")
        return

    print(f"\nDeleting {len(tainted)} rows...")
    client = get_client()
    deleted = 0
    failures: list[tuple[tuple, str]] = []
    for r, _ in tainted:
        key = (r["owner"], r["repo"], r["sha"])
        try:
            (
                client.table("candidate_containers")
                .delete()
                .eq("owner", key[0])
                .eq("repo", key[1])
                .eq("sha", key[2])
                .execute()
            )
            deleted += 1
        except Exception as e:
            failures.append((key, str(e)))
    print(f"Deleted {deleted}/{len(tainted)} ({len(failures)} failures)")
    for key, err in failures[:10]:
        print(f"  FAIL {key}: {err}")
    print("\nThese PRs are now eligible for re-synthesis on the next stage-6 run.")
    print(f"Backup retained at: {backup_path}")


if __name__ == "__main__":
    main()
