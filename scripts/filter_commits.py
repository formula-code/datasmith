from __future__ import annotations

import argparse
import json
import re
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path

import pandas as pd
from tqdm.auto import tqdm

from datasmith.execution.utils import _get_commit_info, find_file_in_tree


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Filter commits for ASV benchmarks (fast version).")

    p.add_argument("--filtered-benchmarks-pth", required=True, help="Path to the filtered benchmarks CSV file.")
    p.add_argument("--merged-commits-pth", required=True, help="Path to the merged commits JSONL file.")
    p.add_argument("--output-pth", required=True, help="Path to save the filtered commits CSV file.")
    p.add_argument(
        "--max-repos", type=int, default=150, help="Maximum number of repositories (sorted by stars) to consider."
    )
    p.add_argument("--procs", type=int, default=1, help="Number of processes for fetching commit metadata (CPU-bound).")

    # Optional knobs. keep defaults sensible
    p.add_argument("--threads", type=int, default=16, help="Worker threads for finding asv.conf.json (I/O-bound).")
    return p.parse_args()


def _asv_conf_worker(repo_name: str) -> str | None:
    """Locate asv.conf.json inside a repo (wrapper for ThreadPool)."""
    return find_file_in_tree(repo_name, "asv.conf.json")


def _commit_info_worker(arg_tuple) -> dict | None:
    """Wrapper for ProcessPool: arg_tuple = (repo_name, sha)."""
    repo, sha = arg_tuple
    return _get_commit_info(repo, sha)


NON_CORE_PATTERNS = re.compile(
    r"""(
           (^|/)tests?(/|$)        |   # any tests/ directory
           (^|/)doc[s]?(/|$)       |   # docs/, doc/, documentation/
           (^|/)examples?(/|$)     |   # examples/
           (^|/)\.github(/|$)      |   # GitHub meta files
           (^|/)benchmarks?(/|$)   |   # benchmarks/
           (^|/)dist-info(/|$)     |   # wheel metadata
           (^|/)build(/|$)         |   # build artifacts
           (^|/)site-packages(/|$) |   # vendored wheels
           (^|/)__(init|pycache)__ |   # __init__.py, __pycache__
           (^|/)requirements-docs\.txt$|
           (^|/)pyproject\.toml$|
           (^|/)README\.md$        |
           \.rst$                  |   # reStructuredText docs
           \.md$                       # markdown docs
       )""",
    re.VERBOSE,
)


def has_core_file(files_changed: str) -> bool:
    """
    Return True if *any* path in the newline-separated `files_changed`
    string is judged to be a *core* file under the rules above.
    """
    for path in files_changed.split("\n"):
        path = path.strip()
        # Empty lines can show up if a commit touches a single file
        if not path:
            continue
        if not NON_CORE_PATTERNS.search(path):
            # As soon as we find one path that is NOT caught by the
            # non-core pattern, we know the commit touched “core” code.
            return True
    return False


def main() -> None:
    args = parse_args()

    benchmarks = pd.read_csv(args.filtered_benchmarks_pth)

    benchmarks = benchmarks.sort_values("stars", ascending=False, ignore_index=True).head(args.max_repos)

    with ThreadPoolExecutor(max_workers=args.threads) as tp:
        benchmarks["asv_conf_path"] = list(
            tqdm(tp.map(_asv_conf_worker, benchmarks["repo_name"]), total=len(benchmarks), desc="Scanning repos")
        )

    benchmarks = benchmarks.dropna(subset=["asv_conf_path"])

    if benchmarks.empty:
        # Nothing to do. create empty output to keep downstream happy.
        Path(args.output_pth).write_text("", encoding="utf-8")
        print("No repositories with asv.conf.json found. Exiting.")
        return

    with open(args.merged_commits_pth, encoding="utf-8") as f:
        commits = pd.DataFrame([json.loads(line.strip().replace("'", '"').replace("None", "null")) for line in f])

    commits = commits.merge(benchmarks, how="right", on="repo_name")
    commits = commits.dropna(subset=["commit_sha"])

    with ProcessPoolExecutor(max_workers=args.procs) as pp:
        commits["commit_info"] = list(
            tqdm(
                pp.map(_commit_info_worker, commits[["repo_name", "commit_sha"]].itertuples(index=False, name=None)),
                total=len(commits),
                desc="Fetching commit metadata",
            )
        )

    commit_meta = pd.json_normalize(commits.pop("commit_info"))
    commits = pd.concat([commits, commit_meta], axis=1)
    commits = commits.dropna(subset=["asv_conf_path", "sha", "date", "message"])
    commits = commits[commits["files_changed"].apply(has_core_file)].reset_index(drop=True)

    out_path = Path(args.output_pth)
    if not out_path.parent.exists():
        out_path.parent.mkdir(parents=True, exist_ok=True)
    # commits.to_csv(out_path, index=False)
    commits.to_json(out_path, orient="records", lines=True, index=False)

    print(f"✔ Wrote {len(commits):,} rows → {out_path}")


if __name__ == "__main__":
    main()
