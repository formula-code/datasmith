from __future__ import annotations

import argparse
import re
import tempfile
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd
from git import Repo
from tqdm.auto import tqdm

from datasmith.execution.collect_commits_offline import collect_commits
from datasmith.execution.utils import _get_commit_info_offline, find_file_in_tree
from datasmith.logging_config import configure_logging

# Configure logging for the script
logger = configure_logging()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Filter commits for ASV benchmarks (fast version).")

    p.add_argument("--filtered-benchmarks-pth", required=True, help="Path to the filtered benchmarks CSV file.")
    p.add_argument("--output-pth", required=True, help="Path to save the filtered commits CSV file.")
    p.add_argument(
        "--max-repos", type=int, default=150, help="Maximum number of repositories (sorted by stars) to consider."
    )
    p.add_argument("--procs", type=int, default=1, help="Number of processes for fetching commit metadata (CPU-bound).")

    # Optional knobs. keep defaults sensible
    p.add_argument("--threads", type=int, default=16, help="Worker threads for finding asv.conf.json (I/O-bound).")
    return p.parse_args()


def _asv_conf_worker(repo_name: str) -> list[str] | None:
    """Locate asv.conf.json inside a repo (wrapper for ThreadPool)."""
    return find_file_in_tree(repo_name, "asv.conf.json")


def _commit_info_worker(arg_tuple: tuple[Repo, str]) -> dict[str, Any] | None:
    """Wrapper for ProcessPool: arg_tuple = (repo_name, sha)."""
    repo, sha = arg_tuple
    # return _get_commit_info(repo, sha)
    return _get_commit_info_offline(repo, sha)


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
            # non-core pattern, we know the commit touched "core" code.
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
        logger.warning("No repositories with asv.conf.json found. Exiting.")
        return

    # with open(args.merged_commits_pth, encoding="utf-8") as f:
    #     commits = pd.DataFrame([json.loads(line.strip().replace("'", '"').replace("None", "null")) for line in f])

    # commits = commits.merge(benchmarks, how="right", on="repo_name")
    # commits = commits.dropna(subset=["commit_sha"])

    # all_repo_names = set(commits["repo_name"])
    all_repo_names = set(benchmarks["repo_name"])

    # download all repos to a temp dir
    with tempfile.TemporaryDirectory(prefix="gh-repos-") as td:

        def clone_repo(repo_name: str) -> tuple[str, Repo]:
            repo_name = repo_name.strip("/")
            owner, name = repo_name.split("/", 1)
            path = Path(td) / f"{owner}__{name}.git"
            repo = Repo.clone_from(
                f"https://github.com/{repo_name}.git",
                path,
                quiet=True,
                allow_unsafe_options=True,
                allow_unsafe_protocols=True,
            )
            logger.debug("Cloned repo %s to %s", repo_name, path)
            return repo_name, repo

        commit2kind = {}
        commit2repo = {}
        all_repos = {}
        commit_info_args: list[tuple[Repo, str]] = []
        with ThreadPoolExecutor(max_workers=args.threads) as tp:
            futures = {tp.submit(clone_repo, repo_name): repo_name for repo_name in all_repo_names}
            for f in tqdm(as_completed(futures), total=len(futures), desc="Cloning repos"):
                repo_name, repo = f.result()
                all_repos[repo_name] = repo
                kind_commit_shas = collect_commits(repo)
                for kind, commit_sha in kind_commit_shas:
                    commit_info_args.append((repo, commit_sha))
                    commit2kind[commit_sha] = kind
                    commit2repo[commit_sha] = repo_name

        if args.procs < 0:
            # sequential
            commit_info = list(
                tqdm(
                    map(_commit_info_worker, commit_info_args),
                    total=len(commit_info_args),
                    desc="Fetching commit metadata",
                )
            )
        else:
            with ProcessPoolExecutor(max_workers=args.procs) as pp:
                commit_info = list(
                    tqdm(
                        pp.map(_commit_info_worker, commit_info_args),
                        total=len(commit_info_args),
                        desc="Fetching commit metadata",
                    )
                )

        for k, repo in all_repos.items():
            repo.close()
            logger.debug("Closed repo %s", k)

    commits_meta = pd.json_normalize(commit_info)  # pyright: ignore[reportArgumentType]
    commits_meta = commits_meta[commits_meta["has_asv"]]  # Take out all commits that don't have asv installed.
    commits_meta["kind"] = commits_meta["sha"].map(commit2kind)
    commits_meta["repo_name"]

    commits_merged = commits_meta[commits_meta["files_changed"].apply(has_core_file)].reset_index(drop=True)
    commits_merged["repo_name"] = commits_merged["sha"].map(commit2repo)

    out_path = Path(args.output_pth)
    if not out_path.parent.exists():
        out_path.parent.mkdir(parents=True, exist_ok=True)
    # save as a parquet file
    commits_merged.to_parquet(out_path, index=False)

    logger.info("✔ Wrote %s rows → %s", len(commits_merged), out_path)


if __name__ == "__main__":
    main()
