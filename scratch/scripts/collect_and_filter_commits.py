from __future__ import annotations

import argparse
import json
import tempfile
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import pandas as pd
from git import Repo
from tqdm.auto import tqdm

from datasmith.execution.collect_commits import collect_merge_shas
from datasmith.execution.utils import _get_commit_info_offline, clone_repo, find_file_in_tree, has_core_file
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
    p.add_argument(
        "--repo2mergepr",
        type=Path,
        default=None,
        help="Path to JSON file mapping repo names to their merge PRs (to avoid redundant GitHub calls).",
    )

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
    ret = _get_commit_info_offline(repo, sha)
    if ret is None:
        ret = _get_commit_info_offline(repo, sha, bypass_cache=True)
    return ret


def main() -> None:
    args = parse_args()
    repo2mergepr = None
    if args.repo2mergepr and args.repo2mergepr.exists():
        repo2mergepr = json.loads(args.repo2mergepr.read_text(encoding="utf-8"))

    benchmarks = pd.read_csv(args.filtered_benchmarks_pth)

    benchmarks = benchmarks.sort_values("stars", ascending=False, ignore_index=True).head(args.max_repos)

    if benchmarks.empty:
        # Nothing to do. create empty output to keep downstream happy.
        Path(args.output_pth).write_text("", encoding="utf-8")
        logger.warning("No repositories with asv.conf.json found. Exiting.")
        return

    all_repo_names = list(set(benchmarks["repo_name"]))

    # download all repos to a temp dir
    with tempfile.TemporaryDirectory(prefix="gh-repos-") as td:
        commit2kind = {}
        commit2repo = {}
        commit2pr = {}
        all_repos = {}
        commit_info_args: list[tuple[Repo, str]] = []
        with ThreadPoolExecutor(max_workers=args.threads) as tp:
            futures = {tp.submit(clone_repo, td, repo_name): repo_name for repo_name in all_repo_names}
            for f in tqdm(as_completed(futures), total=len(futures), desc="Cloning repos"):
                repo_name, repo = f.result()
                all_repos[repo_name] = repo
                merge_prs = repo2mergepr.get(repo_name, []) if repo2mergepr else collect_merge_shas(repo_name)
                commit2pr.update({pr.get("merge_commit_sha"): pr for pr in merge_prs})
                merge_shas = [pr.get("merge_commit_sha") for pr in merge_prs if pr.get("merge_commit_sha")]

                for commit_sha in set(merge_shas):
                    commit_info_args.append((repo, commit_sha))
                    commit2kind[commit_sha] = "commit"
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

    commits_meta["repo_name"] = commits_meta["sha"].map(commit2repo)
    commits_merged = commits_meta[commits_meta["files_changed"].apply(has_core_file)].reset_index(drop=True)
    # commits_merged["pr"] = commits_merged["sha"].map(commit2pr)
    # commit2pr returns a dict that is json-serializable, so we can expand it into multiple columns
    pr_expanded = commits_merged["sha"].map(commit2pr).apply(pd.Series)
    commits_merged = pd.concat([commits_merged, pr_expanded.add_prefix("pr_")], axis=1)

    out_path = Path(args.output_pth)
    if not out_path.parent.exists():
        out_path.parent.mkdir(parents=True, exist_ok=True)
    # save as a parquet file
    commits_merged.to_parquet(out_path, index=False)

    logger.info("✔ Wrote %s rows → %s", len(commits_merged), out_path)


if __name__ == "__main__":
    main()
