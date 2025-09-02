import argparse

import pandas as pd

from datasmith.execution.collect_commits_offline import find_perf_commits, find_tagged_releases
from datasmith.logging_config import configure_logging

# from datasmith.execution.collect_commits import search_commits
# logger = configure_logging(
#     level=logging.DEBUG,
#     stream=open(__file__ + ".log", "a"),
# )
logger = configure_logging()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Scrape repositories that might be using asv.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--repos",
        required=True,
        help="Location of the repos csv that has a column `url` with GitHub repository URLs",
    )
    p.add_argument(
        "--outfile",
        required=True,
        help="Destination JSONL file to save the collected commits",
    )
    p.add_argument("-q", "--query", default="state=closed&sort=popularity&direction=desc", help="Pull request query")
    p.add_argument("--per-page", type=int, default=100, help="Items per page (max 100)")
    p.add_argument(
        "--max-pages",
        type=int,
        default=10,
        help="Stop after this many pages (API caps at 1 000 results)",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    repos = pd.read_csv(args.repos)
    urls = repos["url"]
    repo_names = repos["repo_name"]

    idx = 0
    all_commits = []
    for repo_name, url in zip(repo_names, urls):
        logger.info("Collecting commits for %s (repo_name: %s)", url, repo_name)
        perf_commits = find_perf_commits(
            repo_name=repo_name,
            n_workers=-1,
        )
        tagged_commits = find_tagged_releases(repo_name=repo_name)
        # parent_commits = find_parent_commits(repo_name=repo_name, commits=perf_commits + tagged_commits)
        commits = list(set(perf_commits + tagged_commits))
        for i, commit in enumerate(commits, 1):
            commit_id = f"{repo_name}_{i}"
            all_commits.append({
                "idx": idx,
                "commit_id": commit_id,
                "repo_name": repo_name,
                "commit_sha": commit,
            })
            idx += 1

    # Save as jsonl
    with open(args.outfile, "w", encoding="utf-8") as f:
        for commit in all_commits:
            f.write(f"{commit}\n")
    logger.info("Collected %d commits from %d repositories", len(all_commits), len(urls))
