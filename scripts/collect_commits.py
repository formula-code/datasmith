import argparse
import sys

import pandas as pd

from datasmith.execution.collect_commits import search_commits


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Scrape repositories that might be using asv.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--dashboards",
        required=True,
        help="Location of the dashboards csv that has a column `url` with GitHub repository URLs",
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
    dashboards = pd.read_csv(args.dashboards)
    urls = dashboards["url"]
    repo_names = dashboards["repo_name"]

    idx = 0
    all_commits = []
    for repo_name, url in zip(repo_names, urls):
        sys.stderr.write(f"Collecting commits for {url} (repo_name: {repo_name})\n")
        commits = search_commits(
            repo_name=repo_name,
            query=args.query,
            max_pages=args.max_pages,
            per_page=args.per_page,
        )
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
