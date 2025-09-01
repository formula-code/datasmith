import argparse
from pathlib import Path

import pandas as pd

from datasmith.agents.config import configure_agent_backends
from datasmith.agents.perf_judge import PerfClassifier
from datasmith.execution.collect_commits_offline import batch_classify_commits
from datasmith.logging_config import configure_logging

configure_agent_backends(local=True)

# logger = configure_logging(level=10, stream=open(__file__ + ".log", "a"))
logger = configure_logging()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Collect perf-related commits",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--commits", type=Path, required=True, help="Path to a JSONL file containing commit information.")
    p.add_argument("--outfile", type=Path, required=True, help="Path to save the filtered commits JSONL file.")
    p.add_argument("--max-workers", type=int, default=-1, help="Number of parallel workers. -1 = sequential.")
    return p.parse_args()


def main(args: argparse.Namespace) -> None:
    if args.commits.suffix == ".parquet":
        df = pd.read_parquet(args.commits).sort_values("stars", ascending=False)
    else:
        df = pd.read_json(args.commits, lines=True).sort_values("stars", ascending=False)
    filtered_df = df.copy(deep=True)
    perf_classifier = PerfClassifier()
    all_shas = set()
    for repo_name, group in df.groupby("repo_name"):
        assert isinstance(repo_name, str), f"Unexpected repo_name type: {type(repo_name)}"  # noqa: S101
        logger.info(f"Processing {repo_name} with {len(group)} commits.")
        commits = [(row["sha"], row["message"], row.get("file_change_summary", "")) for _, row in group.iterrows()]
        merge_shas = batch_classify_commits(perf_classifier, repo_name, commits, args.max_workers)
        logger.info(f"Found {len(merge_shas)} perf-related commits in {repo_name}.")
        all_shas.update(merge_shas)

    filtered_df = filtered_df[filtered_df["sha"].isin(all_shas)].reset_index(drop=True)
    logger.info(f"Filtered down to {len(filtered_df)} commits from {len(df)} total commits.")
    filtered_df.to_json(args.outfile, lines=True, orient="records")


if __name__ == "__main__":
    args = parse_args()
    main(args)
