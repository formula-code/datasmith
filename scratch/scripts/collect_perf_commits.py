import argparse
from pathlib import Path

import pandas as pd

from datasmith.agents.config import configure_agent_backends
from datasmith.agents.perf_judge import PerfClassifier
from datasmith.execution.collect_commits_offline import (
    batch_classify_commits,
    find_parent_releases,
    find_tagged_releases,
)
from datasmith.logging_config import configure_logging

configure_agent_backends(PORTKEY_MODEL_NAME="@togetherai/meta-llama/Llama-3.3-70B-Instruct-Turbo")

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
    df = pd.read_parquet(args.commits) if args.commits.suffix == ".parquet" else pd.read_json(args.commits, lines=True)
    filtered_df = df[
        (df["kind"] == "commit")
        & (
            (df["message"].str.lower().str.contains("#|gh-|pr|issue", regex=True))
            | (df["message"].str.lower().str.startswith(("perf", "enh", "speed", "fast", "slow", "benchmark")))
        )
    ].copy(deep=True)

    perf_classifier = PerfClassifier()
    all_shas = set()
    for repo_name, group in filtered_df.groupby("repo_name"):
        assert isinstance(repo_name, str), f"Unexpected repo_name type: {type(repo_name)}"  # noqa: S101
        logger.info(f"Processing {repo_name} with {len(group)} commits.")
        commits = [(row["sha"], row["message"], row.get("file_change_summary", "")) for _, row in group.iterrows()]
        logger.info(f"Classifying {len(commits)} commits in {repo_name}.")
        merge_shas = batch_classify_commits(perf_classifier, repo_name, commits, args.max_workers)
        logger.info(f"Found {len(merge_shas)} perf-related commits in {repo_name}.")
        all_shas.update(merge_shas)

        tagged_shas = find_tagged_releases(repo_name)
        all_shas.update(tagged_shas)

        parent_shas = find_parent_releases(repo_name, list(merge_shas) + tagged_shas, add_first=True)
        all_shas.update(parent_shas)

    all_df = df[df["sha"].isin(all_shas)].copy(deep=True)

    logger.info(f"Filtered down to {len(all_df)} commits from {len(df)} total commits.")
    with open(args.outfile.with_suffix(".txt"), "w") as f:
        for sha in sorted(all_shas):
            f.write(sha + "\n")
    all_df.to_parquet(args.outfile.with_suffix(".parquet"), index=False)


if __name__ == "__main__":
    args = parse_args()
    main(args)
