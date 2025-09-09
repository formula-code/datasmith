from __future__ import annotations

import argparse
import json
from pathlib import Path

from datasmith.benchmark.collection import BenchmarkCollection
from datasmith.detection.detect_breakpoints import detect_all_breakpoints
from datasmith.execution.collect_commits_offline import get_offline_commit_info
from datasmith.logging_config import configure_logging
from datasmith.scrape.build_reports import breakpoints_scrape_comments
from datasmith.scrape.code_coverage import generate_coverage_dataframe
from datasmith.scrape.scrape_dashboards import get_taskname_from_index

# Configure logging for the script
logger = configure_logging()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="detect_breakpoints",
        description=(
            "Detect runtime drop break-points in ASV benchmark summaries and "
            "optionally enrich results with GitHub/Codecov metadata and full "
            "commit reports."
        ),
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        default=None,
        help=("A *.fc.pkl file that contains all the summaries, benchmarks and index.json."),
    )
    parser.add_argument(
        "--compute-core-changes",
        action="store_true",
        help="Flag whether each commit touches core code (needs GH_TOKEN).",
    )
    parser.add_argument(
        "--compute-coverage",
        action="store_true",
        help="Retrieve per-file line-coverage from Codecov for every commit.",
    )
    parser.add_argument(
        "--only",
        action="append",
        metavar="PAT",
        help="Restrict coverage queries to files whose paths contain PAT (repeatable).",
    )
    parser.add_argument(
        "--commit-urls-location",
        type=Path,
        default=None,
        help=(
            "Path to a JSON file containing default commit URLs when show_commit_url is '#'. "
            "If not provided, the script will not resolve '#' commit URLs and throw an error."
        ),
    )

    parser.add_argument(
        "--method",
        choices=["asv", "rbf"],
        default="rbf",
        help=(
            "Method to use for detecting break-points: "
            "'asv' = ASV's built-in regression detection, 'rbf' = ruptures RBF kernel."
        ),
    )

    parser.add_argument(
        "--build-reports",
        action="store_true",
        help=("Generate detailed GitHub commit reports"),
    )
    return parser.parse_args()


def main(args: argparse.Namespace) -> None:  # pragma: no cover - CLI glue
    if args.commit_urls_location is not None:
        with open(args.commit_urls_location, encoding="utf-8") as f:
            commit_urls_dict = json.load(f)
    else:
        commit_urls_dict = None

    dataset_path = args.dataset.expanduser().resolve()
    collection = BenchmarkCollection.load(dataset_path)
    task = get_taskname_from_index(collection.index_data)
    # save the collection with the new task info.
    collection.task = task
    collection.save(dataset_path)
    logger.info("Updated task info to %s/%s and saved.", task.owner, task.repo)
    logger.info("Loaded dataset from '%s'.", dataset_path)

    summary_df = collection.summaries
    breakpoints = detect_all_breakpoints(summary_df, method=args.method).dropna(subset=["hash"])
    collection.breakpoints = breakpoints
    logger.info("Found %s potential downward shifts.", f"{breakpoints['gt_hash'].nunique():,}")

    # get information about offline commits.
    repo_name = f"{collection.task.owner}/{collection.task.repo}"
    assert repo_name != "default/default", "Please provide a valid dataset with owner/repo info."  # noqa: S101
    commits = get_offline_commit_info(breakpoints, repo_name)
    if not commits.empty:
        collection.commits = commits
        collection.breakpoints = breakpoints[breakpoints["gt_hash"].isin(commits["sha"])]
        removed = breakpoints[~breakpoints["gt_hash"].isin(commits["sha"])]["gt_hash"].nunique()
        if removed > 0:
            logger.warning("Removed %s breakpoints whose gt_hash was filtered out by commits metadata.", removed)
            breakpoints = collection.breakpoints

    if args.compute_coverage:
        coverage_df = generate_coverage_dataframe(
            breakpoints,
            index_data=collection.index_data,
            commit_urls=commit_urls_dict,
            only=args.only,
        )
        collection.coverage = coverage_df
    else:
        coverage_df = None

    if args.build_reports:
        logger.info("Building GitHub commit reports and merged dataframe ...")
        new_breakpoints_df, comments_df = breakpoints_scrape_comments(
            breakpoints_df=breakpoints,
            coverage_df=coverage_df,
            index_data=collection.index_data,
        )
        collection.comments = comments_df
        collection.enriched_breakpoints = new_breakpoints_df

    # Save the collection.
    collection.save(dataset_path.parent / "breakpoints.fc.pkl")
    logger.info("Enriched breakpoints saved to '%s'.", dataset_path.parent / "breakpoints.fc.pkl")


if __name__ == "__main__":
    args = parse_args()

    if args.dataset is None:
        # find all datasets under scratch/artifacts/processed and run on them.
        processed_dir = Path("scratch") / "artifacts" / "processed"
        datasets = list(processed_dir.glob("**/dashboard.fc.pkl"))
        logger.info("No dataset provided, running on all datasets (%s found).", f"{len(datasets):,}")
        for dataset_path in datasets:
            args.dataset = dataset_path
            try:
                main(args)
            except Exception as e:
                print(e)
    else:
        main(args)
