import argparse
from pathlib import Path

from datasmith.benchmark.collection import BenchmarkCollection
from datasmith.detection.detect_breakpoints import detect_all_breakpoints
from datasmith.scrape.build_reports import breakpoints_scrape_comments
from datasmith.scrape.code_coverage import generate_coverage_dataframe


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
        required=True,
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


def main() -> None:  # pragma: no cover - CLI glue
    args = parse_args()

    dataset_path = args.dataset.expanduser().resolve()
    collection = BenchmarkCollection.load(dataset_path)
    summary_df = collection.summaries
    breakpoints = detect_all_breakpoints(summary_df, method=args.method).dropna(subset=["hash"])
    collection.breakpoints = breakpoints
    print(f"Found {len(breakpoints):,} potential downward shifts.")

    if args.compute_coverage:
        coverage_df = generate_coverage_dataframe(
            breakpoints,
            index_data=collection.index_data,
            only=args.only,
        )
        collection.coverage = coverage_df

    if args.build_reports:
        print("Building GitHub commit reports and merged dataframe ...", flush=True)
        new_breakpoints_df, comments_df = breakpoints_scrape_comments(
            breakpoints_df=breakpoints,
            coverage_df=coverage_df,
            index_data=collection.index_data,
        )
        collection.comments = comments_df
        collection.enriched_breakpoints = new_breakpoints_df

    # Save the collection.
    collection.save(dataset_path.parent / "breakpoints.fc.pkl")
    print(f"Enriched breakpoints saved to '{dataset_path.parent / 'breakpoints.fc.pkl'}'.")


if __name__ == "__main__":
    main()
