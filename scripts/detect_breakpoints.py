import argparse
from pathlib import Path

from datasmith.detection.detect_breakpoints import detect_all_breakpoints
from datasmith.scrape.build_reports import build_reports_and_merge
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
        required=True,
        help=(
            "A benchmark dashboard directory containing all_summaries.csv, all_benchmarks.csv, and "
            "index.json (e.g. `downloads/astropy`)"
        ),
    )
    parser.add_argument(
        "--output",
        default=None,
        help="CSV for breakpoint output (default: <dataset>/breakpoints.csv)",
    )
    parser.add_argument(
        "--benchmarks-csv",
        default=None,
        help="Path to all_benchmarks.csv (defaults to <dataset>/all_benchmarks.csv).",
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
            "Method to use for detecting break-points. "
            "'asv' uses ASV's internal regression detection, while 'rbf' uses rupture's RBF kernel."
        ),
    )

    parser.add_argument(
        "--build-reports",
        action="store_true",
        help=(
            "Generate detailed GitHub commit reports (requires --    "
            "or an existing coverage.csv, plus github_scraper & tiktoken).",
        ),
    )
    return parser.parse_args()


def main() -> None:  # pragma: no cover - CLI glue
    args = parse_args()

    dataset_dir = Path(args.dataset)
    summary_csv = dataset_dir / "all_summaries.csv"
    index_json = dataset_dir / "index.json"

    if not summary_csv.exists():
        raise FileNotFoundError(summary_csv)

    if not index_json.exists():
        raise FileNotFoundError(index_json)

    output_dir = Path(args.output) if args.output else dataset_dir / "breakpoints"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Detecting break-points in '{summary_csv}' ...", flush=True)
    breakpoints = detect_all_breakpoints(summary_csv, method=args.method).dropna(subset=["hash"])
    print(f"Found {len(breakpoints):,} potential downward shifts.")

    cov_csv = output_dir / "coverage.csv"
    if args.compute_coverage:
        coverage_df = generate_coverage_dataframe(
            breakpoints,
            index_json=str(index_json),
            only=args.only,
        )
        coverage_df.to_csv(cov_csv, index=False)
        print(f"      Wrote coverage data to '{cov_csv}'.")

    if args.build_reports:
        print("Building GitHub commit reports and merged dataframe ...", flush=True)
        merged_df = build_reports_and_merge(
            breakpoints_df=breakpoints,
            coverage_df=None if not args.compute_coverage else coverage_df,
            index_json=index_json,
            reports_dir=output_dir / "reports",
        )
        merged_out = output_dir / "merged.csv"
        merged_df.to_csv(merged_out, index=False)
        print(f"      Wrote merged data to '{merged_out}'.")
        print(f"      Wrote reports to '{output_dir / 'reports'}'.")

    out_bp = output_dir / "breakpoints.csv"
    print("Writing breakpoint results ...", flush=True)
    breakpoints.to_csv(out_bp, index=False)
    print(f"Done. Break-points saved to '{out_bp}'.")


if __name__ == "__main__":
    main()
