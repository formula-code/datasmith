import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from datasmith.benchmark.collection import BenchmarkCollection
from datasmith.logging_config import configure_logging
from datasmith.scrape.scrape_dashboards import make_benchmark_from_html

# Configure logging for the script
logger = configure_logging()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download publicly available asv benchmarks.")
    parser.add_argument(
        "--dashboards",
        type=str,
        required=True,
        help="A JSON Lines file containing benchmark urls and corresponding output directories",
    )
    parser.add_argument("--force", action="store_true", help="Force re-download of files.")
    parser.add_argument(
        "--num_workers",
        type=int,
        default=0,
        help="Number of workers to use for parallel downloading (0 = sequential)",
    )
    return parser.parse_args()


def process_dashboard(row, force: bool) -> tuple[Path, BenchmarkCollection]:
    out_path = Path(row["output_dir"]) / "dashboard.fc.pkl"
    dashboard_collection: BenchmarkCollection | None = make_benchmark_from_html(
        base_url=row["url"], html_dir=row["output_dir"], force=force
    )
    if dashboard_collection is None:
        raise ValueError(f"Failed to create benchmark collection from {row['url']}")
    dashboard_collection.save(path=out_path)
    return out_path, dashboard_collection


def log_dashboard_results(row, out_path: Path, dashboard_collection: BenchmarkCollection) -> None:
    logger.info(
        "Saved %s benchmark rows and %s summary rows -> %s",
        f"{len(dashboard_collection.benchmarks):,}",
        f"{len(dashboard_collection.summaries):,}",
        out_path,
    )
    logger.info("Data downloaded to %s", row["output_dir"])


def run_sequential(dashboards: pd.DataFrame, force: bool) -> None:
    for _, row in dashboards.iterrows():
        out_path, dashboard_collection = process_dashboard(row, force)
        log_dashboard_results(row, out_path, dashboard_collection)


def run_parallel(dashboards: pd.DataFrame, force: bool, num_workers: int) -> None:
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(process_dashboard, row, force): row for _, row in dashboards.iterrows()}
        for future in as_completed(futures):
            row = futures[future]
            try:
                out_path, dashboard_collection = future.result()
                log_dashboard_results(row, out_path, dashboard_collection)
            except Exception:
                logger.exception("Failed to process %s: %s", row["url"])


def main() -> None:
    args = parse_args()
    dashboards = pd.read_json(args.dashboards, lines=True)

    if args.num_workers == 0:
        run_sequential(dashboards, args.force)
    else:
        run_parallel(dashboards, args.force, args.num_workers)


if __name__ == "__main__":
    main()
