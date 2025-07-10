import argparse
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
        help="A JSON Lines file containing benchmark urls and corresponding output directories",
    )
    parser.add_argument("--force", action="store_true", help="Force re-download of files.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dashboards = pd.read_json(args.dashboards, lines=True)

    for _, row in dashboards.iterrows():
        out_path = Path(row["output_dir"]) / "dashboard.fc.pkl"
        dashboard_collection: BenchmarkCollection = make_benchmark_from_html(
            base_url=row["url"], html_dir=row["output_dir"]
        )
        dashboard_collection.save(path=out_path)
        logger.info(
            "Saved %s benchmark rows and %s summary rows -> %s",
            f"{len(dashboard_collection.benchmarks):,}",
            f"{len(dashboard_collection.summaries):,}",
            out_path,
        )
        logger.info("Data downloaded to %s", row["output_dir"])


if __name__ == "__main__":
    main()
