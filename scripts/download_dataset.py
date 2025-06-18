import argparse

import pandas as pd

from datasmith.scraping.scrape_dashboards import scrape_public_dashboard


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download publicly available asv benchmarks.")
    parser.add_argument(
        "--dashboards",
        type=str,
        help="A JSON Lines file containing benchmark urls and corresponding output directories",
    )
    parser.add_argument("--force", action="store_true", help="Force re-download of files.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    dashboards = pd.read_json(args.dashboards, lines=True)

    for _, row in dashboards.iterrows():
        scrape_public_dashboard(base_url=row["url"], dl_dir=row["output_dir"], force=args.force)
        print(f"Data downloaded to {row['output_dir']}")
