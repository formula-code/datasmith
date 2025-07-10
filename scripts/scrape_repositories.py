"""
Python file to scrape relevant GitHub repositories that use asv.

This file will "search" for "asv.conf.json" using the GitHub API.
"""

import argparse
import os
import sys

import pandas as pd

from datasmith.scrape.detect_dashboards import scrape_github
from datasmith.scrape.filter_dashboards import filter_dashboards


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Scrape repositories that might be using asv.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    p.add_argument(
        "-q",
        "--query",
        default="filename:asv.conf.json",
        help="GitHub Search API query string",
    )
    p.add_argument("-o", "--outfile", default="repos.csv", help="Destination CSV file")
    p.add_argument(
        "--filtered-outfile",
        default="repos_filtered.csv",
        help="Destination CSV file for filtered repositories",
    )
    p.add_argument("--per-page", type=int, default=100, help="Items per page (max 100)")
    p.add_argument(
        "--max-pages",
        type=int,
        default=10,
        help="Stop after this many pages (API caps at 1 000 results)",
    )
    p.add_argument(
        "--base-delay",
        type=float,
        default=1.1,
        help="Seconds to wait between successful requests",
    )
    p.add_argument("--max-backoff", type=int, default=60, help="Maximum back-off delay in seconds")
    p.add_argument("--max-retries", type=int, default=6, help="Retry attempts when rate-limited")
    p.add_argument(
        "--jitter",
        type=float,
        default=0.3,
        help="Random extra delay (0-JITTER's) after each call",
    )
    return p.parse_args()


def main():
    args = parse_args()
    if not os.path.exists(args.outfile):
        scrape_github(
            query=args.query,
            outfile=args.outfile,
            search_args={
                "query": args.query,
                "max_pages": args.max_pages,
                "per_page": args.per_page,
                "base_delay": args.base_delay,
                "max_backoff": args.max_backoff,
                "max_retries": args.max_retries,
            },
        )
    else:
        sys.stderr.write(f"File {args.outfile} already exists. Skipping scraping.\n")

    df = pd.read_csv(args.outfile, header=None, names=["repo_name"])
    df["url"] = df.repo_name.apply(lambda x: f"https://github.com/{x}")

    filtered_df = filter_dashboards(df, url_col="url")
    # remove airspeed-velocity/asv
    filtered_df = filtered_df[filtered_df.repo_name != "airspeed-velocity/asv"]
    if filtered_df.empty:
        raise ValueError("No dashboards found in the repositories.")  # noqa: TRY003

    filtered_df.to_csv(args.filtered_outfile, index=False)
    sys.stderr.write(f"✅  Filtered dashboards saved to {args.filtered_outfile}\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.stderr.write("\n⏹️  Interrupted by user.\n")
