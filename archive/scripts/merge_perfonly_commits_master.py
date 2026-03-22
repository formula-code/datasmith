#!/usr/bin/env python
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from datasmith.logging_config import configure_logging

logger = configure_logging()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Merge perfonly parquet outputs into a master parquet file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--new-perfonly",
        type=Path,
        required=True,
        help="Path to the new perfonly parquet file to merge.",
    )
    p.add_argument(
        "--master",
        type=Path,
        default=None,
        help="Path to master parquet file. Defaults to perfonly_commits_master.parquet alongside new file.",
    )
    p.add_argument(
        "--dedupe-columns",
        type=str,
        default="pr_merge_commit_sha,repo_name,pr_base_sha",
        help="Comma-separated columns used as a primary key for de-duplication.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Log merge details without writing the master parquet file.",
    )
    return p.parse_args()


def _merge_columns(existing: list[str], incoming: list[str]) -> list[str]:
    columns = list(existing)
    for col in incoming:
        if col not in columns:
            columns.append(col)
    return columns


def _parse_dedupe_columns(raw: str) -> list[str]:
    return [col.strip() for col in raw.split(",") if col.strip()]


def main(args: argparse.Namespace) -> int:
    new_path = args.new_perfonly
    if not new_path.exists():
        logger.error("Perfonly parquet not found: %s", new_path)
        return 1

    master_path = args.master or new_path.parent / "perfonly_commits_master.parquet"
    dedupe_columns = _parse_dedupe_columns(args.dedupe_columns)
    if not dedupe_columns:
        logger.error("No de-duplication columns provided.")
        return 1

    new_df = pd.read_parquet(new_path)

    if master_path.exists():
        master_df = pd.read_parquet(master_path)
        columns = _merge_columns(list(master_df.columns), list(new_df.columns))
        # Align column sets before concatenation.
        master_df = master_df.reindex(columns=columns)
        new_df = new_df.reindex(columns=columns)
        combined = pd.concat([master_df, new_df], ignore_index=True)
        existing_rows = len(master_df)
    else:
        combined = new_df
        existing_rows = 0

    missing = [col for col in dedupe_columns if col not in combined.columns]
    if missing:
        logger.error("De-duplication columns missing in data: %s", ", ".join(missing))
        return 1

    before = len(combined)
    combined = combined.drop_duplicates(subset=dedupe_columns, keep="last").reset_index(drop=True)
    after = len(combined)

    logger.info("New perfonly file: %s", new_path)
    logger.info("Master parquet: %s", master_path)
    logger.info("Existing master rows: %d", existing_rows)
    logger.info("Combined rows before de-duplication: %d", before)
    logger.info("Combined rows after de-duplication: %d", after)

    if args.dry_run:
        logger.info("Dry run enabled: skipping write.")
        return 0

    tmp_path = master_path.with_suffix(".tmp.parquet")
    combined.to_parquet(tmp_path, index=False)
    tmp_path.replace(master_path)
    logger.info("Wrote master parquet to %s", master_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(parse_args()))
