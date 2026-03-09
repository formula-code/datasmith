#!/usr/bin/env python
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from datasmith.core.storage import read_table, resolve_table_name, table_exists, write_table
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
    p.add_argument("--db", type=str, default=None, help="Pipeline SQLite DB path.")
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
    new_table = resolve_table_name(str(args.new_perfonly))
    master_table = (
        resolve_table_name(str(args.master))
        if args.master
        else "perfonly_commits_master"
    )
    dedupe_columns = _parse_dedupe_columns(args.dedupe_columns)
    if not dedupe_columns:
        logger.error("No de-duplication columns provided.")
        return 1

    if not table_exists(new_table, db_path=args.db):
        logger.error("Perfonly table not found: %s", new_table)
        return 1

    new_df = read_table(new_table, db_path=args.db)

    if table_exists(master_table, db_path=args.db):
        master_df = read_table(master_table, db_path=args.db)
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

    logger.info("New perfonly table: %s", new_table)
    logger.info("Master table: %s", master_table)
    logger.info("Existing master rows: %d", existing_rows)
    logger.info("Combined rows before de-duplication: %d", before)
    logger.info("Combined rows after de-duplication: %d", after)

    if args.dry_run:
        logger.info("Dry run enabled: skipping write.")
        return 0

    write_table(combined, master_table, db_path=args.db, if_exists="replace")
    logger.info("Wrote master table %s", master_table)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(parse_args()))
