#!/usr/bin/env python
"""One-time migration: import existing parquet files into the pipeline SQLite DB."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from datasmith.core.storage import get_pipeline_db, list_tables, path_to_table_name, table_exists, write_table
from datasmith.logging_config import configure_logging

logger = configure_logging()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Migrate existing parquet files into the pipeline SQLite DB.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--parquet-dir",
        type=Path,
        default=Path("scratch/artifacts/pipeflush"),
        help="Directory containing parquet files to migrate.",
    )
    p.add_argument(
        "--db",
        type=str,
        default=None,
        help="Pipeline SQLite DB path. Defaults to PIPELINE_DB env var.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Print table name mappings without writing anything.",
    )
    p.add_argument(
        "--skip-existing",
        action="store_true",
        default=False,
        help="Skip tables that already exist in the DB.",
    )
    p.add_argument(
        "--glob",
        type=str,
        default="*.parquet",
        help="Glob pattern for parquet files.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    db_path = args.db or get_pipeline_db()
    parquet_dir = args.parquet_dir

    if not parquet_dir.exists():
        logger.error("Parquet directory not found: %s", parquet_dir)
        return 1

    files = sorted(parquet_dir.glob(args.glob))
    if not files:
        logger.warning("No parquet files found matching %s in %s", args.glob, parquet_dir)
        return 0

    logger.info("Found %d parquet files in %s", len(files), parquet_dir)
    logger.info("Target DB: %s", db_path)

    if not args.dry_run:
        existing = set(list_tables(db_path))
    else:
        existing = set()

    migrated = 0
    skipped = 0
    for f in files:
        table_name = path_to_table_name(f)
        if args.dry_run:
            logger.info("[DRY-RUN] %s -> table '%s'", f.name, table_name)
            continue

        if args.skip_existing and table_name in existing:
            logger.info("[SKIP] Table '%s' already exists (from %s)", table_name, f.name)
            skipped += 1
            continue

        try:
            df = pd.read_parquet(f)
            write_table(df, table_name, db_path=db_path)
            logger.info("[OK] %s -> '%s' (%d rows, %d cols)", f.name, table_name, len(df), len(df.columns))
            migrated += 1
        except Exception:
            logger.exception("[ERROR] Failed to migrate %s", f.name)

    if not args.dry_run:
        logger.info("Migration complete: %d migrated, %d skipped", migrated, skipped)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
