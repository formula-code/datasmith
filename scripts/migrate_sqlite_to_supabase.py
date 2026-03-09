#!/usr/bin/env python3
"""One-time migration from SQLite to Supabase.

Usage:
    python scripts/migrate_sqlite_to_supabase.py --pipeline-db pipeflush.db --cache-db cache.db
"""

from __future__ import annotations

import argparse
import contextlib
import json
import sqlite3
from pathlib import Path

from datasmith import setup_environment
from datasmith.utils import batch_upsert, get_client, get_logger

logger = get_logger("migrate")


def migrate_pipeline_db(db_path: str) -> int:
    """Migrate pipeline database tables to Supabase."""
    if not Path(db_path).exists():
        logger.warning("Pipeline DB not found: %s", db_path)
        return 0

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    total = 0

    # Get table list
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row["name"] for row in cursor.fetchall()]
    logger.info("Found tables in pipeline DB: %s", tables)

    # Migrate repositories if present
    if "repositories" in tables:
        cursor.execute("SELECT * FROM repositories")
        rows = [dict(row) for row in cursor.fetchall()]
        if rows:
            count = batch_upsert("repositories", rows)
            total += count
            logger.info("Migrated %d repository rows", count)

    # Migrate pull_requests if present
    if "pull_requests" in tables:
        cursor.execute("SELECT * FROM pull_requests")
        rows = []
        for row in cursor.fetchall():
            d = dict(row)
            # Convert JSON string fields
            for field in ("labels", "file_changes"):
                if field in d and isinstance(d[field], str):
                    with contextlib.suppress(json.JSONDecodeError, TypeError):
                        d[field] = json.loads(d[field])
            rows.append(d)
        if rows:
            count = batch_upsert("pull_requests", rows)
            total += count
            logger.info("Migrated %d pull_request rows", count)

    conn.close()
    return total


def migrate_cache_db(db_path: str) -> int:
    """Migrate cache database to Supabase hook_cache table."""
    if not Path(db_path).exists():
        logger.warning("Cache DB not found: %s", db_path)
        return 0

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    total = 0

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row["name"] for row in cursor.fetchall()]
    logger.info("Found tables in cache DB: %s", tables)

    for table_name in tables:
        if table_name.startswith("sqlite_"):
            continue
        cursor.execute(f"SELECT * FROM [{table_name}]")  # noqa: S608
        rows = []
        for row in cursor.fetchall():
            d = dict(row)
            # Map to hook_cache schema
            cache_row = {
                "entity_key": d.get("key", d.get("entity_key", "unknown")),
                "hook_name": table_name,
                "args_hash": d.get("args_hash", d.get("hash", "default")),
                "result_json": d.get("result", d.get("value", d.get("result_json", "{}"))),
            }
            # Parse result_json if it's a string
            if isinstance(cache_row["result_json"], str):
                try:
                    cache_row["result_json"] = json.loads(cache_row["result_json"])
                except (json.JSONDecodeError, TypeError):
                    cache_row["result_json"] = {"raw": cache_row["result_json"]}
            rows.append(cache_row)

        if rows:
            count = batch_upsert("hook_cache", rows)
            total += count
            logger.info("Migrated %d cache rows from table '%s'", count, table_name)

    conn.close()
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate SQLite data to Supabase")
    parser.add_argument("--pipeline-db", default="pipeflush.db", help="Path to pipeline SQLite DB")
    parser.add_argument("--cache-db", default="cache.db", help="Path to cache SQLite DB")
    parser.add_argument("--dry-run", action="store_true", help="Only show what would be migrated")
    args = parser.parse_args()

    setup_environment()

    if args.dry_run:
        logger.info("DRY RUN — no data will be written")
        for db_path in (args.pipeline_db, args.cache_db):
            if Path(db_path).exists():
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cursor.fetchall()]
                for table in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM [{table}]")  # noqa: S608
                    count = cursor.fetchone()[0]
                    logger.info("  %s: %s — %d rows", db_path, table, count)
                conn.close()
        return

    total = 0
    total += migrate_pipeline_db(args.pipeline_db)
    total += migrate_cache_db(args.cache_db)

    logger.info("Migration complete. Total rows migrated: %d", total)

    # Verify counts
    client = get_client()
    for table in ("repositories", "pull_requests", "hook_cache"):
        resp = client.table(table).select("*", count="exact").execute()
        logger.info("Supabase %s: %d rows", table, len(resp.data))


if __name__ == "__main__":
    main()
