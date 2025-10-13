#!/usr/bin/env python3
"""CLI tool for migrating DataSmith pipeline data to SQLite."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

from datasmith.logging_config import configure_logging
from datasmith.storage import DataSmithDB
from datasmith.storage.migration import PipelineMigrator

logger = configure_logging()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate DataSmith pipeline data to SQLite",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "--database",
        type=str,
        default="scratch/artifacts/datasmith.db",
        help="Path to SQLite database file"
    )
    
    # Migration sources
    parser.add_argument(
        "--repos-csv",
        type=str,
        help="Path to repos_valid.csv or repos_discovered.csv"
    )
    parser.add_argument(
        "--commits-jsonl",
        type=str,
        help="Path to commits_filtered.jsonl or commits_all.jsonl"
    )
    parser.add_argument(
        "--context-registry",
        type=str,
        help="Path to context_registry.json"
    )
    parser.add_argument(
        "--benchmark-pkl",
        type=str,
        help="Path to dashboard.fc.pkl file"
    )
    
    # Migration options
    parser.add_argument(
        "--max-commits",
        type=int,
        help="Maximum number of commits to migrate (for testing)"
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate migration by comparing counts"
    )
    parser.add_argument(
        "--export-legacy",
        type=str,
        help="Export database data to legacy format in this directory"
    )
    parser.add_argument(
        "--repository-id",
        type=int,
        help="Repository ID for legacy export"
    )
    
    # Actions
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Initialize database schema"
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show database statistics"
    )
    parser.add_argument(
        "--vacuum",
        action="store_true",
        help="Optimize database (VACUUM)"
    )
    
    return parser.parse_args()


def migrate_repositories(migrator: PipelineMigrator, csv_path: str) -> None:
    """Migrate repositories from CSV."""
    logger.info(f"Migrating repositories from {csv_path}")
    count = migrator.migrate_repositories(Path(csv_path))
    logger.info(f"Successfully migrated {count} repositories")


def migrate_commits(migrator: PipelineMigrator, jsonl_path: str, max_commits: Optional[int]) -> None:
    """Migrate commits from JSONL."""
    logger.info(f"Migrating commits from {jsonl_path}")
    count = migrator.migrate_commits(Path(jsonl_path), max_commits)
    logger.info(f"Successfully migrated {count} commits")


def migrate_contexts(migrator: PipelineMigrator, registry_path: str) -> None:
    """Migrate build contexts from JSON registry."""
    logger.info(f"Migrating build contexts from {registry_path}")
    count = migrator.migrate_context_registry(Path(registry_path))
    logger.info(f"Successfully migrated {count} build contexts")


def migrate_benchmarks(migrator: PipelineMigrator, pkl_path: str) -> None:
    """Migrate benchmark collection from pickle."""
    logger.info(f"Migrating benchmark collection from {pkl_path}")
    count = migrator.migrate_benchmark_collection(Path(pkl_path))
    logger.info(f"Successfully migrated benchmark collection with {count} runs")


def validate_migration(migrator: PipelineMigrator, args: argparse.Namespace) -> None:
    """Validate migration results."""
    logger.info("Validating migration...")
    
    original_files = {}
    if args.repos_csv:
        original_files['repositories_csv'] = Path(args.repos_csv)
    if args.commits_jsonl:
        original_files['commits_jsonl'] = Path(args.commits_jsonl)
    
    results = migrator.validate_migration(original_files)
    
    if results['validation_passed']:
        logger.info("✅ Migration validation PASSED")
    else:
        logger.error("❌ Migration validation FAILED")
        for error in results['errors']:
            logger.error(f"  {error}")
    
    # Print detailed results
    for item_type, details in results['details'].items():
        if isinstance(details, dict) and 'original_count' in details:
            status = "✅" if details['match'] else "❌"
            logger.info(f"{status} {item_type}: {details['original_count']} → {details['migrated_count']}")
        elif isinstance(details, dict):
            logger.info(f"📊 {item_type}: {details}")


def export_legacy_format(migrator: PipelineMigrator, output_dir: str, repository_id: int) -> None:
    """Export database data to legacy format."""
    logger.info(f"Exporting legacy format to {output_dir}")
    migrator.export_legacy_format(Path(output_dir), repository_id)
    logger.info("Legacy format export completed")


def show_stats(db: DataSmithDB) -> None:
    """Show database statistics."""
    stats = db.get_stats()
    
    logger.info("📊 Database Statistics:")
    logger.info(f"  Database file: {db.db_path}")
    logger.info(f"  File size: {stats.get('db_size_mb', 0)} MB")
    logger.info("")
    
    # Table counts
    table_counts = {
        'Repositories': stats.get('repositories_count', 0),
        'Commits': stats.get('commits_count', 0),
        'Build Contexts': stats.get('build_contexts_count', 0),
        'Benchmark Collections': stats.get('benchmark_collections_count', 0),
        'Benchmark Runs': stats.get('benchmark_runs_count', 0),
        'Benchmark Summaries': stats.get('benchmark_summaries_count', 0),
        'Breakpoints': stats.get('breakpoints_count', 0),
        'Pipeline Runs': stats.get('pipeline_runs_count', 0),
        'Pipeline Run Items': stats.get('pipeline_run_items_count', 0)
    }
    
    for table_name, count in table_counts.items():
        logger.info(f"  {table_name}: {count:,}")


def main() -> None:
    """Main CLI entry point."""
    args = parse_args()
    
    # Initialize database
    db = None
    try:
        db = DataSmithDB(args.database)
        migrator = PipelineMigrator(db)
        
        if args.init_db:
            logger.info(f"Database initialized at {args.database}")
        
        # Show stats
        if args.stats:
            show_stats(db)
            return
        
        # Vacuum database
        if args.vacuum:
            logger.info("Optimizing database...")
            db.vacuum()
            logger.info("Database optimization completed")
            return
        
        # Export legacy format
        # Export legacy format
        if args.export_legacy:
            if not args.repository_id:
                logger.error("--repository-id required for legacy export")
                sys.exit(1)
            logger.info("Legacy export not yet implemented")
            return
        
        # Perform migrations
        migration_performed = False
        
        if args.repos_csv:
            migrate_repositories(migrator, args.repos_csv)
            migration_performed = True
        
        if args.commits_jsonl:
            migrate_commits(migrator, args.commits_jsonl, args.max_commits)
            migration_performed = True
        
        if args.context_registry:
            logger.info("Context registry migration not yet implemented")
        
        if args.benchmark_pkl:
            logger.info("Benchmark collection migration not yet implemented")
        
        # Validate if requested
        if args.validate and migration_performed:
            logger.info("Migration validation not yet implemented")
        
        # Show final stats if any migration was performed
        if migration_performed:
            logger.info("")
            show_stats(db)
        
        if not migration_performed and not args.stats and not args.vacuum and not args.export_legacy:
            logger.info("No migration sources specified. Use --help for options.")
            logger.info("Available actions:")
            logger.info("  --repos-csv FILE        Migrate repositories from CSV")
            logger.info("  --commits-jsonl FILE     Migrate commits from JSONL")
            logger.info("  --context-registry FILE  Migrate build contexts from JSON")
            logger.info("  --benchmark-pkl FILE     Migrate benchmark data from pickle")
            logger.info("  --stats                  Show database statistics")
            logger.info("  --vacuum                 Optimize database")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)
    
    finally:
        if db:
            db.close()


if __name__ == "__main__":
    main()