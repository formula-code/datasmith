#!/usr/bin/env python
"""
Orchestration script for updating FormulaCode dataset.

This script runs the full pipeline for a given date range:
1. Collect commits from repositories
2. Filter commits
3. Prepare commits with patches
4. Classify performance commits
5. Synthesize Docker contexts
6. Build and publish to DockerHub

Usage:
    python scratch/scripts/update_formulacode.py \
        --start-date 2025-10-01 \
        --end-date 2025-11-01 \
        [--dockerhub-namespace DOCKERHUB_NAMESPACE] \
        [--skip-existing] \
        [--max-workers 32] \
        [--dry-run]
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from datasmith import setup_environment
from datasmith.logging_config import configure_logging

# Set up environment (loads tokens.env)
setup_environment()

logger = configure_logging()

# Default artifact paths
ARTIFACTS_DIR = Path("scratch/artifacts/pipeflush")
SCRIPTS_DIR = Path("scratch/scripts")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="update_formulacode",
        description="Orchestrate the FormulaCode update pipeline for a given date range.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="Start date for commit collection (ISO format, e.g. 2025-10-01)",
    )
    p.add_argument(
        "--end-date",
        type=str,
        required=True,
        help="End date for commit collection (ISO format, e.g. 2025-11-01)",
    )
    p.add_argument(
        "--skip-existing",
        action="store_true",
        default=False,
        help="Skip existing artifacts where supported (pass-through to downstream scripts).",
    )
    p.add_argument(
        "--max-workers",
        type=int,
        default=32,
        help="Maximum number of parallel workers for processing stages.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Print commands without executing them.",
    )
    p.add_argument(
        "--repos",
        type=Path,
        default=ARTIFACTS_DIR / "repos_valid.csv",
        help="Path to the validated repositories CSV.",
    )
    p.add_argument(
        "--context-registry",
        type=Path,
        default=ARTIFACTS_DIR / "context_registry_final_filtered.json",
        help="Path to the context registry JSON.",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=ARTIFACTS_DIR,
        help="Directory for output artifacts.",
    )
    p.add_argument(
        "--dockerhub-namespace",
        type=str,
        default=os.environ.get("DOCKERHUB_NAMESPACE"),
        help="DockerHub namespace (username or org). Defaults to DOCKERHUB_NAMESPACE env var.",
    )
    return p.parse_args()


def validate_date(date_str: str, name: str) -> None:
    """Validate that a date string is in ISO format."""
    try:
        datetime.fromisoformat(date_str)
    except ValueError as e:
        logger.exception("Invalid %s date format: %s (expected ISO format like 2025-01-01)", name, date_str)
        raise SystemExit(1) from e


def run_command(cmd: list[str], dry_run: bool = False, step_name: str = "") -> int:
    """Run a command and return the exit code."""
    cmd_str = " ".join(cmd)

    if dry_run:
        logger.info("[DRY-RUN] %s: %s", step_name, cmd_str)
        return 0

    logger.info("[RUNNING] %s: %s", step_name, cmd_str)
    try:
        result = subprocess.run(cmd, check=False)  # noqa: S603
    except Exception:
        logger.exception("[ERROR] %s failed with exception", step_name)
        return 1
    else:
        if result.returncode != 0:
            logger.error("[FAILED] %s exited with code %d", step_name, result.returncode)
        else:
            logger.info("[SUCCESS] %s completed", step_name)
        return result.returncode


def main(args: argparse.Namespace) -> int:
    """Run the FormulaCode update pipeline."""
    # Validate inputs
    validate_date(args.start_date, "start")
    validate_date(args.end_date, "end")

    if not args.repos.exists():
        logger.error("Repositories file not found: %s", args.repos)
        return 1

    if not args.dockerhub_namespace:
        logger.error("DockerHub namespace is required. Set DOCKERHUB_NAMESPACE or pass --dockerhub-namespace.")
        return 1

    # Ensure output directory exists
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Define output file paths with date suffix for this run
    date_suffix = f"{args.start_date}_to_{args.end_date}".replace("-", "")
    filtered_parquet = args.output_dir / f"merge_commits_filtered_{date_suffix}.parquet"
    prepared_parquet = args.output_dir / f"merge_commits_filtered_with_patch_{date_suffix}.parquet"
    perfonly_parquet = args.output_dir / f"perfonly_commits_{date_suffix}.parquet"
    perfonly_master_parquet = args.output_dir / "perfonly_commits_master.parquet"

    python = sys.executable
    exit_code = 0

    # Step 1: Collect and filter commits
    logger.info("=" * 60)
    logger.info("Step 1: Collecting and filtering commits from %s to %s", args.start_date, args.end_date)
    logger.info("=" * 60)
    cmd = [
        python,
        str(SCRIPTS_DIR / "collect_and_filter_commits.py"),
        "--filtered-benchmarks-pth",
        str(args.repos),
        "--output-pth",
        str(filtered_parquet),
        "--threads",
        str(args.max_workers),
        "--procs",
        str(args.max_workers),
        "--since",
        args.start_date,
        "--until",
        args.end_date,
    ]
    exit_code = run_command(cmd, args.dry_run, "collect_and_filter_commits")
    if exit_code != 0:
        return exit_code

    # Step 2: Prepare commits with patches
    logger.info("=" * 60)
    logger.info("Step 2: Preparing commits with patches")
    logger.info("=" * 60)
    cmd = [
        python,
        str(SCRIPTS_DIR / "prepare_commits_for_building_reports.py"),
        "--input",
        str(filtered_parquet),
        "--output",
        str(prepared_parquet),
        "--max-workers",
        str(args.max_workers),
        "--fetch-patches",
    ]
    exit_code = run_command(cmd, args.dry_run, "prepare_commits_for_building_reports")
    if exit_code != 0:
        return exit_code

    # Step 3: Classify performance commits
    logger.info("=" * 60)
    logger.info("Step 3: Classifying performance commits")
    logger.info("=" * 60)
    cmd = [
        python,
        str(SCRIPTS_DIR / "collect_perf_commits.py"),
        "--commits",
        str(prepared_parquet),
        "--outfile",
        str(perfonly_parquet.with_suffix("")),  # Script adds suffix
        "--max-workers",
        str(args.max_workers),
    ]
    exit_code = run_command(cmd, args.dry_run, "collect_perf_commits")
    if exit_code != 0:
        return exit_code

    # Step 4: Synthesize Docker contexts
    logger.info("=" * 60)
    logger.info("Step 4: Synthesizing Docker contexts")
    logger.info("=" * 60)
    cmd = [
        python,
        str(SCRIPTS_DIR / "synthesize_contexts.py"),
        "--commits",
        str(perfonly_parquet),
        "--output-dir",
        str(args.output_dir / "results_synthesis"),
        "--context-registry",
        str(args.context_registry),
        "--max-workers",
        str(args.max_workers),
    ]
    if args.skip_existing:
        cmd.append("--ignore-exhausted")
    exit_code = run_command(cmd, args.dry_run, "synthesize_contexts")
    if exit_code != 0:
        return exit_code

    # Step 5: Build and publish to ECR
    logger.info("=" * 60)
    logger.info("Step 5: Building and publishing to ECR")
    logger.info("=" * 60)
    cmd = [
        python,
        str(SCRIPTS_DIR / "build_and_publish_to_ecr.py"),
        "--commits",
        str(perfonly_parquet),
        "--context-registry",
        str(args.context_registry),
        "--max-workers",
        "5",  # ECR has rate limits, keep this low
        "--skip-existing"
    ]
    if args.skip_existing:
        cmd.append("--skip-existing")
    exit_code = run_command(cmd, args.dry_run, "build_and_publish_to_ecr")
    if exit_code != 0:
        return exit_code

    # Step 6: Merge perfonly commits into master parquet
    logger.info("=" * 60)
    logger.info("Step 6: Merging perfonly commits into master parquet")
    logger.info("=" * 60)
    cmd = [
        python,
        str(SCRIPTS_DIR / "merge_perfonly_commits_master.py"),
        "--new-perfonly",
        str(perfonly_parquet),
        "--master",
        str(perfonly_master_parquet),
    ]
    exit_code = run_command(cmd, args.dry_run, "merge_perfonly_commits_master")
    if exit_code != 0:
        return exit_code

    logger.info("=" * 60)
    logger.info("FormulaCode update completed successfully!")
    logger.info("Date range: %s to %s", args.start_date, args.end_date)
    logger.info("Output directory: %s", args.output_dir)
    logger.info("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main(parse_args()))
