from __future__ import annotations

import argparse
import asyncio
import re
import sys

from datasmith.utils import get_logger

logger = get_logger("update.cli")

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="ds-update",
        description="Run the FormulaCode update pipeline",
    )
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--resume", action="store_true", help="Resume from last completed stage")
    parser.add_argument("--stage", type=int, default=None, help="Run only this stage (1-5)")
    parser.add_argument("--dry-run", action="store_true", help="Log what would happen without executing")
    parser.add_argument("--n-concurrent", type=int, default=None, help="Max concurrent items per runner")
    return parser.parse_args(argv)


def validate_dates(args: argparse.Namespace) -> None:
    for name in ("start_date", "end_date"):
        val = getattr(args, name)
        if not _DATE_RE.match(val):
            print(f"Error: {name.replace('_', '-')} must be YYYY-MM-DD, got '{val}'", file=sys.stderr)
            sys.exit(1)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    validate_dates(args)

    from datasmith.update.pipeline import Pipeline

    pipeline = Pipeline(dry_run=args.dry_run, n_concurrent=args.n_concurrent)
    asyncio.run(
        pipeline.run(
            start_date=args.start_date,
            end_date=args.end_date,
            resume=args.resume,
            stage=args.stage,
        )
    )


if __name__ == "__main__":
    main()
