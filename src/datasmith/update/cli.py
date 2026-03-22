from __future__ import annotations

import argparse
import asyncio
import re
import sys

from datasmith.utils import get_logger

logger = get_logger("update.cli")

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


_STAGE_DESCRIPTIONS = {
    1: "scrape_repos      — Fetch repository metadata from GitHub for all tracked repos",
    2: "scrape_commits    — Scrape merged PR commits and patches from each repository",
    3: "classify_prs      — Use LLM agents to classify PRs as performance-related",
    4: "resolve_packages  — Resolve Python dependencies for performance commits via uv",
    5: "synthesize_images — Generate Docker build contexts for confirmed performance commits",
    6: "publish           — Build, verify, and publish Docker images to DockerHub",
}


def _stages_epilog() -> str:
    lines = ["pipeline stages (run in order by default):"]
    for num, desc in _STAGE_DESCRIPTIONS.items():
        lines.append(f"  {num}. {desc}")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="ds-update",
        description="Run the FormulaCode update pipeline — discovers performance-improving "
        "commits from GitHub, classifies them with LLM agents, and builds Docker images "
        "for benchmark evaluation.",
        epilog=_stages_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--start-date", required=True, help="Start of the date range to scan for commits (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End of the date range to scan for commits (YYYY-MM-DD)")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip stages already marked complete and resume from the next pending stage",
    )
    parser.add_argument(
        "--stage",
        type=int,
        default=None,
        metavar="N",
        help="Run only stage N (1-6); see stage list below",
    )
    parser.add_argument("--dry-run", action="store_true", help="Log what each stage would do without executing")
    parser.add_argument(
        "--n-concurrent", type=int, default=None, metavar="N", help="Max concurrent items per runner stage"
    )
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
