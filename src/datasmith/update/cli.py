from __future__ import annotations

import argparse
import asyncio
import os
import re
import signal
import sys

from datasmith.utils import get_logger

logger = get_logger("update.cli")

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


_STAGE_DESCRIPTIONS = {
    1: "scrape_repos      — Fetch repository metadata from GitHub for all tracked repos",
    2: "scrape_commits    — Scrape merged PR commits and patches from each repository",
    3: "classify_prs      — Use LLM agents to classify PRs as performance-related",
    4: "resolve_packages  — Resolve Python dependencies for performance commits via uv",
    5: "render_problems   — Scrape linked issues and render deconstructed problem contexts",
    6: "synthesize_images — Generate Docker build contexts for confirmed performance commits",
    7: "publish           — Build, verify, and publish Docker images to DockerHub",
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
        action="append",
        metavar="N",
        help="Run only stage N (1-7); repeat to run multiple stages (e.g. --stage 1 --stage 2)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Log what each stage would do without executing")
    parser.add_argument(
        "--n-concurrent", type=int, default=None, metavar="N", help="Max concurrent items per runner stage"
    )
    parser.add_argument(
        "--tasks-per-repo",
        type=int,
        default=None,
        metavar="N",
        help="Max tasks per repo for stages 5 (render_problems) and 6 (synthesize_images). Ignored by other stages.",
    )
    parser.add_argument(
        "--agent",
        type=str,
        default=None,
        choices=["claude", "codex", "gemini", "none"],
        help="CLI agent to use for stage 6 synthesis (default: auto-detect first available). "
        "'none' skips LLM generation and relies only on similar-context matching.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-run even for tasks already processed; applies to stages 5 (render_problems) and 6 (synthesize_images)",
    )
    parser.add_argument(
        "--offline-source",
        type=str,
        default=None,
        metavar="PATH",
        help="Path to a parquet file with offline PR data to import into stages 1 and 2",
    )
    parser.add_argument(
        "--min-stars",
        type=int,
        default=500,
        metavar="N",
        help="Minimum stars for GitHub code search repo discovery in stage 1 (default: 500)",
    )
    return parser.parse_args(argv)


def validate_dates(args: argparse.Namespace) -> None:
    for name in ("start_date", "end_date"):
        val = getattr(args, name)
        if not _DATE_RE.match(val):
            print(f"Error: {name.replace('_', '-')} must be YYYY-MM-DD, got '{val}'", file=sys.stderr)
            sys.exit(1)


_sigint_count = 0


def _sigint_handler(signum: int, frame: object) -> None:
    """Handle CTRL+C by killing agent subprocesses so worker threads unblock.

    Agent subprocesses run in their own sessions (``start_new_session=True``)
    and don't receive SIGINT from the terminal.  Without this handler the
    threads blocked on ``proc.communicate()`` never return and the process
    hangs.

    First CTRL+C: SIGTERM all tracked agent processes, raise KeyboardInterrupt.
    Second CTRL+C: SIGKILL all tracked agent processes, force-exit immediately.
    """
    global _sigint_count
    _sigint_count += 1

    from datasmith.agents.installed.base import terminate_all_agents

    if _sigint_count >= 2:
        logger.info("Force-killing all agent subprocesses")
        terminate_all_agents(force=True)
        os._exit(1)

    logger.info("Interrupted — terminating agent subprocesses (press Ctrl+C again to force-quit)")
    terminate_all_agents(force=False)
    raise KeyboardInterrupt


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    validate_dates(args)

    from datasmith.update.pipeline import Pipeline

    signal.signal(signal.SIGINT, _sigint_handler)

    pipeline = Pipeline(
        dry_run=args.dry_run,
        n_concurrent=args.n_concurrent,
        tasks_per_repo=args.tasks_per_repo,
        agent=args.agent,
        force=args.force,
        offline_source=args.offline_source,
        min_stars=args.min_stars,
    )
    try:
        asyncio.run(
            pipeline.run(
                start_date=args.start_date,
                end_date=args.end_date,
                resume=args.resume,
                stage=args.stage,  # None or list[int] from append action
            )
        )
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)


if __name__ == "__main__":
    main()
