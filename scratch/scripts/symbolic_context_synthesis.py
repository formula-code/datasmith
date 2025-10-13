from __future__ import annotations

import argparse
import atexit
import datetime as dt
import json
import logging
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from datasmith.core.models import Task
from datasmith.docker.context import ContextRegistry
from datasmith.docker.orchestrator import get_docker_client
from datasmith.execution.resolution import analyze_commit
from datasmith.logging_config import configure_logging

log_file = open(Path(__file__).with_suffix(".log"), "w", encoding="utf-8")  # noqa: SIM115
atexit.register(log_file.close)
logger = configure_logging(level=logging.DEBUG, stream=log_file)


def symbolic_build_and_validate(
    task: Task,
    args: argparse.Namespace,
    client,
    context_registry: ContextRegistry,
) -> dict:
    """
    Symbolic (non-LLM) build and validation using analyze_commit for dependency resolution.

    Unlike the agent-based version, this:
    1. Uses analyze_commit() to resolve dependencies
    2. Injects them via env_payload (no custom script generation)
    3. Single attempt (no iterative fixing)
    """
    analysis = analyze_commit(task.sha, f"{task.owner}/{task.repo}", bypass_cache=False)
    # if "can_install" not in analysis or not analysis["can_install"]:
    #     analysis = analyze_commit(task.sha, f"{task.owner}/{task.repo}", bypass_cache=True)
    logger.debug("Analysis result: %s", analysis)
    return analysis


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="symbolic_context_synthesis",
        description="Build and validate benchmark containers using symbolic dependency resolution (no LLM).",
    )
    parser.add_argument(
        "--commits",
        type=Path,
        required=True,
        help="Path to a parquet file containing commit information.",
    )
    parser.add_argument(
        "--docker-dir",
        type=Path,
        default=Path("src/datasmith/docker"),
        help="Directory containing the Dockerfile and other necessary files for building the ASV image.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Directory where the results will be stored.",
    )
    parser.add_argument("--max-workers", type=int, default=8, help="Max parallel builds/runs.")
    parser.add_argument("--build-timeout", type=int, default=40 * 60, help="Seconds before aborting a docker build.")
    parser.add_argument("--tail-chars", type=int, default=4000, help="Chars of log tail to include in failure report.")
    parser.add_argument(
        "--limit-per-repo", type=int, default=5, help="Cap SHAs per repo (keeps your small-scale test). -1 = no limit."
    )
    parser.add_argument(
        "--context-registry",
        type=Path,
        required=True,
        help="Path to the context registry JSON file.",
    )
    return parser.parse_args()


def process_inputs(args: argparse.Namespace) -> dict[tuple[str, str], list[tuple[str, float, str]]]:
    """Load commits from parquet file."""
    commits = pd.read_parquet(args.commits)
    all_states = {}

    for _, row in commits.iterrows():
        repo_name = row["repo_name"]
        sha = row["sha"]
        has_asv = row.get("has_asv", True)

        if not has_asv:
            logger.debug("Skipping %s commit %s as it does not have ASV benchmarks.", repo_name, sha)
            continue

        owner, repo = repo_name.split("/")
        commit_date_unix: float = (
            0.0 if row.get("date", None) is None else dt.datetime.fromisoformat(row["date"]).timestamp()
        )
        env_payload = row.get("env_payload", "")

        if (owner, repo) not in all_states:
            all_states[(owner, repo)] = [(sha, commit_date_unix, env_payload)]
        else:
            all_states[(owner, repo)].append((sha, commit_date_unix, env_payload))

    return all_states


def prepare_tasks(
    all_states: dict[tuple[str, str], list[tuple[str, float, str]]],
    limit_per_repo: int,
    context_registry: ContextRegistry,
) -> list[Task]:
    """Create Task objects from commit data."""
    all_tasks: list[Task] = []

    for (owner, repo), tup in all_states.items():
        tasks = [
            Task(owner, repo, sha, commit_date=date, env_payload=env_payload) for sha, date, env_payload in sorted(tup)
        ]

        # Filter out already processed
        # tasks = [t for t in tasks if t.with_tag("pkg") not in context_registry]

        # Limit per repo
        if limit_per_repo > 0:
            tasks = random.sample(tasks, min(limit_per_repo, len(tasks)))

        all_tasks.extend(tasks)

    return all_tasks


def main(args: argparse.Namespace) -> None:
    client = get_docker_client()
    all_states = process_inputs(args)

    # Load or create context registry
    if not args.context_registry.exists():
        logger.warning("Context registry file %s does not exist; starting fresh", args.context_registry)
        context_registry_pth = Path("scratch/context_registry_init.json")
    else:
        context_registry_pth = args.context_registry

    context_registry = (
        ContextRegistry.load_from_file(path=context_registry_pth)
        if context_registry_pth.exists()
        else ContextRegistry()
    )

    tasks = prepare_tasks(all_states, args.limit_per_repo, context_registry)

    # Setup output
    (args.output_dir / "results").mkdir(parents=True, exist_ok=True)
    (args.output_dir / "errors.txt").unlink(missing_ok=True)
    (args.output_dir / "results.jsonl").unlink(missing_ok=True)

    logger.info("Starting work on %d tasks [%d workers]", len(tasks), args.max_workers)

    results: list[dict] = []

    if args.max_workers < 1:
        # Sequential execution
        for t in tasks:
            res = symbolic_build_and_validate(
                task=t,
                args=args,
                client=client,
                context_registry=context_registry,
            )
            results.append(res)

            with open(args.output_dir / "results.jsonl", "a", encoding="utf-8") as jf:
                jf.write(json.dumps(res) + "\n")

    else:
        # Parallel execution
        with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
            futures = [
                ex.submit(
                    symbolic_build_and_validate,
                    task=t,
                    args=args,
                    client=client,
                    context_registry=context_registry,
                )
                for t in random.sample(tasks, len(tasks))
            ]

            for fut in as_completed(futures):
                res = fut.result()
                results.append(res)

                with open(args.output_dir / "results.jsonl", "a", encoding="utf-8") as jf:
                    jf.write(json.dumps(res) + "\n")


if __name__ == "__main__":
    args = parse_args()
    main(args)
