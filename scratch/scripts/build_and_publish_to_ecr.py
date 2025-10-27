from __future__ import annotations

import argparse
import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from docker.client import DockerClient

from datasmith.core.models import Task
from datasmith.docker.context import ContextRegistry, DockerContext, build_base_image
from datasmith.docker.orchestrator import get_docker_client
from datasmith.execution.resolution.task_utils import resolve_task
from datasmith.logging_config import configure_logging
from datasmith.notebooks.utils import update_cr

logger = configure_logging(level=10, stream=open(Path(__file__).with_suffix(".log"), "w"))  # noqa: SIM115


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="build_and_publish_to_ecr",
        description="Build ASV Docker images for commits and publish to AWS ECR.",
    )
    parser.add_argument(
        "--commits",
        type=Path,
        help="Path to a JSONL file containing commit information. Either --dashboard or --commits must be provided.",
    )
    parser.add_argument(
        "--docker-dir",
        type=Path,
        default=Path("src/datasmith/docker"),
        help="Directory containing the Dockerfile and other necessary files for building the ASV image.",
    )
    parser.add_argument("--max-workers", type=int, default=8, help="Max parallel builds/runs.")
    parser.add_argument(
        "--context-registry",
        type=Path,
        required=True,
        help="Path to the context registry JSON file.",
    )
    return parser.parse_args()


def process_inputs(args: argparse.Namespace) -> dict[tuple[str, str], set[tuple[str, float, str]]]:
    commits = (
        pd.read_json(args.commits, lines=True) if args.commits.suffix == ".jsonl" else pd.read_parquet(args.commits)
    )
    all_states = {}
    for _, row in commits.iterrows():
        repo_name = row["repo_name"]
        # sha = row["sha"]
        sha = row["pr_base"]["sha"]
        has_asv = row.get("has_asv", True)
        if not has_asv:
            logger.debug("Skipping %s commit %s as it does not have ASV benchmarks.", repo_name, sha)
            continue
        owner, repo = repo_name.split("/")
        commit_date_unix: float = (
            0.0 if row.get("date", None) is None else datetime.datetime.fromisoformat(row["date"]).timestamp()
        )
        env_payload = row.get("env_payload", "")
        if (owner, repo) not in all_states:
            all_states[(owner, repo)] = [(sha, commit_date_unix, env_payload)]
        else:
            all_states[(owner, repo)].append((sha, commit_date_unix, env_payload))
    return all_states


def within_3_months(unix_time: float) -> bool:
    three_months_ago = datetime.datetime.now() - datetime.timedelta(days=90)
    return datetime.datetime.fromtimestamp(unix_time) >= three_months_ago


def prepare_tasks(
    all_states: dict[tuple[str, str], set[tuple[str, float, str]]],
    context_registry: ContextRegistry,
) -> list[Task]:
    all_tasks: list[Task] = []
    for (owner, repo), tup in all_states.items():
        tasks = list({
            Task(owner, repo, sha, commit_date=date, env_payload=env_payload) for sha, date, env_payload in sorted(tup)
        })
        # tasks = list(filter(lambda t: t.with_tag("pkg") not in context_registry, tasks))
        tasks = [
            t
            for t in tasks
            if (t.with_tag("pkg") in context_registry)
            and (within_3_months(context_registry.get(t.with_tag("pkg")).created_unix))
        ]
        all_tasks.extend(tasks)
    return all_tasks


def main(args: argparse.Namespace) -> None:
    # Size the Docker HTTP connection pool to our concurrency to avoid
    # adapter/pool starvation when many threads issue Docker API calls.
    client = get_docker_client(max_concurrency=args.max_workers)
    all_states = process_inputs(args)
    context_registry_pth = args.context_registry
    context_registry = (
        ContextRegistry.load_from_file(path=context_registry_pth)
        if context_registry_pth.exists()
        else ContextRegistry()
    )
    context_registry = update_cr(context_registry)

    logger.info("Building base image...")
    base_tag = build_base_image(client, DockerContext())
    logger.debug("%s", base_tag)
    # os.environ["DOCKER_CACHE_FROM"] = base_tag

    # Prepare tasks
    tasks = prepare_tasks(all_states, context_registry)
    logger.info("main: Starting work on %d tasks[%d workers]", len(tasks), args.max_workers)

    def build_and_publish_task(task: Task, client: DockerClient) -> tuple[dict, dict]:
        task_analysis, task = resolve_task(task)
        logger.debug("Resolved task: %s", task_analysis)
        ctx = context_registry.get(task.with_tag("pkg"))
        build_res, push_results = ctx.build_and_publish_to_ecr(
            client=client,
            region=os.environ.get("AWS_REGION", "us-east-1"),
            task=task.with_tag("run"),
        )
        return build_res.__dict__, push_results

    results: list[dict] = []
    if args.max_workers < 1:
        for t in tasks:
            build_res, push_res = build_and_publish_task(t, client)
            all_res = {**build_res, **push_res}
            results.append(all_res)
            logger.info("Completed: %s", all_res)
    else:
        with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
            futures = [
                ex.submit(
                    build_and_publish_task,
                    task=t,
                    client=client,
                )
                for t in tasks
            ]
            for fut in as_completed(futures):
                build_res, push_res = fut.result()
                all_res = {**build_res, **push_res}
                results.append(all_res)
                logger.info("Completed: %s", all_res)


if __name__ == "__main__":
    args = parse_args()
    main(args)
