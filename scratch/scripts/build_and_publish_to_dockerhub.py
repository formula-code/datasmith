"""Build ASV Docker images for commits and publish to DockerHub.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import asv
import pandas as pd

from datasmith.agents.build import _do_build
from datasmith.core.storage import read_table, resolve_table_name
from datasmith.core.models import Task
from datasmith.docker.context import ContextRegistry, DockerContext, build_base_image
from datasmith.docker.dockerhub import filter_tasks_not_on_dockerhub
from datasmith.docker.orchestrator import gen_run_labels, get_docker_client
from datasmith.docker.validation import DockerValidator, ValidationConfig
from datasmith.execution.resolution.task_utils import resolve_task
from datasmith.logging_config import configure_logging
from datasmith.notebooks.utils import update_cr

# Concurrency settings
# Note: Lower push concurrency for DockerHub to avoid rate limiting
_BUILD_CONCURRENCY = int(os.getenv("BUILD_CONCURRENCY", "24"))
_PUSH_CONCURRENCY = int(os.getenv("PUSH_CONCURRENCY", "8"))
_build_sem = threading.Semaphore(_BUILD_CONCURRENCY)
_push_sem = threading.Semaphore(_PUSH_CONCURRENCY)
_cr_lock = threading.Lock()  # protect ContextRegistry mutations

logger = configure_logging(level=10, stream=open(Path(__file__).with_suffix(".log"), "a"))  # noqa: SIM115


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="build_and_publish_to_dockerhub",
        description="Build ASV Docker images for commits and publish to DockerHub.",
    )
    parser.add_argument(
        "--commits",
        type=Path,
        required=True,
        help="Path to a JSONL or parquet file containing commit information.",
    )
    parser.add_argument(
        "--docker-dir",
        type=Path,
        default=Path("src/datasmith/docker"),
        help="Directory containing the Dockerfile and other necessary files for building the ASV image.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="Max parallel builds/runs.",
    )
    parser.add_argument(
        "--context-registry",
        type=Path,
        required=True,
        help="Path to the context registry JSON file.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip pushing images that already exist on DockerHub.",
    )
    parser.add_argument(
        "--namespace",
        type=str,
        required=True,
        help="DockerHub namespace (username or organization). Required.",
    )
    parser.add_argument(
        "--username",
        type=str,
        help="DockerHub username for authentication. Can also be set via DOCKERHUB_USERNAME env var.",
    )
    parser.add_argument(
        "--password",
        type=str,
        help="DockerHub password/token for authentication. Can also be set via DOCKERHUB_TOKEN env var.",
    )
    parser.add_argument(
        "--repository-mode",
        type=str,
        default="single",
        choices=["single", "mirror"],
        help="Repository mode: 'single' (all images in one repo) or 'mirror' (one repo per project).",
    )
    parser.add_argument(
        "--single-repo",
        type=str,
        default="all",
        help="Repository name for single mode (default: 'all').",
    )
    parser.add_argument("--db", type=str, default=None, help="Pipeline SQLite DB path.")
    return parser.parse_args()


def process_inputs(args: argparse.Namespace) -> dict[tuple[str, str], set[tuple[str, float, str]]]:
    """
    Process input commit data from JSONL or parquet file.

    Returns:
        Dictionary mapping (owner, repo) -> list of (sha, commit_date, env_payload) tuples
    """
    if args.commits.suffix == ".jsonl":
        commits = pd.read_json(args.commits, lines=True)
    else:
        commits = read_table(resolve_table_name(str(args.commits)), db_path=args.db)
    all_states = {}
    for _, row in commits.iterrows():
        repo_name = row["repo_name"]
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
    """Check if a Unix timestamp is within the last 3 months."""
    three_months_ago = datetime.datetime.now() - datetime.timedelta(days=90)
    return datetime.datetime.fromtimestamp(unix_time) >= three_months_ago


def prepare_tasks(
    all_states: dict[tuple[str, str], set[tuple[str, float, str]]],
    context_registry: ContextRegistry,
) -> list[Task]:
    """
    Prepare list of tasks from commit data.

    Filters tasks to only include those:
    - With a registered context in the registry
    - Created within the last 3 months

    Args:
        all_states: Dictionary of commit data grouped by (owner, repo)
        context_registry: Registry of Docker contexts

    Returns:
        List of Task objects
    """
    all_tasks: list[Task] = []
    for (owner, repo), tup in all_states.items():
        tasks = list({
            Task(owner, repo, sha, commit_date=date, env_payload=env_payload) for sha, date, env_payload in sorted(tup)
        })
        tasks = [
            t
            for t in tasks
            if (t.with_tag("pkg") in context_registry)
            and (within_3_months(context_registry.get(t.with_tag("pkg")).created_unix))
        ]
        all_tasks.extend(tasks)
    return all_tasks


def main(args: argparse.Namespace) -> None:
    """Main execution function."""
    # Validate credentials
    username = args.username or os.environ.get("DOCKERHUB_USERNAME")
    password = args.password or os.environ.get("DOCKERHUB_TOKEN") or os.environ.get("DOCKERHUB_PASSWORD")

    if not username or not password:
        logger.error(
            "DockerHub credentials required. Provide via:\n"
            "  --username/--password arguments, or\n"
            "  DOCKERHUB_USERNAME/DOCKERHUB_TOKEN environment variables, or\n"
            "  docker login docker.io\n"
            "Generate tokens at: https://hub.docker.com/settings/security"
        )
        return

    # Size the Docker HTTP connection pool to our concurrency to avoid
    # adapter/pool starvation when many threads issue Docker API calls.
    client = get_docker_client(max_concurrency=8)
    all_states = process_inputs(args)
    context_registry_pth = args.context_registry
    context_registry = (
        ContextRegistry.load_from_file(path=context_registry_pth)
        if context_registry_pth.exists()
        else ContextRegistry()
    )
    context_registry = update_cr(context_registry)

    machine_defaults: dict[str, str] = asv.machine.Machine.get_defaults()  # pyright: ignore[reportAttributeAccessIssue]
    machine_defaults = {
        k: str(v.replace(" ", "_").replace("'", "").replace('"', "")) for k, v in machine_defaults.items()
    }
    validator = DockerValidator(
        client=client,
        context_registry=context_registry,
        machine_defaults=machine_defaults,
        config=ValidationConfig(
            output_dir=Path("scratch/docker_validation"), build_timeout=3600, run_timeout=3600, tail_chars=4000
        ),
    )

    logger.info("Building base image...")
    base_tag = build_base_image(client, DockerContext())
    logger.debug("%s", base_tag)

    # Prepare tasks
    tasks = prepare_tasks(all_states, context_registry)

    # Filter out tasks already present on DockerHub before building
    if args.skip_existing:
        tasks = filter_tasks_not_on_dockerhub(
            tasks,
            namespace=args.namespace,
            username=username,
            password=password,
            repository_mode=args.repository_mode,
            single_repo=args.single_repo,
        )

    logger.info("main: Starting work on %d tasks [%d workers]", len(tasks), args.max_workers)
    logger.info(
        "Publishing to DockerHub namespace: %s (repository_mode=%s, single_repo=%s)",
        args.namespace,
        args.repository_mode,
        args.single_repo,
    )

    def build_and_publish_task(task: Task) -> tuple[dict, dict]:
        """Build and publish a single task to DockerHub."""
        # Create a fresh client per thread. Small pool is fine; build is mostly daemon-side work.
        client = get_docker_client(max_concurrency=8)
        try:
            _task_analysis, task = resolve_task(task)
            run_labels = gen_run_labels(task, runid=uuid.uuid4().hex)
            ctx = context_registry.get(task.with_tag("pkg"))

            with _build_sem:
                partial_build_res = _do_build(validator, task.with_tag("run"), ctx, run_labels)

            if not partial_build_res.ok:
                # Only one thread should edit/save the registry at a time
                if (
                    "docker_build_pkg" in partial_build_res.stderr_tail
                    or "docker_build_env" in partial_build_res.stderr_tail
                ):
                    with _cr_lock:
                        context_registry.pop(task.with_tag("pkg"))
                        context_registry.save_to_file(context_registry_pth)
                return partial_build_res.__dict__, {}

            with _push_sem:
                build_res, push_results = ctx.build_and_publish_to_dockerhub(
                    client=client,
                    namespace=args.namespace,
                    task=task.with_tag("final").with_benchmarks(partial_build_res.benchmarks),
                    repository_mode=args.repository_mode,
                    single_repo=args.single_repo,
                    timeout_s=3600,
                    skip_existing=args.skip_existing,
                    force=True,
                    username=username,
                    password=password,
                )

            return build_res.__dict__, push_results
        finally:
            with contextlib.suppress(Exception):
                client.api.close()

    results: list[dict] = []
    if args.max_workers < 1:
        for t in tasks:
            build_res, push_res = build_and_publish_task(t)
            all_res = {**build_res, **push_res}
            results.append(all_res)
            logger.info("Completed: %s", all_res)
    else:
        with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
            futures = [
                ex.submit(
                    build_and_publish_task,
                    task=t,
                )
                for t in tasks
            ]
            for fut in as_completed(futures):
                build_res, push_res = fut.result()
                all_res = {**build_res, **push_res}
                results.append(all_res)
                logger.info("Completed: %s", all_res)

    logger.info("All tasks completed. Total results: %d", len(results))


if __name__ == "__main__":
    args = parse_args()
    main(args)
