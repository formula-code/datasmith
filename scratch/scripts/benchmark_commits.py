from __future__ import annotations

import argparse
import asyncio
import datetime
import json
import logging
import math
import os
import pickle
import shutil
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import asv
import pandas as pd
from tqdm import tqdm

from datasmith.benchmark.collection import BenchmarkCollection
from datasmith.docker.context import ContextRegistry, DockerContext, Task, build_base_image
from datasmith.docker.orchestrator import (
    build_repo_sha_image,
    get_docker_client,
    orchestrate,
)
from datasmith.execution.collect_commits_offline import find_parent_releases
from datasmith.logging_config import configure_logging
from datasmith.scrape.utils import _parse_commit_url

logger = configure_logging(level=logging.DEBUG, stream=open(Path(__file__).with_suffix(".log"), "w"))  # noqa: SIM115
# logger = configure_logging(level=logging.DEBUG)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ASV benchmark containers concurrently via Docker SDK",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--commits",
        type=Path,
        required=True,
        help="Path to a jsonl containing a pandas dataframe with commit_ids, repo_name, and the relative asv_conf_location.",
    )
    parser.add_argument(
        "--dashboard",
        type=Path,
        help="Path to the dashboard containing the benchmarks. Either --dashboard or --commits must be provided.",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=-1,
        help="Maximum number of containers to run in parallel.",
    )
    parser.add_argument(
        "--asv-args",
        type=str,
        default="--append-samples -a rounds=2 -a repeat=2 --python=same",
        help="Additional arguments to pass to the asv command inside the container.",
    )
    parser.add_argument(
        "--num-cores",
        type=int,
        default=4,
        help="Number of CPU cores to dedicate to each container. If not specified, defaults to 4 cores per container.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory to store the results of the benchmarks.",
    )
    parser.add_argument(
        "--docker-dir",
        type=Path,
        default=Path("src/datasmith/docker"),
        help="Directory containing the Dockerfile and other necessary files for building the ASV image.",
    )
    parser.add_argument(
        "--force-rebuild",
        action="store_true",
        help="Force rebuild the Docker images even if they already exist.",
    )
    parser.add_argument(
        "--context-registry",
        type=Path,
        help="Path to the context registry JSON file.",
    )
    parser.add_argument(
        "--limit-per-repo",
        type=int,
        default=-1,
        help="Cap SHAs per repo (keeps your small-scale test). -1 = no limit.",
    )
    return parser.parse_args()


def process_inputs(args: argparse.Namespace) -> dict[tuple[str, str], set[tuple[str, float]]]:
    if args.dashboard:
        dashboard = BenchmarkCollection.load(args.dashboard)
        all_states = {}
        for owner, repo, sha in dashboard.enriched_breakpoints.url.apply(_parse_commit_url):
            owner = owner.lower()
            repo = repo.lower()
            sha = sha.lower()
            if (owner, repo) not in all_states:
                all_states[(owner, repo)] = {(sha, 0.0)}
            else:
                all_states[(owner, repo)].add((sha, 0.0))
    elif args.commits:
        commits = (
            pd.read_json(args.commits, lines=True) if args.commits.suffix == ".jsonl" else pd.read_parquet(args.commits)
        )
        all_states = {}
        for _, row in commits.iterrows():
            repo_name = row["repo_name"]
            sha = row["sha"]
            has_asv = row.get("has_asv", True)
            if not has_asv:
                logger.debug(f"Skipping {repo_name} commit {sha} as it does not have ASV benchmarks.")
                continue
            owner, repo = repo_name.split("/")
            commit_date_unix: float = (
                0.0 if row.get("date", None) is None else datetime.datetime.fromisoformat(row["date"]).timestamp()
            )
            if (owner, repo) not in all_states:
                all_states[(owner, repo)] = [(sha, commit_date_unix)]
            else:
                all_states[(owner, repo)].append((sha, commit_date_unix))
    else:
        raise ValueError("Either --dashboard or --commits must be provided.")
    return all_states


def is_benchmarked(task: Task, interim_path: Path, output_dir: Path) -> bool:
    return (
        (interim_path / f"{task.get_container_name()}.json").exists()
        or (output_dir / "results" / f"{task.get_container_name()}").exists()
        or (output_dir / "logs" / f"{task.get_container_name()}").with_suffix(".log").exists()
    )


def main(args: argparse.Namespace) -> None:  # noqa: C901
    client = get_docker_client(args.max_concurrency)
    all_states = process_inputs(args)
    context_registry = ContextRegistry.load_from_file(path=args.context_registry)
    interim_path = Path(os.environ["CACHE_LOCATION"]).parent / "interim"  # Look here for cached docker contexts

    logger.info("Building base image...")
    base_tag = build_base_image(client, DockerContext())
    logger.info("Base image built with tag: %s", base_tag)
    # os.environ["DOCKER_CACHE_FROM"] = base_tag

    # Prepare tasks
    tasks: list[tuple[Task, DockerContext]] = []
    repo_commit_pairs = defaultdict(list)
    for (owner, repo), uniq in all_states.items():
        limited = list(uniq)[: max(0, args.limit_per_repo)] if args.limit_per_repo > 0 else list(uniq)
        for sha, date in limited:
            task = Task(owner, repo, sha, commit_date=date)
            if task in context_registry:
                tasks.append((task, context_registry.get(task)))
                repo_commit_pairs[f"{owner}/{repo}"].append(task)
                # also add the parent commit.
            # else:
            # logger.debug(f"main: skipping {task} as not in context registry")

    # get all parent commits and add them as tasks as well.
    for repo_name, tsks in repo_commit_pairs.items():
        owner, repo = repo_name.split("/")
        shas = [t.sha for t in tsks]
        parent_commits = find_parent_releases(repo_name, shas, add_first=True, incl_datetime=True)
        for i, (parent_sha, date) in enumerate(parent_commits):
            parent_task = Task(owner=owner, repo=repo, sha=parent_sha, commit_date=date)  # pyright: ignore[reportArgumentType]
            # use the child context.
            ctx = context_registry.get(tsks[i])
            tasks.append((parent_task, ctx))

    max_concurrency = (
        args.max_concurrency if args.max_concurrency != -1 else max(4, math.floor(0.5 * (os.cpu_count() or 1)))
    )
    asv_args = args.asv_args

    args.num_cores = max(1, args.num_cores)  # Ensure at least 1 core is used

    if args.num_cores * max_concurrency > (os.cpu_count() or 1):
        raise ValueError()

    n_cores = args.num_cores
    output_dir = args.output_dir.absolute()
    # remove the folders first.
    shutil.rmtree(output_dir / "results", ignore_errors=True)
    shutil.rmtree(output_dir / "logs", ignore_errors=True)

    (args.output_dir / "results").mkdir(parents=True, exist_ok=True)
    (args.output_dir / "logs").mkdir(parents=True, exist_ok=True)

    machine_defaults: dict[str, str] = asv.machine.Machine.get_defaults()  # pyright: ignore[reportAttributeAccessIssue]
    machine_defaults = {
        k: str(v.replace(" ", "_").replace("'", "").replace('"', "")) for k, v in machine_defaults.items()
    }
    logger.debug("main: machine_defaults keys=%d", len(machine_defaults))
    dedup_tasks = []
    seen = set()
    for t, ctx in tasks:
        if t.get_container_name() not in seen:
            dedup_tasks.append((t, ctx))
            seen.add(t.get_container_name())
    tasks = dedup_tasks
    logger.info("Total unique tasks to consider: %d", len(tasks))
    already_benchmarked = list(filter(lambda x: is_benchmarked(x[0], interim_path, output_dir), tasks))
    logger.info("Skipping %d tasks that have already been benchmarked", len(already_benchmarked))
    to_benchmark = list(filter(lambda x: not is_benchmarked(x[0], interim_path, output_dir), tasks))
    logger.info("Total tasks to benchmark: %d", len(to_benchmark))
    if len(to_benchmark) == 0:
        logger.info("No tasks to benchmark. Exiting.")
        return

    # build the containers.
    builds = []
    with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
        futures = [
            pool.submit(build_repo_sha_image, client, ctx, task, args.force_rebuild, run_id="CANARY-BUILD")
            for (task, ctx) in to_benchmark
        ]
        for fut in tqdm(as_completed(futures), total=len(futures), desc="Building containers"):
            builds.append(fut.result())
    to_benchmark = [t for (t, b) in zip(to_benchmark, builds) if b.rc == 0]
    logger.info("Successfully built %d containers", len(to_benchmark))

    machine_args: dict[str, str] = asv.machine.Machine.get_defaults()  # pyright: ignore[reportAttributeAccessIssue]
    machine_args["num_cpu"] = str(args.num_cores)
    files_by_image: dict[Task, dict[str, str]] = asyncio.run(
        orchestrate(
            contexts=to_benchmark,
            asv_args=asv_args,
            machine_args=machine_args,
            max_concurrency=max_concurrency,
            n_cores=n_cores,
            output_dir=args.output_dir.absolute(),
            client=client,
        )
    )
    # Add already benchmarked files to the results
    for task, _ in already_benchmarked:
        if (interim_path / f"{task.get_container_name()}-final.json").exists():
            results = json.loads((interim_path / f"{task.get_container_name()}-final.json").read_text())
            files_by_image[task] = results
    # save the files by image as a pickle file.
    with open(output_dir / "files_by_image.json", "wb") as f:
        pickle.dump(files_by_image, f)

    # save the files by image as a JSON file
    output_file = output_dir / "benchmark_results.json"
    with open(output_file, "w") as f:
        pd.DataFrame.from_dict(files_by_image, orient="index").to_json(f, orient="records", lines=True)

    logger.info("Benchmark results saved to %s", output_file)

    # remove all images with CANARY-BUILD tag.
    try:
        client.images.prune(filters={"label": "datasmith.run=CANARY-BUILD"})
    except Exception:
        logger.exception("Failed to nprune images with CANARY-BUILD tag")


if __name__ == "__main__":
    args = parse_args()
    main(args)
