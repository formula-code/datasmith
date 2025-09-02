from __future__ import annotations

import argparse
import asyncio
import datetime
import logging
import math
import os
import pickle
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import asv
import pandas as pd

from datasmith.benchmark.collection import BenchmarkCollection
from datasmith.docker.context import ContextRegistry
from datasmith.docker.orchestrator import (
    build_repo_sha_image,
    get_docker_client,
    orchestrate,
)
from datasmith.docker.validation import BuildResult, Task
from datasmith.logging_config import configure_logging
from datasmith.scrape.utils import _parse_commit_url

# logger = configure_logging(level=logging.DEBUG, stream=open(Path(__file__).with_suffix(".log"), "w"))
logger = configure_logging(level=logging.DEBUG)


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
        commits = pd.read_json(args.commits, lines=True)
        all_states = {}
        for _, row in commits.iterrows():
            repo_name = row["repo_name"]
            sha = row["commit_sha"]
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


def main(args: argparse.Namespace) -> None:
    client = get_docker_client()
    all_states = process_inputs(args)
    context_registry = ContextRegistry.load_from_file(path=args.context_registry)

    # Prepare tasks
    tasks: list[Task] = []
    for (owner, repo), uniq in all_states.items():
        limited = list(uniq)[: max(0, args.limit_per_repo)] if args.limit_per_repo > 0 else list(uniq)
        for sha, date in limited:
            task = Task(owner, repo, sha, commit_date=date)
            if task in context_registry:
                tasks.append(task)
            else:
                logger.debug(f"main: skipping {task} as not in context registry")

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

    builds: list[BuildResult] = []
    if args.max_concurrency < 1:
        for t in tasks:
            build_res: BuildResult = build_repo_sha_image(
                client=client,
                context_registry=context_registry,
                task=t,
                force=args.force_rebuild,
            )
            builds.append(build_res)
    else:
        with ThreadPoolExecutor(max_workers=args.max_concurrency) as pool:
            futures = [
                pool.submit(
                    build_repo_sha_image,
                    client,
                    context_registry,
                    task,
                    args.force_rebuild,
                )
                for task in tasks
            ]
            for fut in as_completed(futures):
                builds.append(fut.result())

    successful_builds = [b for b in builds if b.rc != 1]

    logger.info("Running benchmarks for %d images", len(successful_builds))
    logger.info("Failed builds for %d images", len(builds) - len(successful_builds))
    for b in builds:
        if b.rc == 1:
            logger.warning("Build failed for %s", b.image_name)

    machine_args: dict[str, str] = asv.machine.Machine.get_defaults()  # pyright: ignore[reportAttributeAccessIssue]
    machine_args["num_cpu"] = str(args.num_cores)
    files_by_image: dict[str, dict[str, str]] = asyncio.run(
        orchestrate(
            docker_image_names=[b.image_name for b in successful_builds],
            asv_args=asv_args,
            machine_args=machine_args,
            max_concurrency=max_concurrency,
            n_cores=n_cores,
            output_dir=args.output_dir.absolute(),
            client=client,
        )
    )
    # save the files by image as a pickle file.
    with open(output_dir / "files_by_image.pkl", "wb") as f:
        pickle.dump(files_by_image, f)

    # save the files by image as a JSON file
    output_file = output_dir / "benchmark_results.json"
    with open(output_file, "w") as f:
        pd.DataFrame.from_dict(files_by_image, orient="index").to_json(f, orient="records", lines=True)

    logger.info("Benchmark results saved to %s", output_file)


if __name__ == "__main__":
    args = parse_args()
    main(args)
