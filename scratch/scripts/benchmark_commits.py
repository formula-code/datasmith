from __future__ import annotations

import argparse
import asyncio
import logging
import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import asv
import pandas as pd

from datasmith.docker.orchestrator import (
    build_repo_sha_image,
    get_docker_client,
    orchestrate,
)
from datasmith.logging_config import configure_logging
from datasmith.scrape.utils import _parse_commit_url

# logger = configure_logging(level=logging.DEBUG, stream=open(Path(__file__).with_suffix(".log").absolute(), "a"))
logger = configure_logging(level=logging.DEBUG)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ASV benchmark containers concurrently via Docker SDK",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--filtered-commits",
        type=Path,
        required=True,
        help="Path to a jsonl containing a pandas dataframe with commit_ids, repo_name, and the relative asv_conf_location.",
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    commits = pd.read_json(args.filtered_commits, lines=True)
    commits["repo_name"] = commits["repo_name"].str.lower()
    commit_urls = ("https://www.github.com/" + commits["repo_name"] + "/commit/" + commits["commit_sha"]).tolist()

    max_concurrency = (
        args.max_concurrency if args.max_concurrency != -1 else max(4, math.floor(0.5 * (os.cpu_count() or 1)))
    )
    asv_args = args.asv_args

    args.num_cores = max(1, args.num_cores)  # Ensure at least 1 core is used

    if args.num_cores * max_concurrency > (os.cpu_count() or 1):
        raise ValueError()

    n_cores = args.num_cores
    output_dir = Path(args.output_dir).absolute()

    # Create the results and logs directories if they don't exist
    Path(f"{output_dir}/results").mkdir(parents=True, exist_ok=True)
    Path(f"{output_dir}/logs").mkdir(parents=True, exist_ok=True)

    client = get_docker_client()

    # Ensure all required Docker images are available
    all_states = {}
    for owner, repo, sha in map(_parse_commit_url, commit_urls):
        if (owner, repo) not in all_states:
            all_states[(owner, repo)] = {sha}
        else:
            all_states[(owner, repo)].add(sha)

    all_states = list(set(map(_parse_commit_url, commit_urls)))
    docker_image_names = []

    with ThreadPoolExecutor(max_workers=args.num_cores * 4) as pool:
        futures = [
            pool.submit(build_repo_sha_image, client, owner, repo, sha, args.force_rebuild)
            for owner, repo, sha in all_states
        ]
        for fut in as_completed(futures):
            docker_image_names.append(fut.result())

    machine_args: dict[str, str] = asv.machine.Machine.get_defaults()  # pyright: ignore[reportAttributeAccessIssue]
    machine_args["num_cpu"] = str(args.num_cores)
    asyncio.run(
        orchestrate(
            docker_image_names=docker_image_names,
            asv_args=asv_args,
            machine_args=machine_args,
            max_concurrency=max_concurrency,
            n_cores=n_cores,
            output_dir=args.output_dir.absolute(),
            client=client,
        )
    )


if __name__ == "__main__":
    main()
