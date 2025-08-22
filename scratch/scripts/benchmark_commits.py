from __future__ import annotations

import argparse
import asyncio
import logging
import math
import os
import pickle
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

# logger = configure_logging(level=logging.DEBUG, stream=open(Path(__file__).with_suffix(".log"), "w"))
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


def process_commits(commits_pth: Path) -> list[tuple[str, str, str]]:
    commits = pd.read_json(commits_pth, lines=True)
    all_states = {}
    for _, row in commits.iterrows():
        repo_name = row["repo_name"]
        sha = row["commit_sha"]
        has_asv = row.get("has_asv", True)
        if not has_asv and "scikit-learn" not in repo_name:
            logger.warning(f"Skipping {repo_name} commit {sha} as it does not have ASV benchmarks.")
            continue
        owner, repo = repo_name.split("/")
        if (owner, repo) not in all_states:
            all_states[(owner, repo)] = {(sha)}
        else:
            all_states[(owner, repo)].add(sha)

    all_states_list = [(owner, repo, sha) for (owner, repo), shas in all_states.items() for sha in shas]

    return all_states_list


def main() -> None:
    args = parse_args()

    all_states = process_commits(args.filtered_commits)

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
    files_by_image: dict[str, dict[str, str]] = asyncio.run(
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
    # save the files by image as a pickle file.
    with open(output_dir / "files_by_image.pkl", "wb") as f:
        pickle.dump(files_by_image, f)

    # save the files by image as a JSON file
    output_file = output_dir / "benchmark_results.json"
    with open(output_file, "w") as f:
        pd.DataFrame.from_dict(files_by_image, orient="index").to_json(f, orient="records", lines=True)

    logger.info("Benchmark results saved to %s", output_file)


if __name__ == "__main__":
    main()
