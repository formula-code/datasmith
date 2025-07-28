from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
from pathlib import Path

import pandas as pd

from datasmith.docker.orchestrator import (
    ensure_image,
    get_docker_client,
    orchestrate,
)


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
        default="--quick",
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
        "--dep-recs",
        type=Path,
        help="An optional json file with recommended dependencies for each package. passed as pip install <recommended_deps> before installation.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    commits = pd.read_json(args.filtered_commits, lines=True)
    commits["repo_name"] = commits["repo_name"].str.lower()

    if not args.dep_recs.exists():
        recommended_deps = {}
    else:
        with open(args.dep_recs) as f:
            recommended_deps = json.load(f)

    commits["dep_recs"] = ""
    for query, custom_deps in recommended_deps.items():
        valid_idxs = commits.query(query).index
        commits.loc[valid_idxs, "dep_recs"] = [custom_deps] * len(valid_idxs)

    repo_urls = ("https://www.github.com/" + commits["repo_name"]).tolist()
    commit_shas = commits["commit_sha"].tolist()
    asv_conf_paths = [paths[0] for paths in commits["asv_conf_path"].tolist()]
    dependency_recs = commits["dep_recs"].tolist()
    # if repo_name is scikit-learn/scikit-learn -> docker container name is `asv-scikit-learn-scikit-learn`
    docker_image_names = [f"asv-{repo_url.split('/')[-2]}-{repo_url.split('/')[-1]}" for repo_url in repo_urls]
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
    visited = set()
    for image_name, repo_url in zip(docker_image_names, repo_urls):
        if image_name not in visited:
            ensure_image(client, image_name, repo_url, docker_dir=str(args.docker_dir))
            visited.add(image_name)

    asyncio.run(
        orchestrate(
            commit_shas=commit_shas,
            asv_conf_paths=asv_conf_paths,
            docker_image_names=docker_image_names,
            asv_args=asv_args,
            recommended_deps=dependency_recs,
            max_concurrency=max_concurrency,
            n_cores=n_cores,
            output_dir=args.output_dir.absolute(),
            client=client,
        )
    )


if __name__ == "__main__":
    main()
