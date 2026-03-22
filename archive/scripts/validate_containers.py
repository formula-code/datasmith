"""
This script builds and validates that each benchmark container can be compiled and will run asv successfully.
"""

import argparse
import json
import os
from pathlib import Path

import asv
import pandas as pd

from datasmith.benchmark.collection import BenchmarkCollection
from datasmith.core.models import Task
from datasmith.docker.context import ContextRegistry
from datasmith.docker.orchestrator import get_docker_client, log_container_output
from datasmith.logging_config import configure_logging
from datasmith.scrape.utils import _parse_commit_url

logger = configure_logging()
# logger = configure_logging(level=logging.DEBUG, stream=open(Path(__file__).with_suffix(".log"), "w"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="validate_containers",
        description="Validate that each benchmark container can be compiled and run with ASV.",
    )

    parser.add_argument(
        "--dashboard",
        type=Path,
        help="Path to the dashboard containing the benchmarks. Either --dashboard or --commits must be provided.",
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
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Directory where the results will be stored.",
    )
    parser.add_argument(
        "--context-registry",
        type=Path,
        help="Path to the context registry JSON file.",
    )
    return parser.parse_args()


def process_inputs(args: argparse.Namespace) -> dict[tuple[str, str], set[str]]:
    if args.dashboard:
        dashboard = BenchmarkCollection.load(args.dashboard)
        all_states = {}
        for owner, repo, sha in dashboard.enriched_breakpoints.url.apply(_parse_commit_url):
            if (owner, repo) not in all_states:
                all_states[(owner, repo)] = {sha}
            else:
                all_states[(owner, repo)].add(sha)
    elif args.commits:
        commits = pd.read_json(args.commits, lines=True)
        all_states = {}
        for _, row in commits.iterrows():
            repo_name = row["repo_name"]
            sha = row["commit_sha"]
            has_asv = row.get("has_asv", True)
            if not has_asv:
                logger.warning(f"Skipping {repo_name} commit {sha} as it does not have ASV benchmarks.")
                continue
            owner, repo = repo_name.split("/")
            if (owner, repo) not in all_states:
                all_states[(owner, repo)] = {(sha)}
            else:
                all_states[(owner, repo)].add(sha)
    else:
        raise ValueError("Either --dashboard or --commits must be provided.")
    return all_states


def main(args: argparse.Namespace) -> None:
    client = get_docker_client()

    all_states = process_inputs(args)
    context_registry = ContextRegistry.load_from_file(path=args.context_registry)

    machine_args: dict[str, str] = asv.machine.Machine.get_defaults()  # pyright: ignore[reportAttributeAccessIssue]
    all_files_by_image = {}
    errors = []
    error_fmt = (
        "$ docker build -t {image_name} src/datasmith/docker/ --build-arg REPO_URL={repo_url} --build-arg COMMIT_SHA={commit_sha}"
        + "\n$ docker run --rm -v $(pwd)/output:/output {image_name} asv run --quick --python=same --set-commit-hash={commit_sha}"
    )
    for (owner, repo), uniq_shas in all_states.items():
        for sha in list(uniq_shas):
            task_key = Task(owner=owner, repo=repo, sha=sha, tag="pkg")
            docker_ctx = context_registry.get(task_key)
            env_payload = str(task_key.env_payload) or ""
            python_version = task_key.python_version or ""
            image_name = f"asv/{owner}/{repo}/{sha}".lower()
            try:
                docker_ctx.build_container(
                    client=client,
                    image_name=image_name,
                    build_args={
                        "REPO_URL": f"https://www.github.com/{owner}/{repo}",
                        "COMMIT_SHA": sha,
                        "ENV_PAYLOAD": env_payload,
                        "PY_VERSION": python_version,
                    },
                    force=True,
                )
                logger.debug(f"Validating {image_name} for commit {sha}")
                # stop any existing container with the same name
                machine_args["machine"] = sha
                container = client.containers.run(
                    image=image_name,
                    detach=True,
                    name=f"asv/{owner}/{repo}/{sha}",
                    environment={
                        "ASV_ARGS": f"--quick --python=same --set-commit-hash={sha}",
                        "ASV_MACHINE_ARGS": " ".join([f"--{k} '{v}'" for k, v in machine_args.items()]),
                    },
                    volumes={str((args.output_dir / "results").absolute()): {"bind": "/output", "mode": "rw"}},
                    network_mode=os.environ.get("DOCKER_NETWORK_MODE", None),
                )
                for line in container.logs(stream=True, follow=True):
                    logger.info(line.decode().strip())

                result = container.wait()
                if result.get("StatusCode", 1) != 0:
                    logger.error(
                        f"Container {image_name} for commit {sha} failed with status code {result.get('StatusCode', 1)}"
                    )
                    errors.append(
                        error_fmt.format(
                            image_name=image_name,
                            repo_url=f"https://www.github.com/{owner}/{repo}",
                            commit_sha=sha,
                        )
                    )
                    files = log_container_output(container, archive="/output")
                    print(f"{image_name} completed failed with status code {result.get('StatusCode', 1)}")
                else:
                    logger.info(f"Container {image_name} for commit {sha} completed successfully.")
                    files = log_container_output(container, archive="/output")
                    print(f"{image_name} completed successfully")
                all_files_by_image[image_name] = files
            except Exception:
                print(f"{image_name} for commit {sha} failed to build or run.")
                logger.exception(f"Error validating {image_name} for commit {sha}")
                errors.append(
                    error_fmt.format(
                        image_name=image_name,
                        repo_url=f"https://www.github.com/{owner}/{repo}",
                        commit_sha=sha,
                    )
                )
                continue

    logger.info("All containers validated successfully.")
    # save errors to a file
    if errors:
        with open(args.output_dir / "errors.txt", "w") as f:
            for error in errors:
                f.write(f"{error}\n")
        logger.error(f"Errors occurred during validation. See {args.output_dir / 'errors.txt'} for details.")
    else:
        logger.info("No errors occurred during validation.")
    # remove all containers
    for container in client.containers.list(all=True):
        if container.name.startswith("asv-"):
            logger.info(f"Removing container {container.name}")
            container.remove(force=True)

    # save all-files as a json file
    with open(args.output_dir / "all_files_by_image.json", "w") as f:
        json.dump(all_files_by_image, f, indent=4)

    logger.info("Results saved to %s", args.output_dir / "all_files_by_image.json")


if __name__ == "__main__":
    args = parse_args()

    main(args)
