"""
This script builds and validates that each benchmark container can be compiled and will run asv successfully.
"""

import argparse
import logging
from pathlib import Path

from datasmith.benchmark.collection import BenchmarkCollection
from datasmith.docker.context_registry import CONTEXT_REGISTRY
from datasmith.docker.orchestrator import get_docker_client
from datasmith.logging_config import configure_logging
from datasmith.scrape.utils import _parse_commit_url

# logger = configure_logging(level=logging.DEBUG, stream=open(Path(__file__).with_suffix(".log"), "a"))
logger = configure_logging(level=logging.DEBUG)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="validate_containers",
        description="Validate that each benchmark container can be compiled and run with ASV.",
    )

    parser.add_argument(
        "--dashboard",
        type=Path,
        required=True,
        help="Path to the dashboard containing the benchmarks.",
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
    return parser.parse_args()


def main(args: argparse.Namespace) -> None:
    dashboard = BenchmarkCollection.load(args.dashboard)
    all_states = {}
    for owner, repo, sha in dashboard.enriched_breakpoints.url.apply(_parse_commit_url):
        if (owner, repo) not in all_states:
            all_states[(owner, repo)] = {sha}
        else:
            all_states[(owner, repo)].add(sha)

    client = get_docker_client()

    for (owner, repo), uniq_shas in all_states.items():
        for sha in uniq_shas:
            image_name = f"asv-{owner}-{repo}-{sha}"
            docker_ctx = CONTEXT_REGISTRY[image_name]
            docker_ctx.build_container(
                client=client,
                image_name=image_name,
                build_args={
                    "REPO_URL": f"https://www.github.com/{owner}/{repo}",
                    "COMMIT_SHA": sha,
                },
                force=True,
            )
            logger.debug(f"Validating {image_name} for commit {sha}")
            # stop any existing container with the same name
            container = client.containers.run(
                image=image_name,
                detach=True,
                remove=True,
                name=f"asv-{owner}-{repo}-{sha}",
                environment={"ASV_ARGS": "--quick --python=same"},
                volumes={str((args.output_dir / "results").absolute()): {"bind": "/output", "mode": "rw"}},
            )
            for line in container.logs(stream=True, follow=True):
                logger.info(line.decode().strip())

            result = container.wait()
            if result.get("StatusCode", 1) != 0:
                logger.error(
                    f"Container {image_name} for commit {sha} failed with status code {result.get('StatusCode', 1)}"
                )
            else:
                logger.info(f"Container {image_name} for commit {sha} completed successfully.")

    logger.info("All containers validated successfully.")


if __name__ == "__main__":
    args = parse_args()

    main(args)
