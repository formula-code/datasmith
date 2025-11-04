from __future__ import annotations

import argparse
import datetime
import os
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import asv
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from docker.client import DockerClient

from datasmith.agents.build import _do_build
from datasmith.core.models import Task
from datasmith.docker.context import ContextRegistry, DockerContext, build_base_image
from datasmith.docker.orchestrator import gen_run_labels, get_docker_client
from datasmith.docker.validation import DockerValidator, ValidationConfig
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
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip pushing images that already exist on ECR.",
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
    one_month_ago = datetime.datetime.now() - datetime.timedelta(days=30)
    return datetime.datetime.fromtimestamp(unix_time) >= one_month_ago


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


def _encode_ecr_tag_from_local(local_ref: str) -> str:
    """Encode a local image reference into the tag used for single-repo ECR publishing.

    Mirrors datasmith.docker.ecr._encode_tag_from_local used when repository_mode="single":
      - local_ref like "repo[:tag]" becomes "repo--tag" (slashes in either side become "__").
      - If the result exceeds 128 chars, add an 8-char hash suffix (not expected here).
    """
    import hashlib

    if ":" in local_ref and "/" not in local_ref.split(":", 1)[1]:
        repo, tag = local_ref.rsplit(":", 1)
    else:
        repo, tag = local_ref, "latest"
    base = repo.replace("/", "__")
    tag_enc = tag.replace("/", "__").replace(":", "--")
    composed = f"{base}--{tag_enc}"
    if len(composed) <= 128:
        return composed
    h = hashlib.sha256(composed.encode()).hexdigest()[:8]
    trimmed = composed[-(128 - 10) :]
    return f"{trimmed}--{h}"


def _list_ecr_tags_single_repo(*, region: str, repo_name: str) -> set[str]:
    """Return the set of existing image tags for an ECR repository.

    Safe: returns empty set on missing repo or auth issues. Logs warnings instead of raising.
    """
    tags: set[str] = set()
    try:
        session = boto3.session.Session(region_name=region)
        ecr = session.client("ecr")
        token: str | None = None
        while True:
            kwargs = {"repositoryName": repo_name, "maxResults": 1000}
            if token:
                kwargs["nextToken"] = token
            try:
                resp = ecr.list_images(**kwargs)
            except ClientError as ce:  # pragma: no cover - network dependent
                code = ce.response.get("Error", {}).get("Code")
                if code == "RepositoryNotFoundException":
                    logger.info("ECR repository %s not found; assuming no existing images.", repo_name)
                    return set()
                logger.warning("Failed to list ECR images for %s: %s", repo_name, ce)
                return set()
            for img in resp.get("imageIds", []):
                t = img.get("imageTag")
                if t:
                    tags.add(str(t))
            token = resp.get("nextToken")
            if not token:
                break
    except Exception as exc:  # pragma: no cover - network dependent
        logger.warning("Could not query ECR for existing tags (region=%s, repo=%s): %s", region, repo_name, exc)
        return set()
    return tags


def filter_tasks_not_on_ecr(tasks: list[Task], *, region: str, repository_mode: str = "single",
                            single_repo: str = "formulacode/all") -> list[Task]:
    """Filter out tasks whose target image already exists on ECR.

    Currently supports repository_mode="single" (default used by Context.build_and_publish_to_ecr).
    """
    if repository_mode != "single":
        # Fallback: if we don't know how tags are computed, don't filter
        logger.warning("ECR pre-filter only supports repository_mode='single'; skipping filter.")
        return tasks

    existing_tags = _list_ecr_tags_single_repo(region=region, repo_name=single_repo)
    if not existing_tags:
        return tasks

    filtered: list[Task] = []
    skipped = 0
    for t in tasks:
        local_ref = t.with_tag("run").get_image_name()  # e.g., owner-repo-sha:run
        enc_tag = _encode_ecr_tag_from_local(local_ref)  # e.g., owner-repo-sha--run
        if enc_tag in existing_tags:
            skipped += 1
            logger.info("Skipping %s (already on ECR as %s:%s)", local_ref, single_repo, enc_tag)
            continue
        filtered.append(t)
    if skipped:
        logger.info("Filtered out %d/%d tasks already on ECR", skipped, len(tasks))
    return filtered


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

    machine_defaults: dict[str, str] = asv.machine.Machine.get_defaults()  # pyright: ignore[reportAttributeAccessIssue]
    machine_defaults = {
        k: str(v.replace(" ", "_").replace("'", "").replace('"', "")) for k, v in machine_defaults.items()
    }
    validator = DockerValidator(
        client=client,
        context_registry=context_registry,
        machine_defaults=machine_defaults,
        config=ValidationConfig(
            output_dir=Path("scratch/docker_validation"),
            build_timeout=3600,
            run_timeout=3600,
            tail_chars=4000
        )
    )

    logger.info("Building base image...")
    base_tag = build_base_image(client, DockerContext())
    logger.debug("%s", base_tag)
    # os.environ["DOCKER_CACHE_FROM"] = base_tag

    # Prepare tasks
    tasks = prepare_tasks(all_states, context_registry)
    # Filter out tasks already present on ECR before building
    # aws_region = os.environ.get("AWS_REGION", "us-east-1")
    # tasks = filter_tasks_not_on_ecr(tasks, region=aws_region)
    # logger.info("main: Starting work on %d tasks[%d workers]", len(tasks), args.max_workers)

    def build_and_publish_task(task: Task, client: DockerClient) -> tuple[dict, dict]:
        task_analysis, task = resolve_task(task)
        logger.debug("Resolved task: %s", task_analysis)
        run_labels = gen_run_labels(task, runid=uuid.uuid4().hex)
        ctx = context_registry.get(task.with_tag("pkg"))
        partial_build_res = _do_build(validator, task.with_tag("run"), ctx, run_labels)
        build_res, push_results = ctx.build_and_publish_to_ecr(
            client=client,
            region=os.environ.get("AWS_REGION", "us-east-1"),
            task=task.with_tag("final").with_benchmarks(partial_build_res.benchmarks),
            timeout_s=3600,
            skip_existing=args.skip_existing,
        )
        # remove the containers.
        try:
            container = client.containers.get(task.with_tag("run").get_container_name())
            container.remove(force=True)
        except Exception:
            logger.exception("Error removing container: %s", task.with_tag("run").get_container_name())

        try:
            container = client.containers.get(task.with_tag("final").get_container_name())
            container.remove(force=True)
        except Exception:
            logger.exception("Error removing container: %s", task.with_tag("final").get_container_name())

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
