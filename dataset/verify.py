from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

import asv

from datasmith.core.models import Task
from datasmith.docker.context import ContextRegistry, DockerContext
from datasmith.docker.orchestrator import build_repo_sha_image, gen_run_labels, get_docker_client
from datasmith.docker.validation import DockerValidator, ValidationConfig
from datasmith.logging_config import configure_logging
from datasmith.notebooks.utils import update_cr

"""
Usage:
    python dataset/verify.py --task dataset/formulacode_verified/<repo>/<sha>

This module separates loading from verification:
  - `load(task_dir)` returns (Task, DockerContext) from a user-editable directory.
  - `verify_task_with_context(...)` takes a Task + DockerContext and performs:
      * Docker build
      * profile.sh validation
      * run_tests.sh validation
      * context registry update
      * ECR publish of the final image
"""


def load(task_dir: Path) -> tuple[Task, DockerContext]:
    """Load a Task and DockerContext from a local, user-editable directory."""
    task_data = task_dir.joinpath("task.txt").read_text()
    dockerfile_data = task_dir.joinpath("Dockerfile").read_text()
    entrypoint_data = task_dir.joinpath("entrypoint.sh").read_text()
    base_building_data = task_dir.joinpath("docker_build_base.sh").read_text()
    run_building_data = task_dir.joinpath("docker_build_run.sh").read_text()
    env_building_data = task_dir.joinpath("docker_build_env.sh").read_text()
    final_building_data = task_dir.joinpath("docker_build_final.sh").read_text()
    building_data = task_dir.joinpath("docker_build_pkg.sh").read_text()
    profile_data = task_dir.joinpath("profile.sh").read_text()
    run_tests_data = task_dir.joinpath("run_tests.sh").read_text()
    task = eval(task_data)  # noqa: S307 - trusted, local task payload
    context = DockerContext(
        dockerfile_data=dockerfile_data,
        entrypoint_data=entrypoint_data,
        base_building_data=base_building_data,
        run_building_data=run_building_data,
        env_building_data=env_building_data,
        final_building_data=final_building_data,
        building_data=building_data,
        profile_data=profile_data,
        run_tests_data=run_tests_data,
    )
    return task, context


def _load_config() -> dict:
    """Load dataset verification config (JSON) from dataset/config.*."""
    cfg_path = Path("dataset") / "config.json"
    if cfg_path.exists():
        return json.loads(cfg_path.read_text())
    raise FileNotFoundError("Expected dataset/config.json or dataset/config.py to exist.")


def _format_failure(stage: str, stdout: str | None, stderr: str | None, rc: int | None = None) -> str:
    """Format a human-readable failure message with full traces."""
    parts: list[str] = [f"Verification failed during '{stage}' stage."]
    if rc is not None:
        parts.append(f"Return code: {rc}")
    parts.append("\n--- STDOUT ---")
    parts.append(stdout or "(no stdout)")
    parts.append("\n--- STDERR ---")
    parts.append(stderr or "(no stderr)")
    return "\n".join(parts)


def verify_task_with_context(
    task: Task,
    context: DockerContext,
    *,
    config: dict,
    context_registry: ContextRegistry,
    docker_client: Any,
    registry_path: Path,
    logger: logging.Logger | None = None,
) -> str:
    """Verify a Task + DockerContext and publish the final image to ECR.

    Returns the ECR image reference on success.
    Raises RuntimeError with a detailed message on failure.
    """
    log = logger or configure_logging()

    if task.sha is None:
        msg = "Task.sha must be set for verification."
        log.error(msg)
        raise RuntimeError(msg)

    # Build the 'run' image for this Task using the provided context.
    run_task = task.with_tag("run")
    run_id = f"verify-{task.sha}"
    log.info("Building image %s for verification", run_task.get_image_name())
    build_res = build_repo_sha_image(
        client=docker_client,
        docker_ctx=context,
        task=run_task,
        force=True,
        run_id=run_id,
    )

    if not build_res.ok:
        msg = _format_failure("build", build_res.stdout_tail, build_res.stderr_tail, build_res.rc)
        log.error("%s", msg)
        raise RuntimeError(msg)

    # Prepare DockerValidator for profile + test validation.
    machine_defaults: dict[str, str] = asv.machine.Machine.get_defaults()  # type: ignore[attr-defined]
    machine_defaults = {
        k: str(v).replace(" ", "_").replace("'", "").replace('"', "") for k, v in machine_defaults.items()
    }

    output_dir = Path("scratch/dataset_verify").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    validator = DockerValidator(
        client=docker_client,
        context_registry=context_registry,
        machine_defaults=machine_defaults,
        config=ValidationConfig(
            output_dir=output_dir,
            build_timeout=3600,
            run_timeout=3600,
            tail_chars=4000,
        ),
    )

    run_labels = gen_run_labels(run_task, runid=run_id)

    # 1) Profile validation
    log.info("Running profiler validation for %s", run_task.get_image_name())
    profile_res = validator.validate_profile(image_name=run_task.get_image_name(), run_labels=run_labels)
    if not profile_res.ok:
        msg = _format_failure("profile", profile_res.stdout, profile_res.stderr)
        log.error("%s", msg)
        raise RuntimeError(msg)

    # 2) Test validation (pytest)
    log.info("Running pytest validation for %s", run_task.get_image_name())
    tests_res = validator.validate_tests(
        image_name=run_task.get_image_name(),
        repo_name=f"{task.owner}/{task.repo}",
        run_labels=run_labels,
    )
    if not tests_res.ok:
        msg = _format_failure("tests", tests_res.stdout, tests_res.stderr)
        log.error("%s", msg)
        raise RuntimeError(msg)

    log.info("Profile and tests both passed for %s", run_task.get_image_name())

    # Register the now-verified context under the pkg tag and persist registry.
    with context_registry.get_lock():
        context_registry.register(task.with_tag("pkg"), context)
        context_registry.save_to_file(registry_path)
    log.info("Registered verified context for %s and saved registry to %s", task, registry_path)

    # Build and publish the final image to ECR (force push).
    ecr_repo = config.get("ecr_repo", "formulacode/all")
    aws_region = (
        config.get("aws_region") or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1"
    )

    final_task = task.with_tag("final").with_benchmarks(profile_res.benchmarks)
    log.info(
        "Building and publishing final image %s to ECR repo %s in region %s",
        final_task.get_image_name(),
        ecr_repo,
        aws_region,
    )

    publish_build_res, push_results = context.build_and_publish_to_ecr(
        client=docker_client,
        task=final_task,
        region=aws_region,
        single_repo=ecr_repo,
        skip_existing=False,
        force=True,
        timeout_s=3600,
    )

    local_ref = final_task.get_image_name()
    if not publish_build_res.ok or local_ref not in push_results:
        msg = _format_failure(
            "ecr_push",
            publish_build_res.stdout_tail,
            publish_build_res.stderr_tail,
            publish_build_res.rc,
        )
        log.error("%s", msg)
        raise RuntimeError(msg)

    ecr_ref = push_results[local_ref]
    log.info("Verification succeeded. Published %s to ECR as %s", local_ref, ecr_ref)
    return ecr_ref


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", type=Path, required=True, help="Path to the task directory")
    args = parser.parse_args()

    config = _load_config()
    registry_path = Path(config["context_registry_path"])

    context_registry = ContextRegistry.load_from_file(registry_path)

    # Normalize any existing registry entries to the current DockerContext layout
    context_registry = update_cr(context_registry)

    logger = configure_logging()
    docker_client = get_docker_client()

    task, context = load(args.task)
    logger.info("Loaded task %s from %s", task, args.task)

    try:
        ecr_ref = verify_task_with_context(
            task=task,
            context=context,
            config=config,
            context_registry=context_registry,
            docker_client=docker_client,
            registry_path=registry_path,
            logger=logger,
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from exc

    final_task = task.with_tag("final")
    local_ref = final_task.get_image_name()
    print(f"SUCCESS: {local_ref} -> {ecr_ref}")


if __name__ == "__main__":
    main()
