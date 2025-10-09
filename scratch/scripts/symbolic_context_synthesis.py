from __future__ import annotations

import argparse
import atexit
import datetime as dt
import json
import logging
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

from datasmith.core.models import Task
from datasmith.docker.context import ContextRegistry
from datasmith.docker.orchestrator import get_docker_client
from datasmith.execution.resolution import analyze_commit
from datasmith.logging_config import configure_logging

log_file = open(Path(__file__).with_suffix(".log"), "w", encoding="utf-8")  # noqa: SIM115
atexit.register(log_file.close)
logger = configure_logging(level=logging.DEBUG, stream=log_file)


def symbolic_build_and_validate(
    task: Task,
    args: argparse.Namespace,
    client,
    context_registry: ContextRegistry,
) -> dict:
    """
    Symbolic (non-LLM) build and validation using analyze_commit for dependency resolution.

    Unlike the agent-based version, this:
    1. Uses analyze_commit() to resolve dependencies
    2. Injects them via env_payload (no custom script generation)
    3. Single attempt (no iterative fixing)
    """
    analysis = analyze_commit(task.sha, f"{task.owner}/{task.repo}", bypass_cache=True)
    # if "can_install" not in analysis or not analysis["can_install"]:
    #     analysis = analyze_commit(task.sha, f"{task.owner}/{task.repo}", bypass_cache=True)
    logger.debug("Analysis result: %s", analysis)
    return analysis
    # run_labels = gen_run_labels(task, runid=uuid.uuid4().hex)
    # logger.info("symbolic_build_and_validate: start for %s/%s@%s", task.owner, task.repo, task.sha)

    # try:
    #     # Phase A: Resolve dependencies symbolically
    #     logger.info("Resolving dependencies for %s/%s@%s", task.owner, task.repo, task.sha)
    #     t_resolve_start = time.time()

    #     try:
    #         resolution = analyze_commit(task.sha, f"{task.owner}/{task.repo}")
    #     except Exception as e:
    #         logger.error("Dependency resolution failed: %s", e, exc_info=True)
    #         return {
    #             "owner": task.owner,
    #             "repo": task.repo,
    #             "sha": task.sha,
    #             "image_name": task.with_tag("pkg").get_image_name(),
    #             "ok": False,
    #             "rc": 1,
    #             "stage": "resolution",
    #             "duration_s": time.time() - t_resolve_start,
    #             "stderr_tail": f"Resolution error: {e}",
    #             "stdout_tail": "",
    #             "attempts": [],
    #             "context_pickle": None,
    #         }

    #     logger.debug("Resolution took %.1fs", time.time() - t_resolve_start)

    #     if not resolution:
    #         logger.warning("analyze_commit returned None (no ASV config or incompatible)")
    #         return {
    #             "owner": task.owner,
    #             "repo": task.repo,
    #             "sha": task.sha,
    #             "image_name": task.with_tag("pkg").get_image_name(),
    #             "ok": False,
    #             "rc": 1,
    #             "stage": "resolution",
    #             "duration_s": time.time() - t_resolve_start,
    #             "stderr_tail": "No ASV config found or Python version incompatible",
    #             "stdout_tail": "",
    #             "attempts": [],
    #             "context_pickle": None,
    #         }

    #     if not resolution.get("can_install", False):
    #         logger.warning("Resolution succeeded but can_install=False")
    #         return {
    #             "owner": task.owner,
    #             "repo": task.repo,
    #             "sha": task.sha,
    #             "image_name": task.with_tag("pkg").get_image_name(),
    #             "ok": False,
    #             "rc": 1,
    #             "stage": "resolution",
    #             "duration_s": time.time() - t_resolve_start,
    #             "stderr_tail": f"Dependencies cannot be installed: {resolution.get('dry_run_log', 'Unknown error')}",
    #             "stdout_tail": "",
    #             "attempts": [],
    #             "context_pickle": None,
    #         }

    #     # Phase B: Generate env_payload from resolved dependencies
    #     final_dependencies = resolution.get("final_dependencies", [])
    #     logger.info("Resolved %d dependencies", len(final_dependencies))

    #     env_payload = {
    #         "constraints": final_dependencies,
    #         "to_install": [],
    #         "banned": []
    #     }

    #     # Create task with env_payload
    #     task_with_payload = Task(
    #         owner=task.owner,
    #         repo=task.repo,
    #         sha=task.sha,
    #         commit_date=task.commit_date,
    #         env_payload=json.dumps(env_payload),
    #         tag=task.tag,
    #     )

    #     # Phase C: Get default context (no custom script needed - deps injected via env)
    #     _, default_context = context_registry.get_default(tag="env")

    #     # Ensure ENV image exists
    #     repo_url = f"https://github.com/{task.owner}/{task.repo}"

    #     if not client.images.list(name=task_with_payload.with_tag("env").get_image_name()):
    #         logger.info("Building ENV image with resolved dependencies")
    #         env_res = build_once_with_context(
    #             client=client,
    #             task=task_with_payload.with_tag("env"),
    #             context=default_context,
    #             repo_url=repo_url,
    #             sha=task.sha,
    #             timeout_s=args.build_timeout,
    #             tail_chars=args.tail_chars,
    #             probe=True,
    #             pull=False,
    #             force=False,
    #             run_labels=run_labels,
    #         )

    #         if not env_res.ok:
    #             logger.error("ENV image build failed")
    #             return {
    #                 "owner": task.owner,
    #                 "repo": task.repo,
    #                 "sha": task.sha,
    #                 "image_name": task_with_payload.with_tag("pkg").get_image_name(),
    #                 "ok": False,
    #                 "rc": env_res.rc,
    #                 "stage": "env_build",
    #                 "duration_s": env_res.duration_s,
    #                 "stderr_tail": env_res.stderr_tail,
    #                 "stdout_tail": env_res.stdout_tail,
    #                 "attempts": [],
    #                 "context_pickle": None,
    #             }

    #     # Phase D: Build and validate PKG image
    #     logger.info("Building and validating PKG image")
    #     build_res = build_and_validate(
    #         client=client,
    #         task=task_with_payload,
    #         context=default_context,
    #         repo_url=repo_url,
    #         sha=task.sha,
    #         run_labels=run_labels,
    #         args=args,
    #     )

    #     # Phase E: Handle result
    #     if build_res.ok:
    #         logger.info("Build and validation successful!")
    #         with context_registry.get_lock():
    #             context_registry.register(task_with_payload.with_tag("pkg"), default_context)

    #         # Save pickle
    #         final_pickle = args.output_dir / f"{task.owner}-{task.repo}-{task.sha}-final.pkl"
    #         # Note: We don't have _save_pickle here, but we can skip it for now
    #         # or import it from context_synthesis

    #         return {
    #             "owner": task.owner,
    #             "repo": task.repo,
    #             "sha": task.sha,
    #             "image_name": task_with_payload.with_tag("pkg").get_image_name(),
    #             "ok": True,
    #             "rc": 0,
    #             "stage": "complete",
    #             "duration_s": build_res.duration_s,
    #             "stderr_tail": build_res.stderr_tail,
    #             "stdout_tail": build_res.stdout_tail,
    #             "attempts": [{
    #                 "attempt": 0,
    #                 "ok": True,
    #                 "rc": 0,
    #                 "stderr_tail": build_res.stderr_tail,
    #                 "stdout_tail": build_res.stdout_tail,
    #                 "building_data": "symbolic resolution (default context)",
    #             }],
    #             "context_pickle": str(final_pickle) if final_pickle else None,
    #             "resolution_info": {
    #                 "package_name": resolution.get("package_name"),
    #                 "package_version": resolution.get("package_version"),
    #                 "python_version": resolution.get("python_version"),
    #                 "num_dependencies": len(final_dependencies),
    #                 "resolution_strategy": resolution.get("resolution_strategy"),
    #             }
    #         }
    #     else:
    #         logger.warning("Build or validation failed")
    #         return {
    #             "owner": task.owner,
    #             "repo": task.repo,
    #             "sha": task.sha,
    #             "image_name": task_with_payload.with_tag("pkg").get_image_name(),
    #             "ok": False,
    #             "rc": build_res.rc,
    #             "stage": "build",
    #             "duration_s": build_res.duration_s,
    #             "stderr_tail": build_res.stderr_tail,
    #             "stdout_tail": build_res.stdout_tail,
    #             "attempts": [{
    #                 "attempt": 0,
    #                 "ok": False,
    #                 "rc": build_res.rc,
    #                 "stderr_tail": build_res.stderr_tail,
    #                 "stdout_tail": build_res.stdout_tail,
    #                 "building_data": "symbolic resolution (default context)",
    #             }],
    #             "context_pickle": None,
    #         }

    # finally:
    #     # Cleanup
    #     try:
    #         run_id = run_labels.get("datasmith.run", "unknown")
    #         remove_containers_by_label(client, run_id)
    #         fast_cleanup_run_artifacts(
    #             client,
    #             run_id,
    #             extra_image_refs=[
    #                 task.with_tag("env").get_image_name(),
    #                 task.with_tag("pkg").get_image_name(),
    #             ],
    #         )
    #     except Exception:
    #         logger.exception("Cleanup error")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="symbolic_context_synthesis",
        description="Build and validate benchmark containers using symbolic dependency resolution (no LLM).",
    )
    parser.add_argument(
        "--commits",
        type=Path,
        required=True,
        help="Path to a parquet file containing commit information.",
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
    parser.add_argument("--max-workers", type=int, default=8, help="Max parallel builds/runs.")
    parser.add_argument("--build-timeout", type=int, default=40 * 60, help="Seconds before aborting a docker build.")
    parser.add_argument("--tail-chars", type=int, default=4000, help="Chars of log tail to include in failure report.")
    parser.add_argument(
        "--limit-per-repo", type=int, default=5, help="Cap SHAs per repo (keeps your small-scale test). -1 = no limit."
    )
    parser.add_argument(
        "--context-registry",
        type=Path,
        required=True,
        help="Path to the context registry JSON file.",
    )
    return parser.parse_args()


def process_inputs(args: argparse.Namespace) -> dict[tuple[str, str], list[tuple[str, float, str]]]:
    """Load commits from parquet file."""
    commits = pd.read_parquet(args.commits)
    all_states = {}

    for _, row in commits.iterrows():
        repo_name = row["repo_name"]
        sha = row["sha"]
        has_asv = row.get("has_asv", True)

        if not has_asv:
            logger.debug("Skipping %s commit %s as it does not have ASV benchmarks.", repo_name, sha)
            continue

        owner, repo = repo_name.split("/")
        commit_date_unix: float = (
            0.0 if row.get("date", None) is None else dt.datetime.fromisoformat(row["date"]).timestamp()
        )
        env_payload = row.get("env_payload", "")

        if (owner, repo) not in all_states:
            all_states[(owner, repo)] = [(sha, commit_date_unix, env_payload)]
        else:
            all_states[(owner, repo)].append((sha, commit_date_unix, env_payload))

    return all_states


def prepare_tasks(
    all_states: dict[tuple[str, str], list[tuple[str, float, str]]],
    limit_per_repo: int,
    context_registry: ContextRegistry,
) -> list[Task]:
    """Create Task objects from commit data."""
    all_tasks: list[Task] = []

    for (owner, repo), tup in all_states.items():
        tasks = [
            Task(owner, repo, sha, commit_date=date, env_payload=env_payload) for sha, date, env_payload in sorted(tup)
        ]

        # Filter out already processed
        # tasks = [t for t in tasks if t.with_tag("pkg") not in context_registry]

        # Limit per repo
        if limit_per_repo > 0:
            tasks = random.sample(tasks, min(limit_per_repo, len(tasks)))

        all_tasks.extend(tasks)

    return all_tasks


def main(args: argparse.Namespace) -> None:
    client = get_docker_client()
    all_states = process_inputs(args)

    # Load or create context registry
    if not args.context_registry.exists():
        logger.warning("Context registry file %s does not exist; starting fresh", args.context_registry)
        context_registry_pth = Path("scratch/context_registry_init.json")
    else:
        context_registry_pth = args.context_registry

    context_registry = (
        ContextRegistry.load_from_file(path=context_registry_pth)
        if context_registry_pth.exists()
        else ContextRegistry()
    )

    # Build base image
    # logger.info("Building base image...")
    # base_tag = build_base_image(client, DockerContext())
    # logger.debug("Base image: %s", base_tag)

    # Prepare tasks
    tasks = prepare_tasks(all_states, args.limit_per_repo, context_registry)

    # Setup output
    (args.output_dir / "results").mkdir(parents=True, exist_ok=True)
    (args.output_dir / "errors.txt").unlink(missing_ok=True)
    (args.output_dir / "results.jsonl").unlink(missing_ok=True)

    logger.info("Starting work on %d tasks [%d workers]", len(tasks), args.max_workers)

    results: list[dict] = []

    if args.max_workers < 1:
        # Sequential execution
        for t in tasks:
            res = symbolic_build_and_validate(
                task=t,
                args=args,
                client=client,
                context_registry=context_registry,
            )
            results.append(res)

            with open(args.output_dir / "results.jsonl", "a", encoding="utf-8") as jf:
                jf.write(json.dumps(res) + "\n")

            # if res["ok"]:
            #     logger.info("SUCCESS %s/%s@%s", res["owner"], res["repo"], res["sha"])
            #     context_registry.save_to_file(path=args.context_registry)
    else:
        # Parallel execution
        with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
            futures = [
                ex.submit(
                    symbolic_build_and_validate,
                    task=t,
                    args=args,
                    client=client,
                    context_registry=context_registry,
                )
                for t in random.sample(tasks, len(tasks))
            ]

            for fut in as_completed(futures):
                res = fut.result()
                results.append(res)

                # if res["ok"]:
                #     logger.info("SUCCESS %s/%s@%s", res["owner"], res["repo"], res["sha"])
                #     context_registry.save_to_file(path=args.context_registry)

                with open(args.output_dir / "results.jsonl", "a", encoding="utf-8") as jf:
                    jf.write(json.dumps(res) + "\n")

    # Rollup
    # rollup = {
    #     r["image_name"]: {
    #         "owner": r["owner"],
    #         "repo": r["repo"],
    #         "sha": r["sha"],
    #         "stage": r["stage"],
    #         "ok": r["ok"],
    #         "rc": r["rc"],
    #         "duration": r.get("duration_s", None),
    #         "stderr_tail": r.get("stderr_tail", ""),
    #         "stdout_tail": r.get("stdout_tail", ""),
    #         "attempts": r.get("attempts", []),
    #     }
    #     for r in results
    # }

    # with open(args.output_dir / "all_files_by_image.json", "w", encoding="utf-8") as f:
    #     json.dump(rollup, f, indent=2)

    # Report failures
    # failed = [r for r in results if not r["ok"]]
    # if failed:
    #     print("\n=== FAILURES ===")
    #     for r in failed:
    #         print(f"{r['image_name']}: rc={r['rc']} stage={r['stage']}")
    #     print(f"\nDetails: {args.output_dir / 'errors.txt'}")
    # else:
    #     print("All containers validated successfully.")


if __name__ == "__main__":
    args = parse_args()
    main(args)
