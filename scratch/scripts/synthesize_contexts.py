from __future__ import annotations

import argparse
import datetime
import json
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import asv
import pandas as pd

from datasmith.agents.config import configure_agent_backends
from datasmith.agents.context_synthesis import agent_build_and_validate
from datasmith.benchmark.collection import BenchmarkCollection
from datasmith.docker.context import ContextRegistry, DockerContext, build_base_image
from datasmith.docker.orchestrator import get_docker_client
from datasmith.docker.validation import Task, _err_lock
from datasmith.logging_config import configure_logging

configure_agent_backends(PORTKEY_MODEL_NAME="@anthropic/claude-3-5-sonnet-latest")
# configure_agent_backends(PORTKEY_MODEL_NAME="@togetherai/meta-llama/Llama-3.3-70B-Instruct-Turbo")
# configure_agent_backends(PORTKEY_MODEL_NAME="@togetherai/deepseek-ai/DeepSeek-V3")

# logger = configure_logging(level=10)
logger = configure_logging(level=10, stream=open(Path(__file__).with_suffix(".tiny.log"), "w"))  # noqa: SIM115


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
    parser.add_argument("--max-workers", type=int, default=8, help="Max parallel builds/runs.")
    parser.add_argument("--max-steps", type=int, default=5, help="Number of ReACT steps to use.")
    parser.add_argument("--max-attempts", type=int, default=3, help="Max attempts per task (build+run).")
    parser.add_argument("--build-timeout", type=int, default=20 * 60, help="Seconds before aborting a docker build.")
    parser.add_argument("--run-timeout", type=int, default=15 * 60, help="Seconds before aborting asv run.")
    parser.add_argument("--tail-chars", type=int, default=4000, help="Chars of log tail to include in failure report.")
    parser.add_argument(
        "--max-similar-candidates",
        type=int,
        default=1,
        help="Number of similar candidates [sorted by recency] to try out before running agent. (default: 1)",
    )
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


def process_inputs(args: argparse.Namespace) -> dict[tuple[str, str], set[tuple[str, float]]]:
    if args.dashboard:
        dashboard = BenchmarkCollection.load(args.dashboard)
        all_states = {}
        for row in dashboard.commits.itertuples():
            owner, repo = row.repo_name.split("/")  # pyright: ignore[reportArgumentType, reportAttributeAccessIssue]
            sha = row.sha
            date_fmt = row.date  # 2019-10-19T11:32:20-04:00
            date_unit = 0.0 if date_fmt is None else datetime.datetime.fromisoformat(date_fmt).timestamp()  # pyright: ignore[reportArgumentType]
            if (owner, repo) not in all_states:
                all_states[(owner, repo)] = {(sha, date_unit)}
            else:
                all_states[(owner, repo)].add((sha, date_unit))
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
                logger.debug("Skipping %s commit %s as it does not have ASV benchmarks.", repo_name, sha)
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


def prepare_tasks(
    all_states: dict[tuple[str, str], set[tuple[str, float]]], limit_per_repo: int, context_registry: ContextRegistry
) -> list[Task]:
    all_tasks: list[Task] = []
    for (owner, repo), tup in all_states.items():
        tasks = list({Task(owner, repo, sha, commit_date=date) for sha, date in tup})
        tasks = list(filter(lambda t: t not in context_registry, tasks))
        if limit_per_repo > 0:
            tasks = random.sample(tasks, min(limit_per_repo, len(tasks)))
        all_tasks.extend(tasks)
    return all_tasks


def main(args: argparse.Namespace) -> None:
    client = get_docker_client()
    all_states = process_inputs(args)
    if not args.context_registry.exists():
        logger.warning("main: context registry file %s does not exist; starting fresh", args.context_registry)
        context_registry_pth = Path("scratch/context_registry_init.json")
    else:
        context_registry_pth = args.context_registry

    context_registry = (
        ContextRegistry.load_from_file(path=context_registry_pth)
        if context_registry_pth.exists()
        else ContextRegistry()
    )

    logger.info("Building base image...")
    base_tag = build_base_image(client, DockerContext())
    logger.debug("%s", base_tag)
    # os.environ["DOCKER_CACHE_FROM"] = base_tag

    # Prepare tasks
    tasks = prepare_tasks(all_states, args.limit_per_repo, context_registry)

    (args.output_dir / "results").mkdir(parents=True, exist_ok=True)
    # reset outputs
    (args.output_dir / "errors.txt").unlink(missing_ok=True)
    (args.output_dir / "results.jsonl").unlink(missing_ok=True)

    machine_defaults: dict[str, str] = asv.machine.Machine.get_defaults()  # pyright: ignore[reportAttributeAccessIssue]
    machine_defaults = {
        k: str(v.replace(" ", "_").replace("'", "").replace('"', "")) for k, v in machine_defaults.items()
    }
    logger.debug("main: machine_defaults keys=%d", len(machine_defaults))
    logger.info("main: Starting work on %d tasks[%d workers]", len(tasks), args.max_workers)

    results: list[dict] = []
    if args.max_workers < 1:
        for t in tasks:
            res = agent_build_and_validate(
                task=t,
                args=args,
                client=client,
                context_registry=context_registry,
                machine_defaults=machine_defaults,
                max_attempts=args.max_attempts,
            )
            results.append(res)
            with _err_lock, open(args.output_dir / "results.jsonl", "a", encoding="utf-8") as jf:
                jf.write(json.dumps(res) + "\n")

            if int(res["rc"]) != 1:
                logger.info("main: SUCCESS %s/%s@%s", res["owner"], res["repo"], res["sha"])
                context_registry.save_to_file(path=args.context_registry)
    else:
        with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
            futures = [
                ex.submit(
                    agent_build_and_validate,
                    task=t,
                    args=args,
                    client=client,
                    machine_defaults=machine_defaults,
                    max_attempts=args.max_attempts,
                    context_registry=context_registry,
                )
                for t in tasks
            ]
            for fut in as_completed(futures):
                res = fut.result()
                results.append(res)
                if int(res["rc"]) != 1:
                    logger.info("main: SUCCESS %s/%s@%s", res["owner"], res["repo"], res["sha"])
                    context_registry.save_to_file(path=args.context_registry)

                with _err_lock, open(args.output_dir / "results.jsonl", "a", encoding="utf-8") as jf:
                    jf.write(json.dumps(res) + "\n")

    # Rollup (minimal, quick to read)
    rollup = {
        r["image_name"]: {
            "owner": r["owner"],
            "repo": r["repo"],
            "sha": r["sha"],
            "stage": r["stage"],
            "ok": r["ok"],
            "rc": r["rc"],
            "duration": r.get("duration_s", None),
            "stderr_tail": r.get("stderr_tail", ""),
            "stdout_tail": r.get("stdout_tail", ""),
            "attempts": r.get("attempts", []),
            "files": r.get("files", []),
        }
        for r in results
    }
    with open(args.output_dir / "all_files_by_image.json", "w", encoding="utf-8") as f:
        json.dump(rollup, f, indent=2)

    failed = [r for r in results if not r["ok"]]
    if failed:
        print("\n=== FAILURES ===")
        for r in failed:
            print(f"{r['image_name']}: rc={r['rc']} stage={r['stage']}")
        print(f"\nDetails: {args.output_dir / 'errors.txt'}")
    else:
        print("All containers validated successfully.")


if __name__ == "__main__":
    args = parse_args()
    main(args)
