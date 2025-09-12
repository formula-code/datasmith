from __future__ import annotations

import argparse
import datetime
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import asv
import pandas as pd

from datasmith.benchmark.collection import BenchmarkCollection
from datasmith.docker.context import ContextRegistry
from datasmith.docker.orchestrator import get_docker_client
from datasmith.docker.validation import Task, _err_lock, validate_one
from datasmith.logging_config import configure_logging
from datasmith.scrape.utils import _parse_commit_url

# logger = configure_logging()
logger = configure_logging(level=10, stream=open(Path(__file__).with_suffix(".log"), "w"))  # noqa: SIM115


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
    parser.add_argument("--build-timeout", type=int, default=30 * 60, help="Seconds before aborting a docker build.")
    parser.add_argument("--run-timeout", type=int, default=15 * 60, help="Seconds before aborting asv run.")
    parser.add_argument("--tail-chars", type=int, default=4000, help="Chars of log tail to include in failure report.")
    parser.add_argument(
        "--limit-per-repo", type=int, default=5, help="Cap SHAs per repo (keeps your small-scale test). -1 = no limit."
    )
    parser.add_argument(
        "--context-registry",
        type=Path,
        help="Path to the context registry JSON file.",
    )
    parser.add_argument(
        "--target",
        required=True,
        type=str,
        help="Tag to apply to built images. One of base,env,pkg,run",
        choices=["base", "env", "pkg", "run"],
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
        commits = (
            pd.read_json(args.commits, lines=True) if args.commits.suffix == ".jsonl" else pd.read_parquet(args.commits)
        )
        all_states = {}
        for _, row in commits.iterrows():
            repo_name = row["repo_name"]
            sha = row["sha"]
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


# === main (parallel) ===
def main(args: argparse.Namespace) -> None:
    client = get_docker_client()
    all_states = process_inputs(args)
    context_registry = ContextRegistry.load_from_file(path=args.context_registry)
    # Prepare tasks
    all_imgs = {t.get_image_name() for t in context_registry.registry}
    tasks: list[Task] = []
    for (owner, repo), uniq in all_states.items():
        limited = list(uniq)[: max(0, args.limit_per_repo)] if args.limit_per_repo > 0 else list(uniq)
        for sha, date in limited:
            task = Task(owner, repo, sha, commit_date=float(date))
            if task.with_tag("pkg").get_image_name() in all_imgs and (sha is not None):
                tasks.append(task.with_tag(args.target))
            else:
                logger.debug(f"main: skipping {task} not in context registry")

    (args.output_dir / "results").mkdir(parents=True, exist_ok=True)
    # reset outputs
    (args.output_dir / "errors.txt").unlink(missing_ok=True)
    (args.output_dir / "failures.jsonl").unlink(missing_ok=True)

    machine_defaults: dict[str, str] = asv.machine.Machine.get_defaults()  # pyright: ignore[reportAttributeAccessIssue]
    machine_defaults = {
        k: str(v.replace(" ", "_").replace("'", "").replace('"', "")) for k, v in machine_defaults.items()
    }

    logger.info("Starting parallel validation of %d tasks with %d workers", len(tasks), args.max_workers)
    results: list[dict] = []

    if args.max_workers < 1:
        for t in tasks:
            rec = validate_one(t, args, client, context_registry, machine_defaults)
            results.append(rec)
            with _err_lock, open(args.output_dir / "logs.jsonl", "a") as jf:
                jf.write(json.dumps(rec) + "\n")
        return
    else:
        with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
            futures = [ex.submit(validate_one, t, args, client, context_registry, machine_defaults) for t in tasks]
            for fut in as_completed(futures):
                rec = fut.result()
                results.append(rec)
                with _err_lock, open(args.output_dir / "logs.jsonl", "a") as jf:
                    jf.write(json.dumps(rec) + "\n")

    # Rollup (minimal, quick to read)
    rollup = {
        r["image_name"]: {
            "owner": r["owner"],
            "repo": r["repo"],
            "sha": r["sha"],
            "stage": r["stage"],
            "ok": r["ok"],
            "rc": r["rc"],
            "cmd_build": r["cmd_build"],
            "cmd_run": r["cmd_run"],
            "files": r.get("files", []),
        }
        for r in results
    }
    with open(args.output_dir / "all_files_by_image.json", "w") as f:
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
