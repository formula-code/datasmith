from __future__ import annotations

import argparse
import datetime
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import asv
import pandas as pd

from datasmith.benchmark.collection import BenchmarkCollection
from datasmith.core.models import Task
from datasmith.docker.context import ContextRegistry
from datasmith.docker.orchestrator import get_docker_client
from datasmith.docker.validation import DockerValidator, ValidationConfig
from datasmith.logging_config import configure_logging
from datasmith.scrape.utils import _parse_commit_url

# logger = configure_logging()
logger = configure_logging(level=10, stream=open(Path(__file__).with_suffix(".log"), "w"))  # noqa: SIM115

ignore_list = {
    "120db4474ee6194efd83cd799565e233c15a84c9",
    "24f7db72a3c93a4d0cfa3763724c01ac65d412c6",
    "24936816994d14b616d4053d618d7de7419c6f89",
    "6db283c367ab178e5464bb5ff323bdc6c9ebce1f",
    "3c15cfdf0e961c4e8f74a205bac6d34e0930f988",
    "62aea0f06dc8bb7b029f551ef2bcfb8b08ee8ddf",
    "8ed7dae2a3a76bdae0da402c12111bf033124b55",
    "4c5f4ca89dc74727c846d2707df84d00f0a39883",
    "d1a245c1bf58f503403c23e64c641f85045534b9",
    "2096b2897b3692c3f4715e5e65e35fbb01e55eea",
    "1bcdfd05ff4bcb071639c149099876e9cf072aa3",
    "63249f2aa95ef0b0300ea2f1cc68200cc8b13484",
    "a393c31931af4fc69ea0006fd851eeb00175be3c",
    "4a3fb4bed5bf5315dac37416c42c9b8c977a3d8c",
    "c1e57c9e9e7ece934a3e157cf0ee32ea1ae5c029",
    "882fa9cd7005ccf2249f04f38a2f2fd661d4e62a",
    "80795dfe5db3a6c089a2ffc31df5b8a324f8dd53",
    "d1c64045921d7f5b4fe0609b5bc428219c279e5e",
    "938763edea1ca4c5343f4174f0af4e72088ccbda",
    "9c5b9ee823702d937d008d761dbe9ae8872f2259",
    "c38e1f1aa9efbfb68b42670de84a4450d04ffd4a",
    "cf6de581e1c27f06107b5be9e7f2923d7bbc98a6",
    "5da9eb726c915412928ee44a0631a74bda614556",
    "25684be44c950116c5006371c0ab5a97c1793108",
    "bb42fc0c700533a67f03ea189c24e57d87d48410",
    "c26935cd9ffd1676403b43f88dfafee0935b979c",
    "d9f9e12199bbd5826406e9a6adef62b68867af61",
    "e982297ffa6d814994a5880f1c12d83af814ede0",
    "ee4ceec8e313c83bbbb53309e412b470ab13a642",
    "12b77ad2e2d8ee766566e19d66845ca02e378ff4",
    "42c25421dccb5ede399472db44131190f41aecf2",
    "478fa28f7ce440d35b2cd23e072e1f731a78aef3",
    "f1de9c74bf49249e4890d768569729d157f5ae11",
    "5b3a3b440525aa5038e2cead1f9b4f23a610cd9f",
    "915e6d3c1b36e22b3d62bc6144fdb01066dd00ce",
    "57c6533d4d54326c7719a792aa9118dfc1079555",
    "2f44dbaba3aac3b47b5da351b36634dffab09e98",
    "92a02040139784392761ff0ac336c0c406e1266d",
    "a6f891bc789b337fa14078722c723261009eafd3",
    "659de5fea920f15e88bed4cf43dc13df8569abad",
    "ad4d1f7cf16e0207d61881df5f7a846052f38559",
    "d1d453c2680f9fd15fbc6e28f3d6788741b6e629",
    "dc5efde931273f0b98d732d3545f18a7b4c029ae",
    "11e3dc2738befa5803aea676f1b68082a8b24a8b",
    "cb8ac5e33ac292cad9641cd01e7283806e63e635",
    "b160966ecfe764c45c3b7b92f03b9c550ed5f2b6",
    "ed1d1f9134185848795e78301ce6582df338e1b2",
    "fd9a9ea7b242093314384d134135ebdd721b5daf",
    "701537ecca85a333449814c82ac2b78db5f534a8",
    "3c91a3e1704c8aa7f1258aa30892040df9d952f4",
    "badbf70324274bdb4299d8c64d3d83a26be2d4c0",
    "667df88cffc320d693dc3fd5ccb848c9a5b06039",
    "37ce99a4ab6f066f1363c33d1ec6f2b4c6c4a583",
    "55b55afb2b6a8d1cc1d69ba1de0b4a370fa34c3a",
    "c22be1defcf3e59ebd79ed3e479ada8ea558f601",
    "af930a9aa4f55361a66051ac9ef151cda3742bf8",
    "6df0f13a59e83651be0272fcd308e1777d9a8d74",
    "7b595569b26f4aa65a74a971ef428f4f071f48c4",
    "6291f668fc0f308e4f048d23ac42e6f3c9f4a1b1",
    "ee5d94e0a05da11272a4af1cd731f9822565048e",
}


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


def process_inputs(args: argparse.Namespace) -> dict[tuple[str, str], set[tuple[str, float, str]]]:
    if args.dashboard:
        dashboard = BenchmarkCollection.load(args.dashboard)
        all_states = {}
        for owner, repo, sha in dashboard.enriched_breakpoints.url.apply(_parse_commit_url):
            owner = owner.lower()
            repo = repo.lower()
            sha = sha.lower()
            env_payload = ""  # Dashboard doesn't have env_payload, use empty string
            if (owner, repo) not in all_states:
                all_states[(owner, repo)] = {(sha, 0.0, env_payload)}
            else:
                all_states[(owner, repo)].add((sha, 0.0, env_payload))
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
            env_payload = row.get("env_payload", "")
            if (owner, repo) not in all_states:
                all_states[(owner, repo)] = [(sha, commit_date_unix, env_payload)]
            else:
                all_states[(owner, repo)].append((sha, commit_date_unix, env_payload))
    else:
        raise ValueError("Either --dashboard or --commits must be provided.")
    return all_states


def main(args: argparse.Namespace) -> None:
    client = get_docker_client()
    all_states = process_inputs(args)
    context_registry = ContextRegistry.load_from_file(path=args.context_registry)
    # Prepare tasks
    tasks: set[Task] = set()
    for (owner, repo), uniq in all_states.items():
        limited = list(uniq)[: max(0, args.limit_per_repo)] if args.limit_per_repo > 0 else list(uniq)
        for sha, date, env_payload in limited:
            task = Task(owner, repo, sha, commit_date=float(date), env_payload=env_payload)
            if (task.with_tag("pkg") in context_registry) and (task.sha not in ignore_list):
                _, task_instance = context_registry.get_with_task_instance(task)
                if task_instance and isinstance(task_instance.metadata, dict):
                    env_payload = str(task_instance.metadata.get("env_payload", env_payload))
                tasks.add(Task(owner, repo, sha, commit_date=float(date), env_payload=env_payload))
            else:
                logger.debug(f"main: skipping {task} not in context registry")
    tasks = list(tasks)

    (args.output_dir / "results").mkdir(parents=True, exist_ok=True)
    # reset outputs
    (args.output_dir / "errors.txt").unlink(missing_ok=True)
    (args.output_dir / "failures.jsonl").unlink(missing_ok=True)

    machine_defaults: dict[str, str] = asv.machine.Machine.get_defaults()  # pyright: ignore[reportAttributeAccessIssue]
    machine_defaults = {
        k: str(v.replace(" ", "_").replace("'", "").replace('"', "")) for k, v in machine_defaults.items()
    }

    config = ValidationConfig(
        output_dir=args.output_dir,
        build_timeout=args.build_timeout,
        run_timeout=args.run_timeout,
        tail_chars=args.tail_chars,
    )
    validator = DockerValidator(
        client=client,
        context_registry=context_registry,
        machine_defaults=machine_defaults,
        config=config,
    )

    logger.info("Starting parallel validation of %d tasks with %d workers", len(tasks), args.max_workers)
    results: list[dict] = []

    if args.max_workers < 1:
        for t in tasks:
            rec = validator.validate_task(t.with_tag(args.target))
            results.append(rec)
            with validator.error_lock, open(args.output_dir / "logs.jsonl", "a") as jf:
                jf.write(json.dumps(rec) + "\n")
        return
    else:
        with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
            futures = [ex.submit(validator.validate_task, t.with_tag(args.target)) for t in tasks]
            for fut in as_completed(futures):
                rec = fut.result()
                results.append(rec)
                with validator.error_lock, open(args.output_dir / "logs.jsonl", "a") as jf:
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
