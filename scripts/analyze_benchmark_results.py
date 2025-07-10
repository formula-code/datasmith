import argparse
import json
from pathlib import Path

import pandas as pd

from datasmith.analysis.analyze_benchmark_results import aggregate_benchmark_runs, publish_repo
from datasmith.scrape.scrape_dashboards import extract_dashboard_results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collects benchmark results from parallel worker runs and creates a merged benchmark file."
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        required=True,
        help="Path to the directory containing benchmark results in format `<results-dir>/<commit-id>/<python-version>/results/`",
    )
    parser.add_argument(
        "--commit-metadata",
        type=Path,
        required=True,
        help="Path to the jsonl file containing the commit IDs and associated metadata (e.g. repo_name)",
    )
    parser.add_argument(
        "--output-dir", type=Path, required=True, help="Path to the directory where merged benchmarks will be saved"
    )
    parser.add_argument(
        "--default-machine-name",
        type=str,
        default=None,
        help="Default machine name to use for all runs. If not provided, the machine name from the run ID will be used.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    # Load results from the specified directory
    all_commits = []
    with open(args.commit_metadata, encoding="utf-8") as f:
        for line in f:
            commit = json.loads(line.strip().replace("None", "null"))
            all_commits.append(commit)
    all_commits_df = pd.DataFrame(all_commits)
    all_commits_df["commit_sha"] = all_commits_df["commit_sha"].astype("string")

    # Aggregate benchmark runs and save them to the output directory
    stats = aggregate_benchmark_runs(
        all_commits_df, args.results_dir, args.output_dir / "results", default_machine_name=args.default_machine_name
    )

    # Run asv publish on the output directory
    repos = {stat["repo_path"]: stat for stat in stats}
    for repo_path, stat in repos.items():
        contains_jsons = any(
            f.name not in ["machine.json", "asv.conf.json"]
            for f in (args.output_dir / "results" / repo_path).glob("*/*.json")
        )
        if not contains_jsons:
            print(f"Warning: No benchmark results found for {repo_path}. Skipping dashboard creation.")
            continue
        repo_url = f"https://github.com/{stat['metadata']['repo_name']}.git"
        publish_repo(
            repo_url=repo_url,
            repo_local_dir=(args.output_dir / "repos" / repo_path).resolve(),
            asv_conf_path=(args.output_dir / "results" / repo_path / "asv.conf.json").resolve(),
            results_dir=(args.output_dir / "results" / repo_path).resolve(),
            html_dir=(args.output_dir / "html" / repo_path).resolve(),
        )
        # make a dashboard for the repo
        extract_dashboard_results(
            base_url=str((args.output_dir / "html" / repo_path).resolve()),
            dl_dir=str((args.output_dir / "html" / repo_path).resolve()),
            force=False,
        )
    print(f"Benchmark results aggregated and saved to {(args.output_dir / 'html').resolve()}.")


if __name__ == "__main__":
    main()
