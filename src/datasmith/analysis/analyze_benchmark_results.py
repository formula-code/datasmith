import json
import shutil
from pathlib import Path
from typing import Optional

import pandas as pd
from asv.commands.publish import Publish  # type: ignore[import-untyped]
from asv.config import Config  # type: ignore[import-untyped]
from asv.util import write_json  # type: ignore[import-untyped]
from git import Repo


def _get_all_commits_dict(all_commits_df: pd.DataFrame) -> dict:
    """Return a dict mapping commit_sha to metadata."""
    return all_commits_df.set_index("commit_sha", inplace=False).to_dict(orient="index")


def _update_dict(pth: Path, new_data: dict) -> None:
    """
    Update a JSON file at the given path with new data.

    Args:
        path (Path): Path to the JSON file.
        new_data (dict): New data to update the JSON file with.
    """
    if not pth.exists():
        pth.parent.mkdir(parents=True, exist_ok=True)
        with open(pth, "w", encoding="utf-8") as f:
            json.dump(new_data, f)
    else:
        with open(pth, "r+", encoding="utf-8") as f:
            try:
                saved_benchmarks = json.load(f)
            except json.JSONDecodeError:
                saved_benchmarks = {}
        saved_benchmarks.update(new_data)

        with open(pth, "w", encoding="utf-8") as f:
            json.dump(saved_benchmarks, f)


def _update_json(src_path: Path, dest_path: Path) -> None:
    """Load a JSON file and save it to dest_path using _update_dict."""
    with open(src_path, encoding="utf-8") as f:
        data = json.load(f)
    _update_dict(dest_path, data)


def _update_jsons(runid: Path, runid_newpath: Path, default_machine_name: str) -> Optional[dict]:
    """Update machine name in machine.json and params['machine'] in other json files."""
    machine_data = None
    old_file_names = [f.name for f in runid.iterdir()]
    for fname in old_file_names:
        src_file = runid / fname
        dest_file = runid_newpath / fname
        if fname == "machine.json":
            with open(src_file, encoding="utf-8") as f:
                machine_data = json.load(f)
            machine_data["machine"] = default_machine_name
            with open(dest_file, "w", encoding="utf-8") as f:
                json.dump(machine_data, f)
        elif fname.endswith(".json"):
            with open(src_file, encoding="utf-8") as f:
                run_data = json.load(f)
            if "params" in run_data and "machine" in run_data["params"]:
                run_data["params"]["machine"] = default_machine_name
            with open(dest_file, "w", encoding="utf-8") as f:
                json.dump(run_data, f)
    return machine_data


def _process_runid_folder(runid: Path, runid_newpath: Path, default_machine_name: Optional[str]) -> Optional[dict]:
    """Copy and update runid folder, handling machine name if needed."""
    machine_data = None
    if default_machine_name is not None:
        runid_newpath.mkdir(parents=True, exist_ok=True)
        machine_data = _update_jsons(runid, runid_newpath, default_machine_name)
    else:
        if runid_newpath.exists():
            shutil.rmtree(runid_newpath)
        shutil.copytree(runid, runid_newpath)
    return machine_data


def aggregate_benchmark_runs(
    all_commits_df: pd.DataFrame, results_dir: Path, output_dir: Path, default_machine_name: Optional[str] = None
) -> list[dict]:
    """
    Aggregates benchmark runs from the specified results directory and saves them to the output directory.

    Args:
        all_commits_df (pd.DataFrame): DataFrame containing commit metadata.
        results_dir (Path): Path to the directory containing benchmark results.
        output_dir (Path): Path to the directory where merged benchmarks will be saved.
    """
    stats = []
    all_commits_dict = _get_all_commits_dict(all_commits_df)

    for commit_pth in results_dir.glob(r'*/"[0-9].[0-9]*"/results/'):
        commit_id = commit_pth.parent.parent.name
        if commit_id not in all_commits_dict:
            continue
        commit_metadata = all_commits_dict[commit_id]
        repo_path = (commit_metadata["repo_name"]).replace("/", "_")
        repo_out_dir = output_dir / repo_path
        repo_out_dir.mkdir(parents=True, exist_ok=True)

        benchmarks_path = commit_pth / "benchmarks.json"
        asv_conf_path = commit_pth.parent / "asv.conf.json"
        if benchmarks_path.exists() and asv_conf_path.exists():
            _update_json(benchmarks_path, repo_out_dir / "benchmarks.json")
            _update_json(asv_conf_path, repo_out_dir / "asv.conf.json")
        n_runids = 0
        machine_data = None
        for runid in commit_pth.iterdir():
            if not runid.is_dir():
                continue
            n_runids += 1
            name = default_machine_name if default_machine_name is not None else runid.name
            runid_newpath = output_dir / repo_path / name
            machine_data = _process_runid_folder(runid, runid_newpath, default_machine_name) or machine_data

        if default_machine_name is not None and machine_data is not None:
            saved_machine_path = output_dir / repo_path / "machine.json"
            with open(saved_machine_path, "w", encoding="utf-8") as f:
                json.dump(machine_data, f)

        stats.append({
            "repo_path": repo_path,
            "metadata": commit_metadata,
            "commit_sha": commit_id,
            "n_runids": n_runids,
        })
    return stats


def publish_repo(
    repo_url: str,
    repo_local_dir: Path,
    asv_conf_path: Path,
    results_dir: Path,
    html_dir: Path,
    *,
    skip_if_present: bool = True,
) -> None:
    """
    Ensure *repo_local_dir* contains an up-to-date clone of *repo_url*,
    rewrite the ASV config at *asv_conf_path* to use the supplied
    directories, then publish the results.

    Parameters
    ----------
    repo_url : str
        Full Git URL, e.g. ``"https://github.com/pandas-dev/pandas.git"``.
    repo_local_dir : pathlib.Path
        Where the repo should live locally.
    asv_conf_path : pathlib.Path
        Path to the repository's ``asv.conf.json`` on disk.
    results_dir : pathlib.Path
        Directory for ASV's benchmark result files.
    html_dir : pathlib.Path
        Directory where ASV should write its HTML report.
    skip_if_present : bool, default True
        If *repo_local_dir* already exists, skip cloning and simply use it.
        Set to ``False`` to force a fresh clone each time.

    Raises
    ------
    FileNotFoundError
        If *asv_conf_path* does not exist.
    """
    # Clone or reuse the repository
    if repo_local_dir.exists():
        if skip_if_present:
            print(f"Repository {repo_local_dir} already exists - reusing.")
        else:
            print(f"Removing {repo_local_dir} for a fresh clone…")
            shutil.rmtree(repo_local_dir)
            Repo.clone_from(repo_url, repo_local_dir)
    else:
        Repo.clone_from(repo_url, repo_local_dir)

    # Load & patch asv.conf.json
    with asv_conf_path.open(encoding="utf-8") as f:
        asv_conf = json.load(f)

    asv_conf.update(
        repo=str(repo_local_dir.resolve()),
        results_dir=str(results_dir.resolve()),
        html_dir=str(html_dir.resolve()),
    )

    cfg = Config.from_json(asv_conf)
    write_json(path=asv_conf_path, data=cfg.__dict__, api_version=1)

    # Publish the results
    Publish.run(cfg)
