import json
import os
import urllib.parse

import pandas as pd
from tqdm import tqdm

from datasmith.scrape.utils import dl_and_open


def make_graph_dir(param_dict: dict, all_keys: list) -> str:
    parts = []
    for k in all_keys:
        v = param_dict.get(k)
        seg = f"{k}-{v}" if v not in ("", None) else k
        parts.append(urllib.parse.quote(seg, safe="()-"))
    return "graphs/" + "/".join(parts) + "/"


def scrape_public_dashboard(base_url: str, dl_dir: str, force: bool) -> None:
    """
    Scrapes bencharked numbers from a publically available asv dashboard.

    All asv repositories follow the same structure, so this function
    can be used to scrape any asv dashboard.
    """
    dl_dir = os.path.abspath(dl_dir)
    os.makedirs(dl_dir, exist_ok=True)

    index_path = dl_and_open(urllib.parse.urljoin(base_url, "index.json"), dl_dir, base=base_url, force=force)
    if not index_path:
        print(f"Failed to download index.json from {base_url}. Check the URL or your internet connection.")
        return
    with open(index_path, encoding="utf-8") as fh:
        index_data = json.load(fh)

    all_keys = sorted(index_data["params"])
    benchmarks = list(index_data["benchmarks"])
    param_sets = index_data["graph_param_list"]
    summaries = [urllib.parse.urljoin(base_url, f"graphs/summary/{x}.json") for x in index_data["benchmarks"]]

    frames = []
    for p in tqdm(param_sets, desc="machines"):
        graph_dir = make_graph_dir(p, all_keys)
        for bench in tqdm(benchmarks, desc="benchmarks", leave=False):
            url = urllib.parse.urljoin(base_url, graph_dir + bench + ".json")
            local = dl_and_open(url, dl_dir, base=base_url, force=force)
            if local is None:
                continue
            try:
                with open(local, encoding="utf-8") as fh:
                    data = json.load(fh)
            except json.JSONDecodeError as e:
                print(f"Failed to decode {local}: {e}")
                continue
            df = pd.DataFrame(data, columns=["revision", "time"])
            df["hash"] = df["revision"].astype(str).map(index_data["revision_to_hash"])
            df["benchmark"] = bench
            df["machine"] = p["machine"]
            df["date"] = df["revision"].astype(str).map(index_data["revision_to_date"])
            frames.append(df)

    all_benchmarks = pd.concat(frames, ignore_index=True)
    print(f"Collected {len(all_benchmarks):,} rows from {len(frames):,} benchmark files.")

    all_summaries = []
    for summary_url in tqdm(summaries, desc="summaries"):
        summary_pth = dl_and_open(summary_url, dl_dir, base=base_url, force=force)
        if summary_pth is None:
            continue
        try:
            with open(summary_pth, encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            print(f"Failed to decode {summary_pth}: {e}")
            continue
        benchmark_name = os.path.basename(summary_pth).replace(".json", "")
        df = pd.DataFrame(data, columns=["revision", "time"])
        df["hash"] = df["revision"].astype("str").map(index_data["revision_to_hash"])
        df["date"] = df["revision"].astype(str).map(index_data["revision_to_date"])
        df["benchmark"] = benchmark_name
        all_summaries.append(df)

    all_summaries_df = pd.concat(all_summaries, ignore_index=True)

    bench_csv = os.path.join(dl_dir, "all_benchmarks.csv")
    summ_csv = os.path.join(dl_dir, "all_summaries.csv")
    all_benchmarks.to_csv(bench_csv, index=False)
    all_summaries_df.to_csv(summ_csv, index=False)
    print(f"Saved benchmark CSV: {bench_csv}\nSaved summary CSV: {summ_csv}")
    print(f"Saved {len(all_benchmarks):,} benchmark rows and {len(all_summaries_df):,} summary rows to '{dl_dir}'")
