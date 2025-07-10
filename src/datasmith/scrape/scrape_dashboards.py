import datetime
import json
import os
import urllib.parse
from datetime import timezone
from pathlib import Path
from typing import Callable

import pandas as pd
from tqdm import tqdm

from datasmith.benchmark.collection import BenchmarkCollection
from datasmith.scrape.utils import dl_and_open


def make_graph_dir(param_dict: dict, all_keys: list, *, quote: bool) -> str:
    parts = []
    for k in all_keys:
        v = param_dict.get(k)
        seg = f"{k}-{v}" if v not in ("", None) else k
        if quote:
            seg = urllib.parse.quote(seg, safe="()-")
        parts.append(seg)
    return "graphs/" + "/".join(parts) + "/"


def _make_joiner(base_url: str) -> Callable[..., str]:
    """
    Return a function that joins paths correctly for either
    a remote dashboard (http/https/ftp/file) or a local folder.
    """
    parsed = urllib.parse.urlparse(base_url)

    # Remote dashboard → keep using urljoin
    if parsed.scheme:  # 'http', 'https', 'file', etc.
        # urljoin needs a trailing slash on the base or it will strip the
        # last path component on the first call.
        base_url_with_slash = base_url + "/" if not base_url.endswith("/") else base_url
        return lambda *parts: urllib.parse.urljoin(base_url_with_slash, "/".join(parts))

    # Local dashboard folder → fall back to os.path.join / pathlib
    base_path = Path(base_url).expanduser().resolve()
    return lambda *parts: str(base_path.joinpath(*parts))


def make_benchmark_from_html(base_url: str, html_dir: str, force: bool) -> BenchmarkCollection | None:
    """
    Extract benchmark metrics from an asv dashboard located either
    online (http/https) *or* on the local filesystem.
    """
    parsed = urllib.parse.urlparse(base_url)
    is_remote = bool(parsed.scheme)  # http / https / file → True
    join_path = _make_joiner(base_url)

    html_dir = os.path.abspath(html_dir)
    os.makedirs(html_dir, exist_ok=True)

    index_src = join_path("index.json")
    index_path = dl_and_open(index_src, html_dir, base=base_url, force=force)
    if not index_path:
        print(f"Failed to read index.json from {base_url}")
        return None
    with open(index_path, encoding="utf-8") as fh:
        index_data = json.load(fh)

    all_keys = sorted(index_data["params"])
    benchmarks = list(index_data["benchmarks"])
    param_sets = index_data["graph_param_list"]

    summaries = [join_path("graphs", "summary", f"{b}.json") for b in benchmarks]

    frames = []
    for p in tqdm(param_sets, desc="machines"):
        graph_dir = make_graph_dir(p, all_keys, quote=is_remote)
        for bench in tqdm(benchmarks, desc="benchmarks", leave=False):
            url = join_path(graph_dir, f"{bench}.json")
            local = dl_and_open(url, html_dir, base=base_url, force=force)
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
        summary_pth = dl_and_open(summary_url, html_dir, base=base_url, force=force)
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

    collection = BenchmarkCollection(
        base_url=base_url,
        collected_at=datetime.datetime.now(timezone.utc),
        modified_at=datetime.datetime.now(timezone.utc),
        param_keys=all_keys,
        index_data=index_data,
        benchmarks=all_benchmarks,
        summaries=all_summaries_df,
    )
    return collection
