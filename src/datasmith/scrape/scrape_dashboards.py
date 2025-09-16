import bisect
import contextlib
import datetime
import json
import os
import tempfile
import urllib.parse
from datetime import timezone
from pathlib import Path
from typing import Callable

import asv
import pandas as pd
from git import Repo
from tqdm import tqdm

from datasmith.benchmark.collection import BenchmarkCollection
from datasmith.docker.context import Task
from datasmith.logging_config import get_logger
from datasmith.scrape.utils import dl_and_open

logger = get_logger("scrape.scrape_dashboards")

KNOWN_COMMIT_URLS = {
    "dask": "https://github.com/dask/dask/",
    "distributed": "https://github.com/dask/distributed/",
    "pymc3": "https://github.com/pymc-devs/pymc3/",
    "joblib": "https://github.com/joblib/joblib/",
    "sklearn": "https://github.com/scikit-learn/scikit-learn/",
    "astropy": "https://github.com/astropy/astropy/",
    # --- dist→GitHub owner/repo ---
    "opencv-python": "https://github.com/opencv/opencv-python/",
    "pyyaml": "https://github.com/yaml/pyyaml/",
    "beautifulsoup4": None,  # No official GitHub; canonical home is crummy.com / Launchpad
    "pillow": "https://github.com/python-pillow/Pillow/",
    "mysqlclient": "https://github.com/PyMySQL/mysqlclient/",
    "psycopg2-binary": "https://github.com/psycopg/psycopg2/",
    "opencv-contrib-python": "https://github.com/opencv/opencv-python/",
    "protobuf": "https://github.com/protocolbuffers/protobuf/",
    "apache-beam": "https://github.com/apache/beam/",
    # --- core scientific stack ---
    "scikit-learn": "https://github.com/scikit-learn/scikit-learn/",
    "numpy": "https://github.com/numpy/numpy/",
    "pandas": "https://github.com/pandas-dev/pandas/",
    "scipy": "https://github.com/scipy/scipy/",
    "scikit-image": "https://github.com/scikit-image/scikit-image/",
    "pywt": "https://github.com/PyWavelets/pywt/",
    "xarray": "https://github.com/pydata/xarray/",
    "bottleneck": "https://github.com/pydata/bottleneck/",
    "h5py": "https://github.com/h5py/h5py/",
    "networkx": "https://github.com/networkx/networkx/",
    "shapely": "https://github.com/shapely/shapely/",
    # --- ML / stats / optimization / viz ---
    "optuna": "https://github.com/optuna/optuna/",
    "arviz": "https://github.com/arviz-devs/arviz/",
    "pymc": "https://github.com/pymc-devs/pymc/",
    "kedro": "https://github.com/kedro-org/kedro/",
    "modin": "https://github.com/modin-project/modin/",
    "napari": "https://github.com/napari/napari/",
    "deepchecks": "https://github.com/deepchecks/deepchecks/",
    "voyager": "https://github.com/spotify/voyager/",
    "warp": "https://github.com/NVIDIA/warp/",
    "newton": "https://github.com/newton-physics/newton/",
    # --- domain / ecosystem libs ---
    "geopandas": "https://github.com/geopandas/geopandas/",
    "cartopy": "https://github.com/SciTools/cartopy/",
    "iris": "https://github.com/SciTools/iris/",
    "anndata": "https://github.com/scverse/anndata/",
    "scanpy": "https://github.com/scverse/scanpy/",
    "sunpy": "https://github.com/sunpy/sunpy/",
    "pvlib-python": "https://github.com/pvlib/pvlib-python/",
    "PyBaMM": "https://github.com/pybamm-team/PyBaMM/",
    "momepy": "https://github.com/pysal/momepy/",
    "satpy": "https://github.com/pytroll/satpy/",
    "pydicom": "https://github.com/pydicom/pydicom/",
    "pynetdicom": "https://github.com/pydicom/pynetdicom/",
    # --- file formats / IO / infra ---
    "asdf": "https://github.com/asdf-format/asdf/",
    "arrow": "https://github.com/apache/arrow/",
    "ArcticDB": "https://github.com/man-group/ArcticDB/",
    "arctic": "https://github.com/man-group/arctic/",
    # --- web / frameworks / utils ---
    "django-components": "https://github.com/django-components/django-components/",
    "h11": "https://github.com/python-hyper/h11/",
    "tqdm": "https://github.com/tqdm/tqdm/",
    "rich": "https://github.com/Textualize/rich/",
    "posthog": "https://github.com/PostHog/posthog/",
    "datalad": "https://github.com/datalad/datalad/",
    "ipyparallel": "https://github.com/ipython/ipyparallel/",
    # --- numerical / symbolic / control ---
    "autograd": "https://github.com/HIPS/autograd/",
    "python-control": "https://github.com/python-control/python-control/",
    "loopy": "https://github.com/inducer/loopy/",
    "thermo": "https://github.com/CalebBell/thermo/",
    "chempy": "https://github.com/bjodah/chempy/",
    "adaptive": "https://github.com/python-adaptive/adaptive/",
    # --- scientific image / signal ---
    "metric-learn": "https://github.com/metric-learn/metric-learn/",
    # --- quantum / physics ---
    "Cirq": "https://github.com/quantumlib/Cirq/",
    "memray": "https://github.com/bloomberg/memray/",
    "devito": "https://github.com/devitocodes/devito/",
    # --- bio / chem / data ---
    "sourmash": "https://github.com/sourmash-bio/sourmash/",
    "dipy": "https://github.com/dipy/dipy/",
    # --- protocol buffers / codegen / outlines ---
    "python-betterproto": "https://github.com/danielgtaylor/python-betterproto/",
    "outlines": "https://github.com/dottxt-ai/outlines/",
    # --- DS viz / raster ---
    "datashader": "https://github.com/holoviz/datashader/",
    "xarray-spatial": "https://github.com/makepath/xarray-spatial/",
    # --- misc ---
    "enlighten": "https://github.com/Rockhopper-Technologies/enlighten/",
    "xorbits": "https://github.com/xorbitsai/xorbits/",
    "lmfit-py": "https://github.com/lmfit/lmfit-py/",
    "mdanalysis": "https://github.com/MDAnalysis/mdanalysis/",
    "nilearn": "https://github.com/nilearn/nilearn/",
}


def noisy_search(noisy_key: str) -> str | None:
    """
    checks to see if any part of the noisy key is in one of the entries in KNOWN_COMMIT_URLS
    """
    for key in KNOWN_COMMIT_URLS:
        if key in noisy_key:
            return KNOWN_COMMIT_URLS[key]
    return None


def get_commit_url_from_index(index_data: dict) -> str | None:
    commit_url = None
    if pot_commit_url := index_data.get("show_commit_url"):
        # get the commit_url
        try:
            if "/commit" in pot_commit_url:
                commit_url = pot_commit_url.replace("/commit", "/").replace("/tree/", "/")
            elif index_data.get("project") in KNOWN_COMMIT_URLS:
                commit_url = KNOWN_COMMIT_URLS[index_data["project"]]
            elif match := noisy_search(index_data.get("project", "")):
                logger.warning("Using fuzzy match for commit URL: %s -> %s", index_data.get("project", ""), match)
                commit_url = match
            elif match := noisy_search(index_data.get("project_url", "")):
                logger.warning("Using fuzzy match for commit URL: %s -> %s", index_data.get("project_url", ""), match)
                commit_url = match
            else:
                logger.warning("Could not find known commit URL for project %s", index_data.get("project"))
        except Exception as e:
            logger.warning("Failed to parse commit URL %s: %s", pot_commit_url, e)
    return commit_url


def get_taskname_from_index(index_data: dict) -> Task:
    commit_url = get_commit_url_from_index(index_data)
    if commit_url is None:
        return Task.default_task()
    owner, repo = commit_url.strip().strip("/").split("/")[-2:]
    task = Task(owner=owner, repo=repo, sha=None)
    return task


# def make_graph_dir(param_dict: dict, all_keys: list, *, quote: bool) -> str:
#     parts = []
#     for k in all_keys:
#         v = param_dict.get(k)
#         seg = f"{k}-{v}" if v not in ("", None) else k
#         if quote:
#             seg = urllib.parse.quote(seg, safe="()-")
#         parts.append(seg)
#     return "graphs/" + "/".join(parts) + "/"


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


def _revlist_with_author_ts(repo: Repo) -> tuple[list[str], list[int]]:
    """
    Return (hashes, author_ts_sec) in ASV order:
    git rev-list --all --date-order --reverse --format=%H %at

    We parse only the lines that look like "<40-hex> <int>".
    """
    out = repo.git.rev_list("--all", "--date-order", "--reverse", "--format=%H %at")
    hashes: list[str] = []
    ats: list[int] = []
    for line in out.splitlines():
        parts = line.strip().split()
        # Robust filter: two tokens, first looks like a 40-hex sha1
        if len(parts) == 2 and len(parts[0]) == 40 and all(c in "0123456789abcdefABCDEF" for c in parts[0]):
            h, at = parts
            hashes.append(h)
            ats.append(int(at))
    return hashes, ats


def _build_segments_from_sparse_hashes(
    hashes: list[str], rev2hash_sparse_int: dict[int, str]
) -> tuple[list[tuple[int, float, int]], Callable[[int], int]]:
    """
    Build piecewise-constant (start_rev, end_rev, delta) segments using known (revision->hash) anchors.
    delta = idx_in_revlist - revision
    """
    idx_by_hash = {h: i for i, h in enumerate(hashes)}
    anchors = sorted((int(r), idx_by_hash[h]) for r, h in rev2hash_sparse_int.items() if h in idx_by_hash)
    if not anchors:
        raise RuntimeError("No anchors available to build segments")

    segments: list[tuple[int, float, int]] = []
    start_rev, start_idx = anchors[0]
    cur_delta = start_idx - start_rev

    for rev, idx in anchors[1:]:
        delta = idx - rev
        if delta != cur_delta:
            segments.append((start_rev, rev, cur_delta))
            start_rev, cur_delta = rev, delta
    segments.append((start_rev, float("inf"), cur_delta))

    seg_starts = [s[0] for s in segments]

    def delta_for_rev(r: int) -> int:
        j = bisect.bisect_right(seg_starts, r) - 1
        return segments[max(j, 0)][2]

    return segments, delta_for_rev


def _compute_dense_mappings(  # noqa: C901
    owner: str,
    repo_name: str,
    revisions_needed: set[int],
    revision_to_hash_sparse: dict[str, str] | None,
    revision_to_date_sparse: dict[str, int] | None = None,
) -> tuple[dict[int, str], dict[int, int]]:
    """
    Clone once, enumerate commits in ASV order, build piecewise segments from sparse hash anchors,
    and return dense revision->hash and revision->date(ms) dicts for the revisions you need.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        repo = Repo.clone_from(f"https://github.com/{owner}/{repo_name}.git", str(Path(tmpdir) / repo_name))
        hashes, ats_sec = _revlist_with_author_ts(repo)

    # Sanity: ensure sparse anchors exist in this history (if provided)
    if revision_to_hash_sparse:
        unknown = [h for h in revision_to_hash_sparse.values() if h not in set(hashes)]
        if unknown:
            logger.warning(
                "Some sparse anchor hashes are missing from cloned repo (%s shown). History drift?", unknown[:3]
            )

    # Build segments from hash anchors (most reliable)
    if revision_to_hash_sparse and len(revision_to_hash_sparse) > 0:
        rev2hash_sparse_int = {int(k): v for k, v in revision_to_hash_sparse.items()}
        segments, delta_for_rev = _build_segments_from_sparse_hashes(hashes, rev2hash_sparse_int)
    else:
        # Very rare fallback: try dates as anchors if no hashes provided
        if not revision_to_date_sparse:
            raise RuntimeError("Neither revision_to_hash nor revision_to_date sparse anchors were provided.")
        # Map timestamps (ms) to indices (first occurrence)
        ts_ms = [t * 1000 for t in ats_sec]
        idx_by_ts: dict[int, int] = {}
        for i, t in enumerate(ts_ms):
            idx_by_ts.setdefault(t, i)
        anchors = []
        for rk, tms in revision_to_date_sparse.items():
            r = int(rk)
            if tms in idx_by_ts:
                anchors.append((r, idx_by_ts[tms]))
        if not anchors:
            raise RuntimeError("Could not derive anchors from dates.")
        anchors.sort()
        segments = []
        start_rev, start_idx = anchors[0]
        cur_delta = start_idx - start_rev
        for rev, idx in anchors[1:]:
            delta = idx - rev
            if delta != cur_delta:
                segments.append((start_rev, rev, cur_delta))
                start_rev, cur_delta = rev, delta
        segments.append((start_rev, float("inf"), cur_delta))
        seg_starts = [s[0] for s in segments]

        def delta_for_rev(r: int) -> int:
            j = bisect.bisect_right(seg_starts, r) - 1
            return segments[max(j, 0)][2]

    n = len(hashes)
    rev2hash_dense: dict[int, str] = {}
    rev2date_dense: dict[int, int] = {}
    for r in revisions_needed:
        i = r + delta_for_rev(r)
        if 0 <= i < n:
            rev2hash_dense[r] = hashes[i]
            rev2date_dense[r] = ats_sec[i] * 1000  # ASV stores ms

    # Optional: log mismatches against sparse (helps diagnose drift)
    if revision_to_hash_sparse:
        mism = [
            (r, revision_to_hash_sparse[str(r)], rev2hash_dense.get(r))
            for r in {int(k) for k in revision_to_hash_sparse}
            if rev2hash_dense.get(r) and rev2hash_dense[r] != revision_to_hash_sparse[str(r)]
        ]
        if mism:
            logger.info("Anchor mismatches after piecewise mapping: %d", len(mism))

    return rev2hash_dense, rev2date_dense


def make_benchmark_from_html(base_url: str, html_dir: str, force: bool) -> BenchmarkCollection | None:  # noqa: C901
    """
    Extract benchmark metrics from an asv dashboard located either
    online (http/https) *or* on the local filesystem. Uses piecewise
    reconstruction to map revision->hash and revision->date(ms).
    """
    join_path = _make_joiner(base_url)

    html_dir = os.path.abspath(html_dir)
    os.makedirs(html_dir, exist_ok=True)

    index_src = join_path("index.json")
    index_path = dl_and_open(index_src, html_dir, base=base_url, force=force)
    if not index_path:
        logger.error("Failed to read index.json from %s", base_url)
        return None
    with open(index_path, encoding="utf-8") as fh:
        index_data = json.load(fh)

    # Determine owner/repo robustly
    task = get_taskname_from_index(index_data)
    if not task.owner or not task.repo:
        logger.error("Could not determine owner/repo from index.json or known mappings.")
        return None
    owner, repo_name = task.owner, task.repo

    # Dashboard descriptors
    all_keys = sorted(index_data["params"])
    benchmarks = list(index_data["benchmarks"])
    param_sets = index_data["graph_param_list"]

    # Sparse anchors from the dashboard
    revision_to_hash_sparse: dict[str, str] = index_data.get("revision_to_hash", {}) or {}
    revision_to_date_sparse: dict[str, int] = index_data.get("revision_to_date", {}) or {}

    # ---- First pass: collect every revision we will need ----
    graph_urls: list[str] = []
    for p in param_sets:
        for bench in benchmarks:
            bench_url_rel = asv.graph.Graph.get_file_path(params=p, benchmark_name=f"{bench}.json")  # pyright: ignore[reportAttributeAccessIssue]
            graph_urls.append(join_path(base_url, bench_url_rel))

    summary_urls = [join_path("graphs", "summary", f"{b}.json") for b in benchmarks]

    revisions_needed: set[int] = set()
    for url in tqdm(graph_urls, desc="scan graphs for revisions"):
        local = dl_and_open(url, html_dir, base=base_url, force=force)
        if local is None:
            continue
        try:
            with open(local, encoding="utf-8") as fh:
                data = json.load(fh)
        except json.JSONDecodeError:
            logger.exception("Failed to decode %s", local)
            continue
        for row in data:
            with contextlib.suppress(Exception):
                revisions_needed.add(int(row[0]))

    # Collect from summaries too
    for url in tqdm(summary_urls, desc="scan summaries for revisions"):
        local = dl_and_open(url, html_dir, base=base_url, force=force)
        if local is None:
            continue
        try:
            with open(local, encoding="utf-8") as fh:
                data = json.load(fh)
        except json.JSONDecodeError:
            logger.exception("Failed to decode %s", local)
            continue
        for row in data:
            with contextlib.suppress(Exception):
                revisions_needed.add(int(row[0]))

    # Ensure we also cover the sparse keys (anchors)
    revisions_needed.update(int(k) for k in (revision_to_hash_sparse.keys() if revision_to_hash_sparse else []))
    revisions_needed.update(int(k) for k in (revision_to_date_sparse.keys() if revision_to_date_sparse else []))

    rev2hash_dense, rev2date_dense = _compute_dense_mappings(
        owner=owner,
        repo_name=repo_name,
        revisions_needed=revisions_needed,
        revision_to_hash_sparse=revision_to_hash_sparse,
        revision_to_date_sparse=revision_to_date_sparse,
    )

    # ---- Second pass: load dataframes and map hash/date ----
    frames: list[pd.DataFrame] = []
    for p in tqdm(param_sets, desc="machines"):
        for bench in tqdm(benchmarks, desc="benchmarks", leave=False):
            bench_url_rel = asv.graph.Graph.get_file_path(params=p, benchmark_name=f"{bench}.json")  # pyright: ignore[reportAttributeAccessIssue]
            full_url = join_path(base_url, bench_url_rel)
            local = dl_and_open(full_url, html_dir, base=base_url, force=force)
            if local is None:
                continue
            try:
                with open(local, encoding="utf-8") as fh:
                    data = json.load(fh)
            except json.JSONDecodeError:
                logger.exception("Failed to decode %s", local)
                continue

            df = pd.DataFrame(data, columns=["revision", "time"])
            df["revision"] = df["revision"].astype(int)
            df["hash"] = df["revision"].map(rev2hash_dense)
            df["date"] = df["revision"].map(rev2date_dense)
            df["benchmark"] = bench
            df["machine"] = p["machine"]
            frames.append(df)

    all_benchmarks = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    logger.info("Collected %s rows from %s benchmark files.", f"{len(all_benchmarks):,}", f"{len(frames):,}")

    # Summaries
    all_summaries: list[pd.DataFrame] = []
    for summary_url in tqdm(summary_urls, desc="summaries"):
        summary_pth = dl_and_open(summary_url, html_dir, base=base_url, force=force)
        if summary_pth is None:
            continue
        try:
            with open(summary_pth, encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            logger.exception("Failed to decode %s", summary_pth)
            continue

        benchmark_name = os.path.basename(summary_pth).replace(".json", "")
        df = pd.DataFrame(data, columns=["revision", "time"])
        df["revision"] = df["revision"].astype(int)
        df["hash"] = df["revision"].map(rev2hash_dense)
        df["date"] = df["revision"].map(rev2date_dense)
        df["benchmark"] = benchmark_name
        all_summaries.append(df)

    all_summaries_df = pd.concat(all_summaries, ignore_index=True) if all_summaries else pd.DataFrame()

    collection = BenchmarkCollection(
        base_url=base_url,
        collected_at=datetime.datetime.now(timezone.utc),
        modified_at=datetime.datetime.now(timezone.utc),
        param_keys=all_keys,
        index_data=index_data,
        benchmarks=all_benchmarks,
        summaries=all_summaries_df,
        task=Task(owner=owner, repo=repo_name, sha=None),
    )
    return collection
