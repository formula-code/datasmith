import re
from typing import Final

import pandas as pd

# Words/phrases that very strongly indicate a performance change.
_POSITIVE_PATTERNS: Final = [
    r"\bperf(?:ormance)?\b",
    r"\boptimi[sz](?:e|ed|es|ation|ations|ing)?\b",
    r"\bspeed(?:\s*up|ed|ing)?\b",
    r"\bfaster\b",
    r"\blatency\b",
    r"\bthroughput\b",
    r"\boverhead\b",
    r"\bmemory(?:\s*(?:usage|footprint|alloc(?:ation|ations)?|pressure))?\b",
    r"\breduc(?:e|ed|es|ing)\s+(?:allocations?|copies|overhead|latency|cpu|memory)\b",
    r"\bavoid(?:ed|s|ing)?\s+(?:allocations?|copies)\b",
    r"\bcache(?:d|ing)?\b",
    r"\bvectori[sz]e(?:d|s|)?\b",
    r"\bparallel(?:ize|ism|ized|)\b",
    r"\bconcurren(?:t|cy)\b",
    r"\bprofil(?:e|ing|er)\b",
    r"\bbenchmark(?:s|ing)?\b",
    r"\bJIT\b|\bnumba\b|\bcython\b|\bsimd\b|\bavx\b|\bsse\b|\bneon\b",
    r"\bgpu\b|\bcuda\b|\bopencl\b",
    r"\bzero-?copy\b",
    r"\bpreallocat(?:e|ion)\b",
    r"\bhot(?:\s*path)?\b",
    r"\bbottleneck\b",
]

# Things that are almost never “performance optimizations” on their own.
_NEGATIVE_PATTERNS: Final = [
    # Meta / maintenance
    # r"^\s*merge (?:pull request|branch)\b",
    r"^\s*revert\b",
    r"^\s*(?:release|prepare(?:d)? release)\b|\bchangelog\b|\btag(?:ging)?\b",
    r"\bbump(?:ing)?\b|\bversions?\b",  # now matches "version" & "versions"
    r"\bupdate\s+versions?\b",  # e.g. "Update versions for 12.0.1"
    r"\[(?:\s*)release(?:\s*)\]",  # e.g. "[Release] ..."
    r"^\s*(?:minor|major|patch)\s*:?\s*\[?release\]?",  # e.g. "MINOR: [Release] ..."
    # Docs / typing / formatting / CI
    r"\bdocs?(?:umentation)?\b|\breadme\b|\bdocstring\b",
    r"\btype\s+comments?\b|\btype\s+annotations?\b|\btyping\b|\bmypy\b|\bpyright\b|\bpytype\b",
    r"\btypo\b",
    r"\bformat(?:ting)?\b|\bfmt\b|\blint(?:s|ing)?\b|\bblack\b|\bisort\b|\bruff\b|\bflake8\b",
    r"\bci\b|\bgithub actions\b|\bworkflow\b|\bpre-commit\b|\bcibuildwheel\b|\btravis\b|\bcircleci\b",
    r"\btests?\b|\bcoverage\b|\bTST:\b",
    # Infra / deps / packaging
    r"\bdependabot\b|\bdeps?\b|\bdependenc(?:y|ies)\b|\bpin(?:ning)?\b|\bunpin\b|\brequirements?\b|\bpyproject\.toml\b",
    r"\bbuild\b|\bwheels?\b|\bpackag(?:e|ing)\b|\bdocker\b|\bk8s\b|\bkubernetes\b|\bhelm\b",
    # Conventional-commits buckets that are rarely perf on their own
    r"^\s*chore\b",  # e.g. "chore(PageHeader): delete title param"
]

# These are repos where majority of commits do not have performance depend
# commits we can measure due to missing dependencies (like CUDA).
_BAD_REPOS = {
    "activitysim",
    "aicsimageio",
    "asdf",
    "chempy",
    "calebbell",
    "dasdae",
    "datalad",
    "devito",
    "dottxt-ai",
    "freegs",
    "datashader",
    "loopy",
    "intelpython",
    "jdasoftwaregroup",
    "janim",
    "oggm",
    "innobi",
    "newton-physics",
    "modin-project",
    "makepath",
    "mars-project",
    "qcodes",
    "sourmash",
    "anndata",
    "contrib-metric-learn",
    "pynetdicom",
    "climpred",
    "nilearn",
    "kedro",
    "mujoco",
    "mongodb-labs",
    "mdanalysis",
    "pvlib",
    "psygnal",
    "nvidia-warp",
    "man-group-arcticdb",
    "pydata-bottleneck",
    "pybamm-team",
    "pydicom-pydicom",
    "pybop-team",
    "python-control",
    "hyper-h11",
    "pymc-devs",
    "pysal-momepy",
    "qiskit",
    "quantumlib-cirq",
    "betterproto",
    "components",
    "django-components",
    "apache-arrow",
    "bloomberg-memray",
    "deepchecks-deepchecks",
    "ipython-ipyparallel",
    "lmfit-lmfit",
    "man-group-arctic",
    "neurostuff",
    "scverse-spatialdata",
    "tensorwerk-hangar",
    "dask",
    "unidata-metpy",
    "scitools-iris",
    "posthog-posthog",
    "scverse-scanpy",
    "stac-utils-pystac",
    "royerlab-ultrack",
}


_POSITIVE_RE = re.compile("|".join(_POSITIVE_PATTERNS), re.I)
_NEGATIVE_RE = re.compile("|".join(_NEGATIVE_PATTERNS), re.I)


def basic_message_filter(msg: str) -> bool:
    """
    Returns True if the commit message looks performance-related (KEEP).
    Returns False if it's safe to filter out as not performance-related.

    Strategy:
      - If it hits any positive/perf signals -> keep.
      - Else if it hits any strong non-perf buckets -> filter out.
      - Else (ambiguous) -> keep (to avoid missing perf work).
    """
    if not msg:
        return False  # nothing useful -> filter

    if _POSITIVE_RE.search(msg):
        return True
    return not _NEGATIVE_RE.search(msg)


def is_delinquient_repo(repo_name: str) -> bool:
    normalized = repo_name.lower().replace("/", "-")
    return any(bad in normalized for bad in _BAD_REPOS)


def crude_perf_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Filter commits DataFrame to likely performance-related ones."""
    filtered_df = df.copy(deep=True)

    filtered_df["total_changes"] = filtered_df["total_additions"] + filtered_df["total_deletions"]
    filtered_df["n_files_changed"] = filtered_df["files_changed"].str.split("\n").apply(len)
    filtered_df["is_perf"] = filtered_df["message"].apply(basic_message_filter)

    mask = (
        filtered_df["is_perf"]
        & (filtered_df["total_changes"] < 4000)
        & (filtered_df["n_files_changed"] < 500)
        & (filtered_df["patch"].str.len() < 20000)
        & (~filtered_df["repo_name"].apply(is_delinquient_repo))
    )

    return filtered_df.loc[mask].copy(deep=True)
