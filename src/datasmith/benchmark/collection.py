from __future__ import annotations

import pickle
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import pandas as pd


@dataclass(slots=True)
class BenchmarkCollection:
    """
    Container for benchmark data and metadata.

    Attributes
    ----------
    base_url:
        Root ASV dashboard URL the data were pulled from.
    collected_at:
        UTC timestamp when the collection was produced.
    modified_at:
        UTC timestamp when the collection was last modified (e.g. after adding breakpoints).
    param_keys:
        Ordered list of *asv* parameter names (e.g. ['arch', 'machine']).
    index_data:
        Dictionary with metadata about the collection, including:
        - 'project': project name
        - 'project_url': URL to the project homepage
        - 'show_commit_url': URL template for showing commits
        - 'hash_length': length of commit hashes
        - 'revision_to_hash': mapping from ASV revision numbers to commit hashes
        - 'revision_to_date': mapping from ASV revision numbers to commit dates
        - 'params': dictionary of parameter names and their values
        - 'graph_param_list': list of parameter sets used in the benchmarks
        - 'benchmarks': list of benchmark names
        - 'machines': list of machine names used in the benchmarks
        - 'tags': tags associated with the benchmarks
        - 'pages': list of pages in the dashboard (if applicable)
    benchmarks:
        DataFrame with raw per-run timing (≈ all_benchmarks.csv).
    summaries:
        DataFrame with aggregate timings (≈ all_summaries.csv).

    breakpoints:
        DataFrame with detected breakpoints (if computed).
    coverage:
        DataFrame with coverage data for each breakpoint (if computed).
    comments:
        DataFrame with comments associated with each breakpoint (if computed).
    enriched_breakpoints:
        DataFrame with detected breakpoints, including metadata from GitHub/Codecov (if computed).
    """

    base_url: str
    collected_at: datetime
    modified_at: datetime
    param_keys: list[str]
    index_data: dict[str, str] = field(default_factory=dict, repr=False)
    benchmarks: pd.DataFrame = field(default_factory=pd.DataFrame, repr=False)
    summaries: pd.DataFrame = field(default_factory=pd.DataFrame, repr=False)

    breakpoints: pd.DataFrame = field(default_factory=pd.DataFrame, repr=False)
    coverage: pd.DataFrame = field(default_factory=pd.DataFrame, repr=False)
    comments: pd.DataFrame = field(default_factory=pd.DataFrame, repr=False)
    enriched_breakpoints: pd.DataFrame = field(default_factory=pd.DataFrame, repr=False)

    def save(self, path: str | Path) -> Path:
        """
        Pickle the collection to *path* (extension forcibly set to .fc.pkl).

        Returns
        -------
        Path to the file actually written.
        """
        self.modified_at = datetime.now(timezone.utc)
        path = Path(path)
        if any(suffix not in [".fc", ".pkl"] for suffix in path.suffixes):
            path = path.with_suffix(".fc.pkl")
        with open(path, "wb") as fh:
            pickle.dump(self, fh, protocol=pickle.HIGHEST_PROTOCOL)
        return path

    @classmethod
    def load(cls, path: str | Path) -> BenchmarkCollection:
        """
        Load a previously-saved BenchmarkCollection.
        """
        with open(path, "rb") as fh:
            return cast(BenchmarkCollection, pickle.load(fh))  # noqa: S301
