"""
This file implements many test cases pertaining to the use of the BenchmarkCollection class.

- `BenchmarkCollection.save` and `load` round-trip integrity.
- `save` enforces `.fc.pkl` suffix; updates `modified_at`; `load` returns correct type.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from datasmith.benchmark.collection import BenchmarkCollection


def _make_collection() -> BenchmarkCollection:
    now = datetime.now(timezone.utc)
    benchmarks = pd.DataFrame({
        "revision": [1],
        "time": [1.0],
        "hash": ["abc1234"],
        "benchmark": ["bench.fn"],
        "machine": ["docker"],
        "date": ["2024-01-01T00:00:00Z"],
    })
    summaries = pd.DataFrame({
        "revision": [1],
        "time": [1.0],
        "hash": ["abc1234"],
        "benchmark": ["bench.fn"],
        "date": ["2024-01-01T00:00:00Z"],
    })
    index_data = {
        "project": "demo",
        "project_url": "https://example.com",
        "show_commit_url": "https://github.com/org/repo/commit",
        "hash_length": 7,
        "revision_to_hash": {"1": "abc1234"},
        "revision_to_date": {"1": "2024-01-01T00:00:00Z"},
        "params": ["machine"],
        "graph_param_list": [{"machine": "docker"}],
        "benchmarks": ["bench.fn"],
        "machines": ["docker"],
        "tags": [],
        "pages": [],
    }
    return BenchmarkCollection(
        base_url="file:///tmp/html",
        collected_at=now,
        modified_at=now,
        param_keys=["machine"],
        index_data=index_data,
        benchmarks=benchmarks,
        summaries=summaries,
    )


def test_save_load_roundtrip(tmp_path: Path) -> None:
    c = _make_collection()
    out = c.save(tmp_path / "dashboard.fc.pkl")
    loaded = BenchmarkCollection.load(out)

    assert isinstance(loaded, BenchmarkCollection)
    assert loaded.base_url == c.base_url
    assert loaded.param_keys == c.param_keys
    assert loaded.index_data == c.index_data
    assert loaded.benchmarks.equals(c.benchmarks)
    assert loaded.summaries.equals(c.summaries)


def test_save_enforces_fc_pkl_suffix(tmp_path: Path) -> None:
    c = _make_collection()
    candidates = [
        tmp_path / "a",
        tmp_path / "a.pkl",
        tmp_path / "a.fc",
        tmp_path / "a.txt",
        tmp_path / "a.fc.pkl",
    ]
    for p in candidates:
        written = c.save(p)
        # Expect exact .fc.pkl suffix convention
        assert Path(written).suffixes[-2:] == [".fc", ".pkl"]
        assert Path(written).exists()


def test_modified_at_updates_on_save(tmp_path: Path) -> None:
    c = _make_collection()
    before = c.modified_at
    out = c.save(tmp_path / "x.fc.pkl")
    loaded = BenchmarkCollection.load(out)
    assert loaded.modified_at >= before
    assert loaded.modified_at != before


def test_load_returns_correct_type(tmp_path: Path) -> None:
    c = _make_collection()
    out = c.save(tmp_path / "y.fc.pkl")
    obj = BenchmarkCollection.load(out)
    assert isinstance(obj, BenchmarkCollection)
