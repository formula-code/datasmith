from __future__ import annotations

import numpy as np
import pandas as pd

from datasmith.update.offline import (
    _dict_sha,
    _extract_labels,
    _row_to_record,
    _sanitize_text,
    load_offline_pull_requests,
    load_offline_repo_names,
    parse_file_change_summary,
)


class TestSanitizeText:
    def test_normal_string(self):
        assert _sanitize_text("hello") == "hello"

    def test_none(self):
        assert _sanitize_text(None) == ""

    def test_nan(self):
        assert _sanitize_text(float("nan")) == ""

    def test_null_bytes(self):
        assert _sanitize_text("a\u0000b") == "ab"


class TestParseFileChangeSummary:
    def test_valid_table(self):
        summary = (
            "| File | Lines Added | Lines Removed | Total Changes |\n"
            "|------|-------------|----------------|----------------|\n"
            "| foo.py | 10 | 3 | 13 |\n"
            "| bar.py | 0 | 5 | 5 |"
        )
        result = parse_file_change_summary(summary)
        assert result == [
            {"filename": "foo.py", "additions": 10, "deletions": 3},
            {"filename": "bar.py", "additions": 0, "deletions": 5},
        ]

    def test_empty_string(self):
        assert parse_file_change_summary("") is None

    def test_none(self):
        assert parse_file_change_summary(None) is None

    def test_nan(self):
        assert parse_file_change_summary(float("nan")) is None

    def test_no_data_rows(self):
        summary = "| File | Lines Added | Lines Removed |\n|------|-------------|----------------|"
        assert parse_file_change_summary(summary) is None


class TestExtractLabels:
    def test_empty_array(self):
        assert _extract_labels(np.array([], dtype=object)) == []

    def test_label_dicts(self):
        labels = np.array([
            {"name": "bug", "color": "red"},
            {"name": "enhancement", "color": "blue"},
        ])
        assert _extract_labels(labels) == ["bug", "enhancement"]

    def test_none(self):
        assert _extract_labels(None) == []


class TestDictSha:
    def test_dict_with_sha(self):
        assert _dict_sha({"sha": "abc123", "ref": "main"}) == "abc123"

    def test_dict_without_sha(self):
        assert _dict_sha({"ref": "main"}) == ""

    def test_none(self):
        assert _dict_sha(None) == ""


class TestRowToRecord:
    def _make_row(self, **overrides):
        data = {
            "repo_name": "owner/repo",
            "pr_number": 42,
            "pr_title": "Speed up parsing",
            "pr_body": "This PR improves parsing speed.",
            "pr_state": "closed",
            "pr_created_at": "2024-01-01T00:00:00Z",
            "pr_merged_at": "2024-01-02T00:00:00Z",
            "pr_closed_at": "2024-01-02T00:00:00Z",
            "pr_merge_commit_sha": "abc123",
            "pr_base": {"sha": "base000"},
            "pr_head": {"sha": "head111"},
            "pr_labels": np.array([{"name": "performance"}]),
            "original_patch": "diff --git a/foo.py b/foo.py\n+fast code",
            "file_change_summary": (
                "| File | Lines Added | Lines Removed | Total Changes |\n"
                "|------|-------------|----------------|----------------|\n"
                "| foo.py | 5 | 2 | 7 |"
            ),
        }
        data.update(overrides)
        return pd.Series(data)

    def test_basic_conversion(self):
        rec = _row_to_record(self._make_row())
        assert rec["owner"] == "owner"
        assert rec["repo"] == "repo"
        assert rec["issue_number"] == 42
        assert rec["title"] == "Speed up parsing"
        assert rec["merge_commit_sha"] == "abc123"
        assert rec["base_sha"] == "base000"
        assert rec["head_sha"] == "head111"
        assert rec["labels"] == ["performance"]
        assert "patch" in rec
        assert rec["file_changes"] == [{"filename": "foo.py", "additions": 5, "deletions": 2}]
        assert isinstance(rec["is_performance_commit_symbolic"], bool)

    def test_missing_patch(self):
        rec = _row_to_record(self._make_row(original_patch=None))
        assert "patch" not in rec

    def test_nan_body(self):
        rec = _row_to_record(self._make_row(pr_body=float("nan")))
        assert rec["body"] == ""


class TestLoadOfflineRepoNames:
    def test_extracts_unique_pairs(self, tmp_path):
        df = pd.DataFrame({"repo_name": ["a/b", "a/b", "c/d"]})
        path = str(tmp_path / "test.parquet")
        df.to_parquet(path)

        pairs = load_offline_repo_names(path)
        assert set(pairs) == {("a", "b"), ("c", "d")}


class TestLoadOfflinePullRequests:
    def _make_df(self):
        return pd.DataFrame([
            {
                "repo_name": "owner/repo",
                "pr_number": 1,
                "pr_title": "Optimize query",
                "pr_body": "Faster queries.",
                "pr_state": "closed",
                "pr_created_at": "2024-06-01T00:00:00Z",
                "pr_merged_at": "2024-06-15T00:00:00Z",
                "pr_closed_at": "2024-06-15T00:00:00Z",
                "pr_merge_commit_sha": "sha1",
                "pr_base": {"sha": "base1"},
                "pr_head": {"sha": "head1"},
                "pr_labels": np.array([]),
                "original_patch": "diff",
                "file_change_summary": "",
            },
            {
                "repo_name": "owner/repo",
                "pr_number": 2,
                "pr_title": "Fix typo",
                "pr_body": "",
                "pr_state": "closed",
                "pr_created_at": "2024-07-01T00:00:00Z",
                "pr_merged_at": "2024-07-10T00:00:00Z",
                "pr_closed_at": "2024-07-10T00:00:00Z",
                "pr_merge_commit_sha": "sha2",
                "pr_base": {"sha": "base2"},
                "pr_head": {"sha": "head2"},
                "pr_labels": np.array([]),
                "original_patch": "diff2",
                "file_change_summary": "",
            },
        ])

    def test_date_filtering(self, tmp_path):
        path = str(tmp_path / "test.parquet")
        self._make_df().to_parquet(path)

        # Only the June record should match
        records = load_offline_pull_requests(path, since="2024-06-01", until="2024-07-01")
        assert len(records) == 1
        assert records[0]["issue_number"] == 1

    def test_no_date_filter(self, tmp_path):
        path = str(tmp_path / "test.parquet")
        self._make_df().to_parquet(path)

        records = load_offline_pull_requests(path)
        assert len(records) == 2
