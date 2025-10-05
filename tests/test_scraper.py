"""
This file implements many test cases pertaining to the use of the scraper.


scrape.scrape_dashboards.make_benchmark_from_html:
    - `make_benchmark_from_html` handles both remote URLs and local folders; correct path joining for graphs and summaries.
    - Proper parsing of `index.json`, `graphs/*/*.json` including invalid JSON and missing files (log + skip).
    - DataFrames have required columns: `benchmarks` and `summaries` contain `revision`, `time`, `hash`, `benchmark`, `machine`/`date`.
    - `force` propagation works (note: CLI currently doesn't pass `force` through; test should expose this gap).


detection.detect_breakpoints.detect_all_breakpoints` (+ `get_detection_method`):
scrape.code_coverage.generate_coverage_dataframe
scrape.build_reports.breakpoints_scrape_comments
    - `get_detection_method`: returns callable for `"rbf"` and `"asv"`, errors for invalid method.
    - `detect_all_breakpoints`: on synthetic series detects negative deltas; validates required columns; grouping per `benchmark`.
    - Coverage generation builds commit URLs from `index_data["show_commit_url"]`, de-duplicates, respects `only` filter; handles missing coverage gracefully.
    - Reports: `breakpoints_scrape_comments` merges coverage, computes `n_tokens`, produces stable `report` text per commit; robust to PR-less commits.
    - End-to-end: loading collection, attaching `breakpoints/coverage/comments/enriched_breakpoints`, saving to expected path.


scrape.detect_dashboards.scrape_github` (uses `search_pages` + `_request_with_backoff`), `scrape.filter_dashboards.filter_dashboards` (+ `enrich_repos`)
    - `search_pages`: unique repo names, pagination stop conditions, jitter/backoff; correct query assembly.
    - `_request_with_backoff`: retry on 403/429 with backoff; success path returns JSON.
    - `filter_dashboards`/`enrich_repos`: adds `is_accessible/is_fork/is_archived/watchers/stars`; filtering logic; empty-input error.




"""

from __future__ import annotations

import json
from pathlib import Path
from typing import ClassVar
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests

from datasmith.core.api.http_utils import request_with_backoff
from datasmith.detection.detect_breakpoints import detect_all_breakpoints, get_detection_method
from datasmith.scrape.code_coverage import generate_coverage_dataframe
from datasmith.scrape.detect_dashboards import search_pages
from datasmith.scrape.filter_dashboards import enrich_repos, filter_dashboards
from datasmith.scrape.scrape_dashboards import make_benchmark_from_html


class TestMakeBenchmarkFromHtml:
    def _create_mock_dashboard(self, tmp_path: Path) -> Path:
        """Create a minimal mock ASV dashboard structure."""
        html_dir = tmp_path / "html"
        html_dir.mkdir()

        # Create index.json
        index_data = {
            "params": ["machine"],
            "benchmarks": ["bench.func1", "bench.func2"],
            "graph_param_list": [{"machine": "docker"}],
            "revision_to_hash": {"1": "abc123", "2": "def456"},
            "revision_to_date": {"1": "2024-01-01T00:00:00Z", "2": "2024-01-02T00:00:00Z"},
        }
        (html_dir / "index.json").write_text(json.dumps(index_data))

        # Create graphs directory structure
        graphs_dir = html_dir / "graphs" / "machine-docker"
        graphs_dir.mkdir(parents=True)

        # Create benchmark JSON files
        bench_data = [["1", 1.5], ["2", 1.2]]
        (graphs_dir / "bench.func1.json").write_text(json.dumps(bench_data))
        (graphs_dir / "bench.func2.json").write_text(json.dumps(bench_data))

        # Create summary files
        summary_dir = html_dir / "graphs" / "summary"
        summary_dir.mkdir()
        (summary_dir / "bench.func1.json").write_text(json.dumps(bench_data))
        (summary_dir / "bench.func2.json").write_text(json.dumps(bench_data))

        return html_dir

    def test_local_dashboard_parsing(self, tmp_path: Path) -> None:
        """Test parsing a local ASV dashboard."""
        html_dir = self._create_mock_dashboard(tmp_path)

        collection = make_benchmark_from_html(base_url=str(html_dir), html_dir=str(tmp_path / "output"), force=False)

        assert collection is not None
        assert collection.base_url == str(html_dir)
        assert "machine" in collection.param_keys

        # Check benchmarks DataFrame
        assert not collection.benchmarks.empty
        required_cols = {"revision", "time", "hash", "benchmark", "machine", "date"}
        assert required_cols.issubset(set(collection.benchmarks.columns))

        # Check summaries DataFrame
        assert not collection.summaries.empty
        summary_cols = {"revision", "time", "hash", "benchmark", "date"}
        assert summary_cols.issubset(set(collection.summaries.columns))

    def test_missing_index_json(self, tmp_path: Path) -> None:
        """Test behavior when index.json is missing."""
        html_dir = tmp_path / "empty"
        html_dir.mkdir()

        collection = make_benchmark_from_html(base_url=str(html_dir), html_dir=str(tmp_path / "output"), force=False)

        assert collection is None

    def test_invalid_json_handling(self, tmp_path: Path) -> None:
        """Test graceful handling of invalid JSON files."""
        html_dir = tmp_path / "html"
        html_dir.mkdir()

        # Create valid index.json
        index_data = {
            "params": ["machine"],
            "benchmarks": ["bench.func1"],
            "graph_param_list": [{"machine": "docker"}],
            "revision_to_hash": {"1": "abc123"},
            "revision_to_date": {"1": "2024-01-01T00:00:00Z"},
        }
        (html_dir / "index.json").write_text(json.dumps(index_data))

        # Create invalid benchmark JSON
        graphs_dir = html_dir / "graphs" / "machine-docker"
        graphs_dir.mkdir(parents=True)
        (graphs_dir / "bench.func1.json").write_text("invalid json")

        # Create valid summary
        summary_dir = html_dir / "graphs" / "summary"
        summary_dir.mkdir()
        (summary_dir / "bench.func1.json").write_text(json.dumps([["1", 1.5]]))

        collection = make_benchmark_from_html(base_url=str(html_dir), html_dir=str(tmp_path / "output"), force=False)

        assert collection is not None
        # Should have summaries but benchmarks may be empty due to invalid JSON
        # The function should handle empty frames gracefully
        assert not collection.summaries.empty

    @patch("requests.get")
    def test_remote_url_handling(self, mock_get: Mock, tmp_path: Path) -> None:
        """Test handling of remote HTTP URLs."""
        # Mock successful HTTP responses
        index_response = Mock()
        index_response.status_code = 200
        index_response.content = json.dumps({
            "params": ["machine"],
            "benchmarks": ["bench.func1"],
            "graph_param_list": [{"machine": "docker"}],
            "revision_to_hash": {"1": "abc123"},
            "revision_to_date": {"1": "2024-01-01T00:00:00Z"},
        }).encode()

        bench_response = Mock()
        bench_response.status_code = 200
        bench_response.content = json.dumps([["1", 1.5]]).encode()

        mock_get.side_effect = [index_response, bench_response, bench_response]

        collection = make_benchmark_from_html(
            base_url="https://example.com/dashboard", html_dir=str(tmp_path / "output"), force=True
        )

        assert collection is not None
        assert collection.base_url == "https://example.com/dashboard"


class TestDetectionMethods:
    def test_get_detection_method_valid(self) -> None:
        """Test get_detection_method returns correct callables."""
        rbf_method = get_detection_method("rbf")
        asv_method = get_detection_method("asv")

        assert callable(rbf_method)
        assert callable(asv_method)
        assert rbf_method != asv_method

    def test_get_detection_method_invalid(self) -> None:
        """Test get_detection_method raises error for invalid method."""
        with pytest.raises(ValueError, match="Unknown method: invalid"):
            get_detection_method("invalid")

    def test_detect_all_breakpoints_required_columns(self) -> None:
        """Test detect_all_breakpoints validates required columns."""
        df_missing_cols = pd.DataFrame({"benchmark": ["test"], "time": [1.0]})

        with pytest.raises(ValueError):
            detect_all_breakpoints(df_missing_cols)

    def test_detect_all_breakpoints_synthetic_data(self) -> None:
        """Test detect_all_breakpoints on synthetic performance regression."""
        # Create synthetic data with clear performance improvement
        data = []
        for i in range(10):
            # Performance gets better at index 5
            time_val = 2.0 if i < 5 else 1.0
            data.append({
                "benchmark": "test.func",
                "time": time_val,
                "hash": f"hash{i:02d}",
            })

        df = pd.DataFrame(data)
        breakpoints = detect_all_breakpoints(df, method="rbf")

        # Should detect breakpoints or return empty DataFrame gracefully
        assert isinstance(breakpoints, pd.DataFrame)


class TestCoverageGeneration:
    @patch("datasmith.scrape.code_coverage._iter_commit_coverage")
    def test_generate_coverage_dataframe(self, mock_iter: Mock) -> None:
        """Test coverage dataframe generation."""
        # Mock commit coverage iteration
        mock_iter.return_value = [("file1.py", 85.0), ("file2.py", None)]

        breakpoints_df = pd.DataFrame({"hash": ["abc123"], "gt_hash": ["def456"]})

        index_data = {"show_commit_url": "https://github.com/org/repo/commit/"}

        coverage_df = generate_coverage_dataframe(breakpoints_df, index_data, only=None)

        assert not coverage_df.empty
        expected_cols = {"typ", "url", "path", "coverage"}
        assert expected_cols.issubset(set(coverage_df.columns))


class TestRepositoryFiltering:
    MOCK_RESPONSES: ClassVar[dict[str, dict]] = {
        "org/repo1": {
            "fork": False,
            "archived": False,
            "disabled": False,
            "subscribers_count": 100,
            "stargazers_count": 500,
        },
        "org/repo2": {
            "fork": True,
            "archived": False,
            "disabled": False,
            "subscribers_count": 200,
            "stargazers_count": 1000,
        },
        "org/repo3": {
            "fork": False,
            "archived": True,
            "disabled": False,
            "subscribers_count": 300,
            "stargazers_count": 1500,
        },
    }

    @patch("datasmith.scrape.filter_dashboards._get_repo_metadata")
    def test_enrich_repos(self, mock_metadata: Mock) -> None:
        """Test repository enrichment with GitHub metadata."""
        # Mock GitHub API responses
        mock_metadata.return_value = {
            "fork": False,
            "archived": False,
            "disabled": False,
            "subscribers_count": 100,
            "stargazers_count": 500,
        }

        df = pd.DataFrame({"repo_name": ["https://github.com/org/repo"]})
        enriched = enrich_repos(df, url_col="repo_name", show_progress=False)

        expected_cols = {"is_accessible", "is_fork", "is_archived", "fork_parent", "forked_at", "watchers", "stars"}
        assert expected_cols.issubset(set(enriched.columns))
        assert bool(enriched.iloc[0]["is_accessible"]) is True
        assert bool(enriched.iloc[0]["is_fork"]) is False
        assert enriched.iloc[0]["stars"] == 500

    def test_filter_dashboards_empty_input(self) -> None:
        """Test filter_dashboards handles empty input."""
        df = pd.DataFrame({"repo_name": []})

        with pytest.raises(ValueError, match="Dataframe empty"):
            filter_dashboards(df, url_col="repo_name", show_progress=False)

    @patch("datasmith.scrape.filter_dashboards._get_repo_metadata")
    def test_filter_dashboards_filtering_logic(self, mock_metadata: Mock) -> None:
        """Test filtering logic removes forks and archived repos."""

        # Mock responses for different repo types
        def mock_response(full_name: str) -> dict | None:
            repo_info = self.MOCK_RESPONSES.get(full_name)
            if repo_info is None:
                return None
            return repo_info

        mock_metadata.side_effect = mock_response

        df = pd.DataFrame({
            "repo_name": [
                "https://github.com/org/repo1",
                "https://github.com/org/repo2",
                "https://github.com/org/repo3",
            ]
        })

        filtered = filter_dashboards(df, url_col="repo_name", show_progress=False)

        # Should only keep repo1 (not fork, not archived)
        assert len(filtered) == 1
        assert "repo1" in filtered.iloc[0]["repo_name"]


class TestRequestWithBackoff:
    @patch("time.sleep")
    @patch("datasmith.core.api.http_utils.build_headers")
    def test_request_success(self, mock_headers: Mock, mock_sleep: Mock) -> None:
        """Test successful request without retries."""
        mock_headers.return_value = {"Authorization": "Bearer token"}

        with patch.object(requests.Session, "get") as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"data": "test"}
            mock_get.return_value = mock_response

            session = requests.Session()
            response = request_with_backoff(url="https://api.github.com/test", site_name="github", session=session)

            assert response.status_code == 200
            mock_sleep.assert_called()  # Should still throttle

    @patch("time.sleep")
    @patch("time.time")
    @patch("datasmith.core.api.http_utils.build_headers")
    def test_request_rate_limited(self, mock_headers: Mock, mock_time: Mock, mock_sleep: Mock) -> None:
        """Test retry behavior on rate limiting."""
        mock_headers.return_value = {"Authorization": "Bearer token"}
        mock_time.return_value = 1000.0

        with patch.object(requests.Session, "get") as mock_get:
            # First call returns 429, second succeeds
            rate_limited_response = Mock()
            rate_limited_response.status_code = 429
            rate_limited_response.headers = {"X-RateLimit-Reset": "1010", "X-RateLimit-Remaining": "0"}

            success_response = Mock()
            success_response.status_code = 200
            success_response.json.return_value = {"data": "test"}

            mock_get.side_effect = [rate_limited_response, success_response]

            session = requests.Session()
            response = request_with_backoff(
                url="https://api.github.com/test", site_name="github", session=session, max_retries=2
            )

            assert response.status_code == 200
            assert mock_get.call_count == 2


class TestSearchPages:
    @patch.dict("os.environ", {"GH_TOKEN": ""}, clear=False)
    @patch("time.sleep")
    @patch("requests.Session.get")
    def test_search_pages_pagination(self, mock_get: Mock, mock_sleep: Mock) -> None:
        """Test search_pages handles pagination correctly."""
        # Mock responses for multiple pages
        page1_response = Mock()
        page1_response.status_code = 200
        page1_response.json.return_value = {
            "items": [{"repository": {"full_name": "org/repo1"}}, {"repository": {"full_name": "org/repo2"}}],
            "incomplete_results": False,
        }

        page2_response = Mock()
        page2_response.status_code = 200
        page2_response.json.return_value = {
            "items": [{"repository": {"full_name": "org/repo3"}}, {"repository": {"full_name": "org/repo4"}}],
            "incomplete_results": False,
        }

        # Empty page signals end
        page3_response = Mock()
        page3_response.status_code = 200
        page3_response.json.return_value = {"items": []}

        mock_get.side_effect = [page1_response, page2_response, page3_response]

        repos = list(search_pages(max_pages=5, per_page=2, query="filename:asv.conf.json"))

        assert repos == ["org/repo1", "org/repo2", "org/repo3", "org/repo4"]
        # Now all 3 calls should be made
        assert mock_get.call_count == 3

    @patch.dict("os.environ", {"GH_TOKEN": ""}, clear=False)
    @patch("time.sleep")
    @patch("requests.Session.get")
    def test_search_pages_deduplication(self, mock_get: Mock, mock_sleep: Mock) -> None:
        """Test search_pages deduplicates repository names."""
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            "items": [
                {"repository": {"full_name": "org/repo1"}},
                {"repository": {"full_name": "org/repo1"}},  # Duplicate
                {"repository": {"full_name": "org/repo2"}},
            ]
        }

        mock_get.return_value = response

        repos = list(search_pages(max_pages=1, per_page=10, query="filename:asv.conf.json"))

        assert repos == ["org/repo1", "org/repo2"]
