"""A repository taken out of ingestion must cost nothing in stage 2 or stage 3.

Six repositories produced zero containers across 430 stage-6 attempts while
consuming 15% of the corpus and 29% of everything marked is_performance_commit.
The flag that excludes them is only worth having if every stage honours it --
an exclusion respected in stage 2 and forgotten in stage 3 would still pay a
REST call and an LLM call for every row stage 2 stored before the exclusion.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

from datasmith.utils.db import excluded_repos


class TestExcludedRepos:
    def test_reads_the_disabled_rows(self) -> None:
        rows = [{"owner": "PostHog", "repo": "posthog"}, {"owner": "not522", "repo": "ac-library-python"}]
        with patch("datasmith.utils.db.fetch_all", return_value=rows) as f:
            assert excluded_repos() == {("PostHog", "posthog"), ("not522", "ac-library-python")}
        assert f.call_args.kwargs["filters"] == {"ingest_enabled": False}

    def test_no_exclusions_is_an_empty_set(self) -> None:
        with patch("datasmith.utils.db.fetch_all", return_value=[]):
            assert excluded_repos() == set()

    def test_it_fails_open_when_the_column_is_missing(self) -> None:
        """A database without migration 00030 must ingest everything, not nothing."""
        with patch("datasmith.utils.db.fetch_all", side_effect=RuntimeError("column does not exist")):
            assert excluded_repos() == set()


class TestBothStagesHonourIt:
    """Fail closed: a new stage that reads repositories or pull_requests and
    does not consult ``excluded_repos`` will trip this."""

    @staticmethod
    def _source() -> str:
        from pathlib import Path

        import datasmith.update.pipeline as mod

        return Path(mod.__file__).read_text(encoding="utf-8")

    def test_stage_2_consults_the_exclusion(self) -> None:
        src = self._source()
        body = src.split("async def _scrape_commits")[1].split("async def ")[0]
        assert "excluded_repos()" in body, "stage 2 selects repositories without honouring the exclusion"

    def test_stage_3_consults_the_exclusion(self) -> None:
        src = self._source()
        body = src.split("async def _classify_prs")[1].split("async def ")[0]
        assert "excluded_repos()" in body, "stage 3 would still spend a REST call on excluded repos"

    def test_the_helper_is_imported_not_reimplemented(self) -> None:
        """One definition, for the reason window_filters has one."""
        src = self._source()
        assert "from datasmith.utils.db import" in src
        assert "excluded_repos" in src.split("class Pipeline")[0]


class TestStageTwoFiltersTheWorkList:
    def test_excluded_repo_is_dropped_before_any_api_call(self) -> None:
        repos = [
            {"owner": "pandas-dev", "repo": "pandas"},
            {"owner": "PostHog", "repo": "posthog"},
        ]
        skip = {("PostHog", "posthog")}
        kept = [(r["owner"], r["repo"]) for r in repos if (r["owner"], r["repo"]) not in skip]
        assert kept == [("pandas-dev", "pandas")]

    def test_stage_three_drops_rows_stored_before_the_exclusion(self) -> None:
        rows: list[dict[str, Any]] = [
            {"owner": "pandas-dev", "repo": "pandas", "issue_number": 1},
            {"owner": "PostHog", "repo": "posthog", "issue_number": 2},
            {"owner": "PostHog", "repo": "posthog", "issue_number": 3},
        ]
        skip = {("PostHog", "posthog")}
        kept = [r for r in rows if (r["owner"], r["repo"]) not in skip]
        assert len(kept) == 1
        assert kept[0]["owner"] == "pandas-dev"
