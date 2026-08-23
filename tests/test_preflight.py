"""Tests for datasmith.preflight — the reports, not the network checks.

``run_preflight`` talks to a model server, Docker, Supabase and GitHub, so it
is not testable offline.  The part spec section 6.4 added is: an operator on a
new machine must see their real limits.  That part is pure, and it is what is
tested here.
"""

from __future__ import annotations

import pytest

from datasmith.github import client as gh_client
from datasmith.preflight import _report, resolved_concurrency_caps
from datasmith.runners import classify_prs, render_problems, resolve_packages, scrape_commits


class TestResolvedConcurrencyCaps:
    def test_every_stage_dial_is_reported(self) -> None:
        labels = [label for label, _ in resolved_concurrency_caps()]
        for stage in ("stage 2", "stage 3", "stage 4", "stage 5"):
            assert any(stage in label for label in labels), f"{stage} has no reported dial: {labels}"

    def test_the_reported_values_are_the_ones_the_run_will_use(self) -> None:
        """Read from the modules, so a default an operator never set still shows."""
        caps = dict(resolved_concurrency_caps())
        assert caps["stage 2 repositories in flight"] == scrape_commits.DATASMITH_SCRAPE_COMMITS_CONCURRENCY
        assert caps["stage 2 search POSTs (bisection fan-out)"] == gh_client.DATASMITH_GH_SEARCH_CONCURRENCY
        assert caps["stage 3 diff fetches"] == classify_prs.DATASMITH_CLASSIFY_DIFF_CONCURRENCY
        assert caps["stage 3 LLM worker threads"] == classify_prs.DATASMITH_CLASSIFY_LLM_WORKERS
        assert caps["stage 4 resolver worker threads"] == resolve_packages.DATASMITH_RESOLVE_PACKAGES_WORKERS
        assert caps["stage 5 render worker threads"] == render_problems.DATASMITH_RENDER_PROBLEMS_WORKERS

    def test_an_override_is_reflected(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An operator who raised a dial in tokens.env sees the raised number."""
        monkeypatch.setattr(resolve_packages, "DATASMITH_RESOLVE_PACKAGES_WORKERS", 64)
        assert dict(resolved_concurrency_caps())["stage 4 resolver worker threads"] == 64

    def test_a_report_never_gates_the_run(self, capsys: pytest.CaptureFixture[str]) -> None:
        """Reports return nothing to accumulate into ``all_ok``.

        A measured 32.7 ms round trip to db.formulacode.org is slower than
        5.1 ms to local Postgres and is still a legitimate way to run the
        pipeline, so measuring it must not be able to refuse the run.
        """
        assert _report("Round-trip latency", "median 32.7 ms") is None
        assert "[INFO] Round-trip latency" in capsys.readouterr().out
