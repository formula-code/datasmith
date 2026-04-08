"""Tests for datasmith.agents.synthesizer."""

from __future__ import annotations

import datetime
from unittest.mock import MagicMock, patch

from datasmith.agents.sandbox import SandboxResult
from datasmith.agents.synthesizer import SynthesisState, Synthesizer, _format_prior_attempts
from datasmith.docker.context import DockerContext


class TestSynthesizerCacheHit:
    @patch("datasmith.agents.synthesizer.get_client")
    def test_cache_hit_returns_immediately(self, mock_get_client: MagicMock) -> None:
        """Mock cache hit returns DockerContext immediately."""
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.data = [{"build_pkg_sh": "#!/bin/bash\necho cached"}]
        mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.eq.return_value.execute.return_value = mock_resp
        mock_get_client.return_value = mock_client

        synth = Synthesizer()
        result = synth.run("owner", "repo", 42, "pr context", sha="abc123")

        assert result is not None
        assert result.build_pkg_sh == "#!/bin/bash\necho cached"
        assert SynthesisState.CHECK_CACHE in synth.trace
        assert SynthesisState.FIND_SIMILAR not in synth.trace
        assert SynthesisState.LLM_GENERATE not in synth.trace


class TestSynthesizerSimilarContext:
    @patch("datasmith.agents.synthesizer.verify_context")
    def test_similar_context_passes(self, mock_verify: MagicMock) -> None:
        """Cache miss, similar context passes verification."""
        similar_ctx = DockerContext(
            build_pkg_sh="#!/bin/bash\necho similar-pkg",
            build_run_sh="#!/bin/bash\necho similar-run",
        )

        mock_verify.return_value = SandboxResult(
            success=True,
            docker_context=similar_ctx,
        )

        synth = Synthesizer()
        with (
            patch.object(synth, "_check_cache", return_value=None),
            patch.object(synth, "_find_similar", return_value=[similar_ctx]),
            patch.object(synth, "_save_context"),
        ):
            result = synth.run("owner", "repo", 42, "pr context")

        assert result is not None
        assert result.build_pkg_sh == "#!/bin/bash\necho similar-pkg"
        assert result.build_run_sh == "#!/bin/bash\necho similar-run"
        assert SynthesisState.CHECK_CACHE in synth.trace
        assert SynthesisState.FIND_SIMILAR in synth.trace
        assert SynthesisState.TRY_SIMILAR in synth.trace
        # Should not proceed to LLM
        assert SynthesisState.LLM_GENERATE not in synth.trace
        mock_verify.assert_called_once()

    @patch("datasmith.agents.synthesizer.verify_context")
    def test_similar_context_fails_falls_through(self, mock_verify: MagicMock) -> None:
        """Similar context fails verification, falls through to LLM generation."""
        similar_ctx = DockerContext(build_pkg_sh="#!/bin/bash\necho fails")

        mock_verify.return_value = SandboxResult(
            success=False,
            failure_json={"stage": "build", "return_code": 1, "error_message": "missing dep"},
        )

        synth = Synthesizer()
        with (
            patch.object(synth, "_check_cache", return_value=None),
            patch.object(synth, "_find_similar", return_value=[similar_ctx]),
            patch.object(synth, "_sandbox_generate", return_value=(None, {})),
        ):
            result = synth.run("owner", "repo", 42, "pr context")

        assert result is None
        assert SynthesisState.TRY_SIMILAR in synth.trace
        assert SynthesisState.LLM_GENERATE in synth.trace
        assert SynthesisState.FAIL in synth.trace


class TestSynthesizerAllFail:
    def test_all_fail_returns_none(self) -> None:
        """All synthesis attempts fail, returns None."""
        synth = Synthesizer()
        with (
            patch.object(synth, "_check_cache", return_value=None),
            patch.object(synth, "_find_similar", return_value=[]),
            patch.object(synth, "_sandbox_generate", return_value=(None, {})),
        ):
            result = synth.run("owner", "repo", 42, "pr context")

        assert result is None
        assert SynthesisState.FAIL in synth.trace


class TestSynthesizerStateTransitions:
    def test_state_transitions_logged(self) -> None:
        """Verify trace has correct states in order for full failure path."""
        synth = Synthesizer()
        with (
            patch.object(synth, "_check_cache", return_value=None),
            patch.object(synth, "_find_similar", return_value=[]),
            patch.object(synth, "_sandbox_generate", return_value=(None, {})),
        ):
            synth.run("owner", "repo", 42, "pr context")

        trace = synth.trace
        assert trace[0] == SynthesisState.CHECK_CACHE
        assert trace[1] == SynthesisState.FIND_SIMILAR
        # No similar contexts found, so TRY_SIMILAR should be skipped
        assert SynthesisState.TRY_SIMILAR not in trace
        assert trace[2] == SynthesisState.LLM_GENERATE
        assert trace[3] == SynthesisState.FAIL

    @patch("datasmith.agents.synthesizer.get_client")
    def test_trace_is_copy(self, mock_get_client: MagicMock) -> None:
        """Verify that trace property returns a copy, not the internal list."""
        mock_client = MagicMock()
        cache_resp = MagicMock()
        cache_resp.data = [{"build_pkg_sh": "cached"}]
        mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.eq.return_value.execute.return_value = cache_resp
        mock_get_client.return_value = mock_client

        synth = Synthesizer()
        synth.run("owner", "repo", 1, "ctx", sha="abc")

        trace1 = synth.trace
        trace2 = synth.trace
        assert trace1 == trace2
        assert trace1 is not trace2


def _make_client_mock(
    pr_date_rows: list[dict],
    ctx_rows: list[dict],
    batch_pr_rows: list[dict],
) -> MagicMock:
    """Build a mock Supabase client that handles the three queries in _find_similar.

    Queries issued in order:
      1. pull_requests  — current PR's created_at
      2. candidate_containers — all contexts for the repo
      3. pull_requests  — batch PR dates for context issue numbers
    """
    mock_client = MagicMock()

    pr_resp1 = MagicMock()
    pr_resp1.data = pr_date_rows
    ctx_resp = MagicMock()
    ctx_resp.data = ctx_rows
    pr_resp2 = MagicMock()
    pr_resp2.data = batch_pr_rows

    # Route by table name; pull_requests is called twice so track call count.
    pr_call_count = [0]

    def table_side_effect(name: str) -> MagicMock:
        t = MagicMock()
        if name == "pull_requests":
            idx = pr_call_count[0]
            pr_call_count[0] += 1
            resp = pr_resp1 if idx == 0 else pr_resp2
            # Chain: .select().eq().eq().eq().execute()  or  .select().eq().eq().in_().execute()
            t.select.return_value.eq.return_value.eq.return_value.eq.return_value.execute.return_value = resp
            t.select.return_value.eq.return_value.eq.return_value.in_.return_value.execute.return_value = resp
        else:  # candidate_containers
            t.select.return_value.eq.return_value.eq.return_value.execute.return_value = ctx_resp
        return t

    mock_client.table.side_effect = table_side_effect
    return mock_client


class TestFindSimilar:
    @patch("datasmith.agents.synthesizer.get_client")
    def test_returns_candidate_containers(self, mock_get_client: MagicMock) -> None:
        """_find_similar queries candidate_containers table and returns DockerContext list."""
        mock_client = _make_client_mock(
            pr_date_rows=[{"created_at": "2024-01-15T12:00:00Z"}],
            ctx_rows=[
                {"issue_number": 10, "build_pkg_sh": "#!/bin/bash\npkg1", "build_run_sh": "#!/bin/bash\nrun1"},
                {"issue_number": 20, "build_pkg_sh": "#!/bin/bash\npkg2", "build_run_sh": ""},
            ],
            batch_pr_rows=[
                {"issue_number": 10, "created_at": "2024-01-10T12:00:00Z"},
                {"issue_number": 20, "created_at": "2024-01-14T12:00:00Z"},
            ],
        )
        mock_get_client.return_value = mock_client

        synth = Synthesizer()
        results = synth._find_similar("owner", "repo", 42)

        assert len(results) == 2
        assert isinstance(results[0], DockerContext)
        # issue 20 is 1 day away, issue 10 is 5 days away — closer one first
        assert results[0].build_pkg_sh == "#!/bin/bash\npkg2"
        assert results[1].build_pkg_sh == "#!/bin/bash\npkg1"

    @patch("datasmith.agents.synthesizer.get_client")
    def test_skips_empty_pkg(self, mock_get_client: MagicMock) -> None:
        """Rows without build_pkg_sh are filtered out."""
        mock_client = _make_client_mock(
            pr_date_rows=[{"created_at": "2024-01-15T12:00:00Z"}],
            ctx_rows=[
                {"issue_number": 1, "build_pkg_sh": "", "build_run_sh": "run"},
                {"issue_number": 2, "build_pkg_sh": "pkg", "build_run_sh": ""},
            ],
            batch_pr_rows=[{"issue_number": 2, "created_at": "2024-01-14T12:00:00Z"}],
        )
        mock_get_client.return_value = mock_client

        synth = Synthesizer()
        results = synth._find_similar("owner", "repo", 99)

        assert len(results) == 1
        assert results[0].build_pkg_sh == "pkg"

    @patch("datasmith.agents.synthesizer.get_client")
    def test_chronological_ordering(self, mock_get_client: MagicMock) -> None:
        """Contexts are returned in order of chronological proximity to the current PR."""
        base = datetime.datetime(2024, 6, 1, tzinfo=datetime.timezone.utc)
        # Distances from base: 60 days, 5 days, 30 days
        mock_client = _make_client_mock(
            pr_date_rows=[{"created_at": "2024-06-01T00:00:00Z"}],
            ctx_rows=[
                {"issue_number": 1, "build_pkg_sh": "pkg-60d", "build_run_sh": ""},
                {"issue_number": 2, "build_pkg_sh": "pkg-5d", "build_run_sh": ""},
                {"issue_number": 3, "build_pkg_sh": "pkg-30d", "build_run_sh": ""},
            ],
            batch_pr_rows=[
                {"issue_number": 1, "created_at": "2024-04-02T00:00:00Z"},  # 60 days before
                {"issue_number": 2, "created_at": "2024-05-27T00:00:00Z"},  # 5 days before
                {"issue_number": 3, "created_at": "2024-05-02T00:00:00Z"},  # 30 days before
            ],
        )
        mock_get_client.return_value = mock_client

        synth = Synthesizer()
        results = synth._find_similar("owner", "repo", 99)

        assert len(results) == 3
        assert results[0].build_pkg_sh == "pkg-5d"
        assert results[1].build_pkg_sh == "pkg-30d"
        assert results[2].build_pkg_sh == "pkg-60d"

    @patch("datasmith.agents.synthesizer.get_client")
    def test_limits_to_five_closest_when_more_exist(self, mock_get_client: MagicMock) -> None:
        """With >5 contexts, only the 5 chronologically closest are returned."""
        # 7 contexts at distances 1,2,3,4,5,6,7 days before reference date
        ctx_rows = [{"issue_number": i, "build_pkg_sh": f"pkg-{i}d", "build_run_sh": ""} for i in range(1, 8)]
        batch_rows = [
            {"issue_number": i, "created_at": f"2024-05-{31 - i:02d}T00:00:00Z"}  # i days before June 1
            for i in range(1, 8)
        ]
        mock_client = _make_client_mock(
            pr_date_rows=[{"created_at": "2024-06-01T00:00:00Z"}],
            ctx_rows=ctx_rows,
            batch_pr_rows=batch_rows,
        )
        mock_get_client.return_value = mock_client

        synth = Synthesizer()
        results = synth._find_similar("owner", "repo", 99)

        assert len(results) == 5
        # Closest 5: 1d, 2d, 3d, 4d, 5d
        assert results[0].build_pkg_sh == "pkg-1d"
        assert results[1].build_pkg_sh == "pkg-2d"
        assert results[4].build_pkg_sh == "pkg-5d"

    @patch("datasmith.agents.synthesizer.get_client")
    def test_contexts_without_issue_number_sorted_last(self, mock_get_client: MagicMock) -> None:
        """Contexts whose issue_number cannot be resolved get sorted to the end."""
        mock_client = _make_client_mock(
            pr_date_rows=[{"created_at": "2024-06-01T00:00:00Z"}],
            ctx_rows=[
                {"issue_number": None, "build_pkg_sh": "pkg-no-date", "build_run_sh": ""},
                {"issue_number": 1, "build_pkg_sh": "pkg-dated", "build_run_sh": ""},
            ],
            batch_pr_rows=[{"issue_number": 1, "created_at": "2024-05-15T00:00:00Z"}],
        )
        mock_get_client.return_value = mock_client

        synth = Synthesizer()
        results = synth._find_similar("owner", "repo", 99)

        assert len(results) == 2
        assert results[0].build_pkg_sh == "pkg-dated"
        assert results[1].build_pkg_sh == "pkg-no-date"

    @patch("datasmith.agents.synthesizer.get_client")
    def test_fallback_when_current_pr_not_found(self, mock_get_client: MagicMock) -> None:
        """When the current PR has no created_at, return up to 5 results without ordering."""
        mock_client = _make_client_mock(
            pr_date_rows=[],  # current PR not found
            ctx_rows=[{"issue_number": i, "build_pkg_sh": f"pkg-{i}", "build_run_sh": ""} for i in range(1, 4)],
            batch_pr_rows=[],
        )
        mock_get_client.return_value = mock_client

        synth = Synthesizer()
        results = synth._find_similar("owner", "repo", 99)

        # Should still return contexts (up to 5), just unordered
        assert len(results) == 3
        assert all(isinstance(r, DockerContext) for r in results)


class TestNoneAgentSkipsLLM:
    def test_none_agent_skips_llm_generate(self) -> None:
        """With agent='none', synthesizer skips LLM_GENERATE and returns None."""
        synth = Synthesizer(agent="none")
        with (
            patch.object(synth, "_check_cache", return_value=None),
            patch.object(synth, "_find_similar", return_value=[]),
        ):
            result = synth.run("owner", "repo", 42, "pr context")

        assert result is None
        assert SynthesisState.CHECK_CACHE in synth.trace
        assert SynthesisState.FIND_SIMILAR in synth.trace
        assert SynthesisState.LLM_GENERATE not in synth.trace
        assert SynthesisState.FAIL in synth.trace

    @patch("datasmith.agents.synthesizer.verify_context")
    def test_none_agent_still_tries_similar(self, mock_verify: MagicMock) -> None:
        """With agent='none', similar contexts are still tried before giving up."""
        similar_ctx = DockerContext(
            build_pkg_sh="#!/bin/bash\necho similar",
            build_run_sh="#!/bin/bash\necho run",
        )
        mock_verify.return_value = SandboxResult(success=True, docker_context=similar_ctx)

        synth = Synthesizer(agent="none")
        with (
            patch.object(synth, "_check_cache", return_value=None),
            patch.object(synth, "_find_similar", return_value=[similar_ctx]),
            patch.object(synth, "_save_context"),
        ):
            result = synth.run("owner", "repo", 42, "pr context")

        assert result is not None
        assert result.build_pkg_sh == "#!/bin/bash\necho similar"
        assert SynthesisState.TRY_SIMILAR in synth.trace
        assert SynthesisState.LLM_GENERATE not in synth.trace


class TestResourceMetricsPersistence:
    @patch("datasmith.agents.synthesizer.verify_context")
    def test_try_similar_passes_metrics_to_save_context(self, mock_verify: MagicMock) -> None:
        """When TRY_SIMILAR succeeds, resource_metrics flow to _save_context."""
        similar_ctx = DockerContext(build_pkg_sh="pkg", build_run_sh="run")
        metrics = {"build_duration_s": 8.0, "peak_memory_bytes": 2_000_000}
        mock_verify.return_value = SandboxResult(
            success=True,
            docker_context=similar_ctx,
            resource_metrics=metrics,
        )

        synth = Synthesizer()
        with (
            patch.object(synth, "_check_cache", return_value=None),
            patch.object(synth, "_find_similar", return_value=[similar_ctx]),
            patch.object(synth, "_save_context") as mock_save,
        ):
            synth.run("owner", "repo", 42, "pr context")

        mock_save.assert_called_once()
        assert mock_save.call_args.kwargs["resource_metrics"] == metrics

    @patch("datasmith.agents.synthesizer.get_client")
    def test_log_attempt_includes_resource_metrics(self, mock_get_client: MagicMock) -> None:
        """_log_attempt persists resource_metrics to error_logs."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        metrics = {"build_duration_s": 5.0, "image_size_bytes": 1_000_000}
        result = SandboxResult(
            success=False,
            failure_json={"stage": "build", "return_code": 1, "error_message": "err"},
            resource_metrics=metrics,
        )

        synth = Synthesizer()
        synth._log_attempt("o", "r", "sha123", 1, 0, result)

        insert_call = mock_client.table.return_value.insert
        insert_call.assert_called_once()
        row = insert_call.call_args[0][0]
        assert row["resource_metrics"] == metrics


class TestFormatPriorAttempts:
    def test_formats_failed_attempts(self) -> None:
        ctx = DockerContext(build_pkg_sh="#!/bin/bash\npkg", build_run_sh="#!/bin/bash\nrun")
        result = SandboxResult(
            success=False,
            failure_json={"stage": "build", "return_code": 1, "error_message": "missing dep"},
            agent_output="build output here",
        )

        text = _format_prior_attempts([(ctx, result)])

        assert "## Attempt 1" in text
        assert "**Stage**: build" in text
        assert "**Return code**: 1" in text
        assert "### docker_build_pkg.sh" in text
        assert "#!/bin/bash\npkg" in text
        assert "### docker_build_run.sh" in text
        assert "#!/bin/bash\nrun" in text
        assert "missing dep" in text
        assert "build output here" in text

    def test_empty_attempts(self) -> None:
        text = _format_prior_attempts([])
        assert "# Prior Attempts" in text
        assert "## Attempt" not in text
