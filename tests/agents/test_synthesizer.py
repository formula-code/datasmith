"""Tests for datasmith.agents.synthesizer."""

from __future__ import annotations

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
            patch.object(synth, "_sandbox_generate", return_value=None),
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
            patch.object(synth, "_sandbox_generate", return_value=None),
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
            patch.object(synth, "_sandbox_generate", return_value=None),
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


class TestFindSimilar:
    @patch("datasmith.agents.synthesizer.get_client")
    def test_returns_docker_contexts(self, mock_get_client: MagicMock) -> None:
        """_find_similar queries docker_contexts table and returns DockerContext list."""
        mock_client = MagicMock()
        resp = MagicMock()
        resp.data = [
            {"build_pkg_sh": "#!/bin/bash\npkg1", "build_run_sh": "#!/bin/bash\nrun1"},
            {"build_pkg_sh": "#!/bin/bash\npkg2", "build_run_sh": ""},
        ]
        mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = resp
        mock_get_client.return_value = mock_client

        synth = Synthesizer()
        results = synth._find_similar("owner", "repo")

        assert len(results) == 2
        assert isinstance(results[0], DockerContext)
        assert results[0].build_pkg_sh == "#!/bin/bash\npkg1"
        assert results[0].build_run_sh == "#!/bin/bash\nrun1"
        assert results[1].build_run_sh == ""

        # Verify we queried docker_contexts table
        mock_client.table.assert_called_once_with("docker_contexts")

    @patch("datasmith.agents.synthesizer.get_client")
    def test_skips_empty_pkg(self, mock_get_client: MagicMock) -> None:
        """Rows without build_pkg_sh are filtered out."""
        mock_client = MagicMock()
        resp = MagicMock()
        resp.data = [
            {"build_pkg_sh": "", "build_run_sh": "run"},
            {"build_pkg_sh": "pkg", "build_run_sh": ""},
        ]
        mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = resp
        mock_get_client.return_value = mock_client

        synth = Synthesizer()
        results = synth._find_similar("owner", "repo")

        assert len(results) == 1
        assert results[0].build_pkg_sh == "pkg"


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
