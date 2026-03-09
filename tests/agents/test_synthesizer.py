"""Tests for datasmith.agents.synthesizer."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from datasmith.agents.synthesizer import SynthesisState, Synthesizer
from datasmith.docker.verifiers import VerifyResult


def _make_verifier(ok: bool = True) -> MagicMock:
    """Create a mock verifier that returns a VerifyResult."""
    verifier = MagicMock()
    verifier.verify.return_value = VerifyResult(
        ok=ok,
        rc=0 if ok else 1,
        stdout="output" if ok else "",
        stderr="" if ok else "build failed",
        duration_s=1.0,
        stage="test",
    )
    return verifier


class TestSynthesizerCacheHit:
    @patch("datasmith.agents.synthesizer.get_client")
    def test_cache_hit_returns_immediately(self, mock_get_client: MagicMock) -> None:
        """Mock cache hit returns DockerContext immediately."""
        mock_client = MagicMock()
        mock_resp = MagicMock()
        mock_resp.data = [{"result_json": {"build_pkg_sh": "#!/bin/bash\necho cached"}}]
        mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = (
            mock_resp
        )
        mock_get_client.return_value = mock_client

        verifier = _make_verifier()
        synth = Synthesizer()
        result = synth.run("owner", "repo", 42, "pr context", verifier)

        assert result is not None
        assert result.build_pkg_sh == "#!/bin/bash\necho cached"
        assert SynthesisState.CHECK_CACHE in synth.trace
        # Should not proceed to other states
        assert SynthesisState.FIND_SIMILAR not in synth.trace
        assert SynthesisState.LLM_GENERATE not in synth.trace


class TestSynthesizerSimilarScript:
    @patch("datasmith.agents.synthesizer.get_client")
    def test_similar_script_passes(self, mock_get_client: MagicMock) -> None:
        """Cache miss, similar script passes verification."""
        mock_client = MagicMock()

        # Cache miss
        cache_resp = MagicMock()
        cache_resp.data = []

        # Similar scripts found
        similar_resp = MagicMock()
        similar_resp.data = [{"script": "#!/bin/bash\necho similar"}]

        # Insert for save_attempt
        insert_resp = MagicMock()

        def table_side_effect(name: str) -> MagicMock:
            table_mock = MagicMock()
            if name == "hook_cache":
                table_mock.select.return_value.eq.return_value.eq.return_value.execute.return_value = cache_resp
            elif name == "build_attempts":
                # For select (find_similar)
                table_mock.select.return_value.eq.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = similar_resp
                # For insert (save_attempt)
                table_mock.insert.return_value.execute.return_value = insert_resp
            return table_mock

        mock_client.table.side_effect = table_side_effect
        mock_get_client.return_value = mock_client

        verifier = _make_verifier(ok=True)
        synth = Synthesizer()
        result = synth.run("owner", "repo", 42, "pr context", verifier)

        assert result is not None
        assert result.build_pkg_sh == "#!/bin/bash\necho similar"
        assert SynthesisState.CHECK_CACHE in synth.trace
        assert SynthesisState.FIND_SIMILAR in synth.trace
        assert SynthesisState.TRY_SIMILAR in synth.trace

    @patch("datasmith.agents.synthesizer.get_client")
    def test_similar_script_fails_falls_through(self, mock_get_client: MagicMock) -> None:
        """Similar script fails verification, falls through to LLM generation."""
        mock_client = MagicMock()

        # Cache miss
        cache_resp = MagicMock()
        cache_resp.data = []

        # Similar scripts found but they will fail
        similar_resp = MagicMock()
        similar_resp.data = [{"script": "#!/bin/bash\necho fails"}]

        def table_side_effect(name: str) -> MagicMock:
            table_mock = MagicMock()
            if name == "hook_cache":
                table_mock.select.return_value.eq.return_value.eq.return_value.execute.return_value = cache_resp
            elif name == "build_attempts":
                table_mock.select.return_value.eq.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = similar_resp
                table_mock.insert.return_value.execute.return_value = MagicMock()
            return table_mock

        mock_client.table.side_effect = table_side_effect
        mock_get_client.return_value = mock_client

        # Similar fails, LLM also fails
        fail_verifier = _make_verifier(ok=False)

        with patch("datasmith.agents.synthesizer.Synthesizer._llm_generate", return_value=None):
            synth = Synthesizer()
            result = synth.run("owner", "repo", 42, "pr context", fail_verifier)

        assert result is None
        assert SynthesisState.TRY_SIMILAR in synth.trace
        assert SynthesisState.LLM_GENERATE in synth.trace
        assert SynthesisState.FAIL in synth.trace


class TestSynthesizerAllFail:
    @patch("datasmith.agents.synthesizer.get_client")
    def test_all_fail_returns_none(self, mock_get_client: MagicMock) -> None:
        """All synthesis attempts fail, returns None."""
        mock_client = MagicMock()

        # Cache miss
        cache_resp = MagicMock()
        cache_resp.data = []

        # No similar scripts
        similar_resp = MagicMock()
        similar_resp.data = []

        def table_side_effect(name: str) -> MagicMock:
            table_mock = MagicMock()
            if name == "hook_cache":
                table_mock.select.return_value.eq.return_value.eq.return_value.execute.return_value = cache_resp
            elif name == "build_attempts":
                table_mock.select.return_value.eq.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = similar_resp
                table_mock.insert.return_value.execute.return_value = MagicMock()
            return table_mock

        mock_client.table.side_effect = table_side_effect
        mock_get_client.return_value = mock_client

        with patch("datasmith.agents.synthesizer.Synthesizer._llm_generate", return_value=None):
            synth = Synthesizer()
            result = synth.run("owner", "repo", 42, "pr context", _make_verifier(ok=False))

        assert result is None
        assert SynthesisState.FAIL in synth.trace


class TestSynthesizerStateTransitions:
    @patch("datasmith.agents.synthesizer.get_client")
    def test_state_transitions_logged(self, mock_get_client: MagicMock) -> None:
        """Verify trace has correct states in order for full failure path."""
        mock_client = MagicMock()

        # Cache miss
        cache_resp = MagicMock()
        cache_resp.data = []

        # No similar scripts
        similar_resp = MagicMock()
        similar_resp.data = []

        def table_side_effect(name: str) -> MagicMock:
            table_mock = MagicMock()
            if name == "hook_cache":
                table_mock.select.return_value.eq.return_value.eq.return_value.execute.return_value = cache_resp
            elif name == "build_attempts":
                table_mock.select.return_value.eq.return_value.eq.return_value.eq.return_value.limit.return_value.execute.return_value = similar_resp
                table_mock.insert.return_value.execute.return_value = MagicMock()
            return table_mock

        mock_client.table.side_effect = table_side_effect
        mock_get_client.return_value = mock_client

        with patch("datasmith.agents.synthesizer.Synthesizer._llm_generate", return_value=None):
            synth = Synthesizer()
            synth.run("owner", "repo", 42, "pr context", _make_verifier(ok=False))

        trace = synth.trace
        assert trace[0] == SynthesisState.CHECK_CACHE
        assert trace[1] == SynthesisState.FIND_SIMILAR
        # No similar scripts found, so TRY_SIMILAR should be skipped
        assert SynthesisState.TRY_SIMILAR not in trace
        assert trace[2] == SynthesisState.LLM_GENERATE
        assert trace[3] == SynthesisState.FAIL

    @patch("datasmith.agents.synthesizer.get_client")
    def test_trace_is_copy(self, mock_get_client: MagicMock) -> None:
        """Verify that trace property returns a copy, not the internal list."""
        mock_client = MagicMock()
        cache_resp = MagicMock()
        cache_resp.data = [{"result_json": {"build_pkg_sh": "cached"}}]
        mock_client.table.return_value.select.return_value.eq.return_value.eq.return_value.execute.return_value = (
            cache_resp
        )
        mock_get_client.return_value = mock_client

        synth = Synthesizer()
        synth.run("owner", "repo", 1, "ctx", _make_verifier())

        trace1 = synth.trace
        trace2 = synth.trace
        assert trace1 == trace2
        assert trace1 is not trace2
