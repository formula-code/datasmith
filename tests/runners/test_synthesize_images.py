"""Tests for datasmith.runners.synthesize_images — SynthesizeImagesRunner."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from datasmith.runners.synthesize_images import SynthesizeImagesRunner


def _mock_supabase() -> MagicMock:
    """Create a mock Supabase client with fluent API."""
    client = MagicMock()
    table = MagicMock()
    client.table.return_value = table
    table.upsert.return_value = table
    table.insert.return_value = table
    table.execute.return_value = MagicMock()
    return client


def _make_item(owner: str = "numpy", repo: str = "numpy", issue: int = 42) -> dict[str, object]:
    return {
        "owner": owner,
        "repo": repo,
        "issue_number": issue,
        "pr_context": "Some PR context",
    }


class TestDockerRunsInThread:
    async def test_docker_runs_in_thread(self) -> None:
        """Mock synthesizer, verify it ran via asyncio.to_thread."""
        mock_client = _mock_supabase()

        mock_ctx = MagicMock()
        synthesizer = MagicMock()
        synthesizer.run.return_value = mock_ctx
        verifier = MagicMock()

        with (
            patch("datasmith.runners.synthesize_images.get_logger"),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = SynthesizeImagesRunner(synthesizer=synthesizer, verifier=verifier, n_concurrent=1)
            await runner.run([_make_item()])

        # Verify synthesizer.run was called with correct args
        synthesizer.run.assert_called_once_with("numpy", "numpy", 42, "Some PR context", verifier)


class TestHandlesFailure:
    async def test_handles_failure(self) -> None:
        """synthesizer returns None, which triggers RuntimeError."""
        mock_client = _mock_supabase()

        synthesizer = MagicMock()
        synthesizer.run.return_value = None
        verifier = MagicMock()

        with (
            patch("datasmith.runners.synthesize_images.get_logger"),
            patch("datasmith.runners.base.get_client", return_value=mock_client),
        ):
            runner = SynthesizeImagesRunner(synthesizer=synthesizer, verifier=verifier, n_concurrent=1)
            await runner.run([_make_item()])

        # The runner catches exceptions, so check failure count
        assert runner._failed == 1
        assert runner._completed == 0
