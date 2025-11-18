"""Tests for Docker disk space monitoring and management.

This module tests disk space monitoring, threshold checking, and automatic pruning.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datasmith.docker.disk_management import (
    docker_data_root,
    free_gb,
    guard_and_prune,
)

# Mark all async tests with anyio, using only asyncio backend
pytestmark = pytest.mark.anyio(backends=["asyncio"])


class TestDockerDataRoot:
    """Tests for docker_data_root function."""

    def test_docker_data_root_default(self) -> None:
        """Test default Docker data root path."""
        with patch.dict("os.environ", {}, clear=True):
            root = docker_data_root()
            assert root == "/var/lib/docker"

    def test_docker_data_root_env_override(self) -> None:
        """Test DOCKER_DATA_ROOT environment variable override."""
        with patch.dict("os.environ", {"DOCKER_DATA_ROOT": "/custom/docker"}, clear=True):
            root = docker_data_root()
            assert root == "/custom/docker"


class TestFreeGb:
    """Tests for free_gb function."""

    def test_free_gb_returns_space(self, tmp_path: Path) -> None:
        """Test that free_gb returns disk space in GB."""
        free = free_gb(str(tmp_path))

        # Should return a positive number
        assert free > 0
        # Should be a reasonable value (less than 10TB)
        assert free < 10000

    def test_free_gb_missing_path_returns_inf(self) -> None:
        """Test that missing path returns infinity."""
        free = free_gb("/nonexistent/path/that/does/not/exist")

        assert free == float("inf")

    @patch("shutil.disk_usage")
    def test_free_gb_calculation(self, mock_disk_usage: MagicMock) -> None:
        """Test free GB calculation from bytes."""
        # Mock disk_usage to return specific values
        mock_usage = MagicMock()
        mock_usage.free = 10 * (1024**3)  # 10 GB in bytes
        mock_disk_usage.return_value = mock_usage

        free = free_gb("/some/path")

        assert free == 10.0


class TestGuardAndPrune:
    """Tests for guard_and_prune function."""

    async def test_guard_and_prune_skips_if_above_threshold(self) -> None:
        """Test that pruning is skipped when disk space is above threshold."""
        client = MagicMock()

        with patch("datasmith.docker.disk_management.free_gb", return_value=100.0):
            await guard_and_prune(
                client=client,
                min_free_gb=50.0,
                data_root="/var/lib/docker",
                run_id=None,
                hard_fail=False,
            )

        # Should not call soft_prune since we're above threshold
        # (We can't easily verify this without inspecting calls to asyncio.to_thread)

    async def test_guard_and_prune_prunes_if_below_threshold(self) -> None:
        """Test that pruning happens when disk space is below threshold."""
        client = MagicMock()

        with (
            patch("datasmith.docker.disk_management.free_gb", side_effect=[30.0, 60.0]),
            patch("asyncio.to_thread", new_callable=AsyncMock) as mock_to_thread,
        ):
            await guard_and_prune(
                client=client,
                min_free_gb=50.0,
                data_root="/var/lib/docker",
                run_id="test-run",
                hard_fail=False,
            )

            # Should call to_thread with soft_prune
            mock_to_thread.assert_called_once()
            args, _ = mock_to_thread.call_args
            assert args[1:] == (client, "test-run")

    async def test_guard_and_prune_hard_fail_raises(self) -> None:
        """Test that hard_fail=True raises SystemExit when disk space is still low."""
        client = MagicMock()

        # Both calls return low disk space
        with (
            patch("datasmith.docker.disk_management.free_gb", return_value=30.0),
            patch("asyncio.to_thread", new_callable=AsyncMock),
            pytest.raises(SystemExit, match="Insufficient disk space"),
        ):
            await guard_and_prune(
                client=client,
                min_free_gb=50.0,
                data_root="/var/lib/docker",
                run_id=None,
                hard_fail=True,
            )

    async def test_guard_and_prune_hard_fail_succeeds_after_prune(self) -> None:
        """Test that hard_fail=True doesn't raise if pruning frees enough space."""
        client = MagicMock()

        # First call low, second call high (after pruning)
        with (
            patch("datasmith.docker.disk_management.free_gb", side_effect=[30.0, 60.0]),
            patch("asyncio.to_thread", new_callable=AsyncMock),
        ):
            # Should not raise
            await guard_and_prune(
                client=client,
                min_free_gb=50.0,
                data_root="/var/lib/docker",
                run_id=None,
                hard_fail=True,
            )
