"""Tests for Docker disk space monitoring and management.

This module tests disk space monitoring, threshold checking, and automatic pruning.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from datasmith.docker.disk_management import (
    docker_data_root,
    free_gb,
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
