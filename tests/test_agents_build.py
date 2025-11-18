"""Tests for build agent utilities.

This module tests helper functions and dataclasses used in the build agent module.
"""

from __future__ import annotations

import pickle
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from docker.errors import ImageNotFound

from datasmith.agents.build import (
    AttemptRecord,
    _image_exists,
    _merge_tail,
    _save_pickle,
    _ts_to_iso,
)
from datasmith.core.models import BuildResult
from datasmith.docker.context import DockerContext


class TestTsToIso:
    """Tests for timestamp to ISO conversion."""

    def test_ts_to_iso_converts_timestamp(self) -> None:
        """Test Unix timestamp conversion to ISO format."""
        # 2024-01-01 00:00:00 UTC
        timestamp = 1704067200.0
        iso_string = _ts_to_iso(timestamp)

        assert iso_string.startswith("2024-01-01")
        assert iso_string.endswith("Z")

    def test_ts_to_iso_handles_none(self) -> None:
        """Test that None returns empty string."""
        result = _ts_to_iso(None)
        assert result == ""

    def test_ts_to_iso_handles_invalid(self) -> None:
        """Test that invalid input returns string representation."""
        result = _ts_to_iso("invalid")  # type: ignore[arg-type]
        assert result == "invalid"

    def test_ts_to_iso_handles_integer(self) -> None:
        """Test that integer timestamps work."""
        timestamp = 1704067200
        iso_string = _ts_to_iso(timestamp)

        assert iso_string.startswith("2024-01-01")
        assert iso_string.endswith("Z")


class TestMergeTail:
    """Tests for log tail merging."""

    def test_merge_tail_combines_logs(self) -> None:
        """Test that stderr and stdout are merged."""
        stderr = "Error line 1\nError line 2"
        stdout = "Output line 1\nOutput line 2"

        merged = _merge_tail(stderr, stdout)

        assert "Error line 1" in merged
        assert "Error line 2" in merged
        assert "Output line 1" in merged
        assert "Output line 2" in merged

    def test_merge_tail_respects_max_len(self) -> None:
        """Test that output is truncated to max_len."""
        stderr = "A" * 5000
        stdout = "B" * 5000

        merged = _merge_tail(stderr, stdout, max_len=100)

        assert len(merged) == 100
        # Should keep the last 100 characters
        assert merged[-1] == "B"

    def test_merge_tail_handles_empty_stderr(self) -> None:
        """Test with empty stderr."""
        merged = _merge_tail("", "stdout content")

        assert "stdout content" in merged

    def test_merge_tail_handles_empty_stdout(self) -> None:
        """Test with empty stdout."""
        merged = _merge_tail("stderr content", "")

        assert "stderr content" in merged

    def test_merge_tail_handles_none_values(self) -> None:
        """Test with None values (treats as empty)."""
        merged = _merge_tail(None, "stdout content")  # type: ignore[arg-type]

        assert "stdout content" in merged


class TestSavePickle:
    """Tests for Docker context serialization."""

    def test_save_pickle_creates_file(self, tmp_path: Path) -> None:
        """Test that pickle file is created."""
        ctx = DockerContext()
        path = tmp_path / "context.pkl"

        _save_pickle(ctx, path)

        assert path.exists()

    def test_save_pickle_creates_parent_dirs(self, tmp_path: Path) -> None:
        """Test that parent directories are created."""
        ctx = DockerContext()
        path = tmp_path / "subdir" / "context.pkl"

        _save_pickle(ctx, path)

        assert path.exists()
        assert path.parent.exists()

    def test_save_pickle_can_be_loaded(self, tmp_path: Path) -> None:
        """Test that saved pickle can be loaded back."""
        ctx = DockerContext()
        path = tmp_path / "context.pkl"

        _save_pickle(ctx, path)

        with open(path, "rb") as f:
            loaded_ctx = pickle.load(f)  # noqa: S301

        assert isinstance(loaded_ctx, DockerContext)


class TestImageExists:
    """Tests for Docker image existence checking."""

    def test_image_exists_returns_true_when_present(self) -> None:
        """Test that True is returned when image exists."""
        client = MagicMock()
        client.images.get.return_value = MagicMock()  # Image found

        result = _image_exists(client, "test:image")

        assert result is True
        client.images.get.assert_called_once_with("test:image")

    def test_image_exists_returns_false_when_missing(self) -> None:
        """Test that False is returned when image not found."""
        client = MagicMock()
        client.images.get.side_effect = ImageNotFound("not found", response=None)

        result = _image_exists(client, "test:image")

        assert result is False

    def test_image_exists_retries_on_error(self) -> None:
        """Test that function retries on transient errors."""
        client = MagicMock()
        # Fail first 2 times, succeed on 3rd
        client.images.get.side_effect = [
            Exception("transient error"),
            Exception("transient error"),
            MagicMock(),  # Success
        ]

        with patch("time.sleep"):  # Don't actually sleep
            result = _image_exists(client, "test:image", retries=3)

        assert result is True
        assert client.images.get.call_count == 3

    def test_image_exists_raises_after_max_retries(self) -> None:
        """Test that exception is raised after max retries."""
        client = MagicMock()
        client.images.get.side_effect = Exception("persistent error")

        with patch("time.sleep"), pytest.raises(Exception, match="persistent error"):
            _image_exists(client, "test:image", retries=3)

        assert client.images.get.call_count == 3

    def test_image_exists_exponential_backoff(self) -> None:
        """Test that retry delays use exponential backoff."""
        client = MagicMock()
        client.images.get.side_effect = [
            Exception("error 1"),
            Exception("error 2"),
            MagicMock(),
        ]

        with patch("time.sleep") as mock_sleep:
            _image_exists(client, "test:image", retries=3, delay=0.5)

        # Should sleep with increasing delays: 0.5, 1.0
        assert mock_sleep.call_count == 2
        calls = [call[0][0] for call in mock_sleep.call_args_list]
        assert calls[0] == 0.5  # 0.5 * 2^0
        assert calls[1] == 1.0  # 0.5 * 2^1


class TestAttemptRecord:
    """Tests for AttemptRecord dataclass."""

    def test_attempt_record_creation(self) -> None:
        """Test AttemptRecord can be created."""
        record = AttemptRecord(
            attempt_idx=1,
            building_data="build script content",
        )

        assert record.attempt_idx == 1
        assert record.building_data == "build script content"
        assert record.build_result is None

    def test_attempt_record_with_build_result(self) -> None:
        """Test AttemptRecord with BuildResult."""
        build_result = BuildResult(
            ok=True,
            image_name="test:image",
            image_id="sha256:abc123",
            rc=0,
            duration_s=10.5,
            stderr_tail="",
            stdout_tail="build logs",
        )

        record = AttemptRecord(
            attempt_idx=2,
            building_data="build script v2",
            build_result=build_result,
        )

        assert record.attempt_idx == 2
        assert record.build_result == build_result
        assert record.build_result.ok is True
        assert record.build_result.duration_s == 10.5
