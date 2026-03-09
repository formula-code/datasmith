"""Tests for datasmith.docker.images — ImageManager."""

from __future__ import annotations

import concurrent.futures
from unittest.mock import MagicMock, patch

import pytest

from datasmith.docker.images import ImageManager

_CTX = "/tmp/ctx"  # noqa: S108


@pytest.fixture()
def manager() -> ImageManager:
    with patch("datasmith.docker.images.DockerClient") as mock_cls:
        mgr = ImageManager(timeout=60)
        mgr._mock_docker = mock_cls.return_value  # type: ignore[attr-defined]
        yield mgr


class TestBuildBaseImage:
    def test_build_base_image_returns_tag(self, manager: ImageManager) -> None:
        tag = manager.build_base_image(_CTX)
        assert tag == "formulacode/base:latest"
        manager._mock_docker.build.assert_called_once_with(  # type: ignore[attr-defined]
            _CTX,
            tags=["formulacode/base:latest"],
            file=f"{_CTX}/Dockerfile.base",
        )

    def test_build_base_image_default_context(self, manager: ImageManager) -> None:
        tag = manager.build_base_image()
        assert tag == "formulacode/base:latest"
        manager._mock_docker.build.assert_called_once_with(  # type: ignore[attr-defined]
            ".",
            tags=["formulacode/base:latest"],
            file="./Dockerfile.base",
        )


class TestBuildRepoImage:
    def test_build_repo_image_from_base(self, manager: ImageManager) -> None:
        tag = manager.build_repo_image("pandas-dev", "pandas", _CTX)
        assert tag == "formulacode/pandas-dev-pandas:latest"
        manager._mock_docker.build.assert_called_once_with(  # type: ignore[attr-defined]
            _CTX,
            tags=["formulacode/pandas-dev-pandas:latest"],
            file=f"{_CTX}/Dockerfile.repo",
            build_args={"BASE_IMAGE": "formulacode/base:latest"},
        )


class TestBuildPrImage:
    def test_build_pr_image_from_repo(self, manager: ImageManager) -> None:
        tag = manager.build_pr_image("pandas-dev", "pandas", 42, _CTX)
        assert tag == "formulacode/pandas-dev-pandas:42"
        manager._mock_docker.build.assert_called_once_with(  # type: ignore[attr-defined]
            _CTX,
            tags=["formulacode/pandas-dev-pandas:42"],
            file=f"{_CTX}/Dockerfile.pr",
            build_args={"REPO_IMAGE": "formulacode/pandas-dev-pandas:latest"},
        )

    def test_build_pr_image_with_build_script(self, manager: ImageManager) -> None:
        tag = manager.build_pr_image("numpy", "numpy", 99, _CTX, build_script="custom_build.sh")
        assert tag == "formulacode/numpy-numpy:99"
        call_kwargs = manager._mock_docker.build.call_args  # type: ignore[attr-defined]
        assert call_kwargs[1]["build_args"]["BUILD_SCRIPT"] == "custom_build.sh"


class TestImageExists:
    def test_image_exists_true(self, manager: ImageManager) -> None:
        manager._mock_docker.image.inspect.return_value = MagicMock()  # type: ignore[attr-defined]
        assert manager.image_exists("formulacode/base:latest") is True

    def test_image_exists_false(self, manager: ImageManager) -> None:
        manager._mock_docker.image.inspect.side_effect = Exception("not found")  # type: ignore[attr-defined]
        assert manager.image_exists("formulacode/base:latest") is False


class TestThreadSafety:
    def test_concurrent_builds(self) -> None:
        """Five threads building images concurrently should not raise."""
        with patch("datasmith.docker.images.DockerClient") as mock_cls:
            mock_docker = mock_cls.return_value
            mock_docker.build.return_value = None

            mgr = ImageManager(timeout=60)

            def build_task(i: int) -> str:
                return mgr.build_repo_image(f"owner{i}", f"repo{i}", _CTX)

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(build_task, i) for i in range(5)]
                results = [f.result() for f in concurrent.futures.as_completed(futures)]

            assert len(results) == 5
            assert mock_docker.build.call_count == 5
            # Each result should be a unique tag
            assert len(set(results)) == 5


class TestRemoveAndPrune:
    def test_remove_image(self, manager: ImageManager) -> None:
        manager.remove_image("formulacode/base:latest")
        manager._mock_docker.image.remove.assert_called_once_with(  # type: ignore[attr-defined]
            "formulacode/base:latest", force=True
        )

    def test_remove_image_failure_does_not_raise(self, manager: ImageManager) -> None:
        manager._mock_docker.image.remove.side_effect = Exception("fail")  # type: ignore[attr-defined]
        # Should not raise
        manager.remove_image("formulacode/base:latest")

    def test_prune_dangling(self, manager: ImageManager) -> None:
        manager.prune_dangling()
        manager._mock_docker.image.prune.assert_called_once_with(all=False)  # type: ignore[attr-defined]
