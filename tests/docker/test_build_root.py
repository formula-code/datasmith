"""The discovered package root must reach the image."""

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pytest

from datasmith.docker.images import ImageManager

TEMPLATES = Path("src/datasmith/docker/templates")


def test_dockerfile_declares_build_root():
    text = (TEMPLATES / "Dockerfile.repo").read_text()
    assert "ARG BUILD_ROOT" in text


def test_dockerfile_uses_build_root_for_the_workdir():
    text = (TEMPLATES / "Dockerfile.repo").read_text()
    assert "WORKDIR /workspace/repo" not in text.replace("WORKDIR /workspace/repo/${BUILD_ROOT}", "")


def test_build_repo_image_accepts_build_root():
    import inspect

    assert "build_root" in inspect.signature(ImageManager.build_repo_image).parameters


class TestTheRootReachesTheBuild:
    """A parameter nothing passes is the bug this task exists to fix.

    ``primary_root`` was discovered correctly for 733 rows and then discarded.
    Accepting a ``build_root`` argument is not the fix on its own — the value
    has to arrive as the ``BUILD_ROOT`` build arg, and the empty and null
    cases have to land on ``.`` rather than on an empty path segment.
    """

    @pytest.fixture()
    def manager(self) -> Iterator[ImageManager]:
        with patch("datasmith.docker.images.DockerClient") as mock_cls:
            mgr = ImageManager(timeout=60)
            mgr._mock_docker = mock_cls.return_value  # type: ignore[attr-defined]
            yield mgr

    def _build_args(self, manager: ImageManager) -> dict[str, str]:
        return manager._mock_docker.build.call_args[1]["build_args"]  # type: ignore[attr-defined,no-any-return]

    def test_build_root_becomes_a_build_arg(self, manager: ImageManager) -> None:
        manager.build_repo_image("apache", "arrow", "/tmp/ctx", build_root="python")
        assert self._build_args(manager)["BUILD_ROOT"] == "python"

    def test_the_default_is_the_repository_root(self, manager: ImageManager) -> None:
        manager.build_repo_image("numpy", "numpy", "/tmp/ctx")
        assert self._build_args(manager)["BUILD_ROOT"] == "."

    def test_an_empty_root_is_not_an_empty_path_segment(self, manager: ImageManager) -> None:
        # ``packages.primary_root`` is nullable, and every legacy row predates
        # this argument. "" would build in /workspace/repo/ by accident.
        manager.build_repo_image("numpy", "numpy", "/tmp/ctx", build_root="")
        assert self._build_args(manager)["BUILD_ROOT"] == "."


def test_prerequisite_images_forward_the_build_root() -> None:
    """The runner is the only caller that knows the row's ``primary_root``."""
    import inspect

    from datasmith.runners.synthesize_images import _ensure_prerequisite_images

    assert "build_root" in inspect.signature(_ensure_prerequisite_images).parameters
