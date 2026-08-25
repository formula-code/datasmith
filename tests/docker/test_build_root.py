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


class TestThePrerequisiteBuildAgreesWithItself:
    """The tag checked and the tag built must be one string.

    ``_ensure_prerequisite_images`` builds only when the tag is absent, so a
    lookup that names fewer facts than the build either rebuilds forever or —
    the case that shipped — reads a differently-rooted image as this row's own.
    """

    @pytest.fixture()
    def docker_manager(self) -> Iterator[object]:
        with patch("datasmith.docker.images.ImageManager") as mock_cls:
            mock_cls.return_value.image_exists.return_value = False
            yield mock_cls.return_value

    def test_the_tag_looked_up_is_the_tag_built(self, docker_manager: object) -> None:
        from datasmith.docker.images import get_repo_image_name
        from datasmith.runners.synthesize_images import _ensure_prerequisite_images

        _ensure_prerequisite_images("apache", "arrow", "3.11", "python")

        expected = get_repo_image_name("apache", "arrow", "3.11", "python")
        looked_up = [c.args[0] for c in docker_manager.image_exists.call_args_list]  # type: ignore[attr-defined]
        assert expected in looked_up

    def test_the_interpreter_and_the_root_both_reach_the_builder(self, docker_manager: object) -> None:
        from datasmith.runners.synthesize_images import _ensure_prerequisite_images

        _ensure_prerequisite_images("apache", "arrow", "3.11", "python")

        kwargs = docker_manager.build_repo_image.call_args.kwargs  # type: ignore[attr-defined]
        assert kwargs["py_version"] == "3.11"
        assert kwargs["build_root"] == "python"
