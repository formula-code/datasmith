"""The tag must name what varies inside the image."""

from collections.abc import Iterator
from unittest.mock import patch

import pytest

from datasmith.docker.images import ImageManager, get_repo_image_name


def test_interpreter_appears_in_the_tag():
    assert get_repo_image_name("apache", "arrow", "3.11").endswith(":py3.11")


def test_two_interpreters_give_two_tags():
    a = get_repo_image_name("dask", "dask", "3.9")
    b = get_repo_image_name("dask", "dask", "3.12")
    assert a != b


def test_same_interpreter_gives_a_stable_tag():
    assert get_repo_image_name("dask", "dask", "3.9") == get_repo_image_name("dask", "dask", "3.9")


def test_tag_is_lowercased():
    assert get_repo_image_name("PostHog", "posthog", "3.12") == get_repo_image_name("posthog", "posthog", "3.12")


def test_missing_version_still_yields_a_usable_tag():
    # Legacy callers and images built before this change.
    assert get_repo_image_name("dask", "dask").endswith(":latest")


class TestTheTagTheBuilderUses:
    """A tag that only the name helper agrees on is a tag nothing builds.

    The five tests above pin the helper alone. The invariant that actually
    matters is that the tag ``build_repo_image`` writes and the ``REPO_IMAGE``
    ``build_pr_image`` reads are the same string for the same interpreter —
    a missed thread-through at either call site is silent otherwise.
    """

    @pytest.fixture(autouse=True)
    def _set_namespace(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DOCKERHUB_USERNAME", "formulacode")

    @pytest.fixture()
    def manager(self) -> Iterator[ImageManager]:
        with patch("datasmith.docker.images.DockerClient") as mock_cls:
            mgr = ImageManager(timeout=60)
            mgr._mock_docker = mock_cls.return_value  # type: ignore[attr-defined]
            yield mgr

    def test_build_repo_image_tags_the_interpreter(self, manager: ImageManager) -> None:
        tag = manager.build_repo_image("apache", "arrow", "/tmp/ctx", py_version="3.11")
        assert tag == get_repo_image_name("apache", "arrow", "3.11")
        assert manager._mock_docker.build.call_args[1]["tags"] == [tag]  # type: ignore[attr-defined]

    def test_build_pr_image_reads_the_tag_build_repo_image_writes(self, manager: ImageManager) -> None:
        repo_tag = manager.build_repo_image("apache", "arrow", "/tmp/ctx", py_version="3.11")
        manager.build_pr_image("apache", "arrow", 42, "/tmp/ctx", py_version="3.11")
        args = manager._mock_docker.build.call_args[1]["build_args"]  # type: ignore[attr-defined]
        assert args["REPO_IMAGE"] == repo_tag

    def test_two_interpreters_do_not_share_a_parent(self, manager: ImageManager) -> None:
        manager.build_pr_image("apache", "arrow", 42, "/tmp/ctx", py_version="3.9")
        first = manager._mock_docker.build.call_args[1]["build_args"]["REPO_IMAGE"]  # type: ignore[attr-defined]
        manager.build_pr_image("apache", "arrow", 43, "/tmp/ctx", py_version="3.12")
        second = manager._mock_docker.build.call_args[1]["build_args"]["REPO_IMAGE"]  # type: ignore[attr-defined]
        assert first != second
