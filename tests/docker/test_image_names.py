"""The tag must name what varies inside the image."""

import re
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


class TestThePackageRootIsPartOfTheName:
    """The WORKDIR is what the root changes, and the WORKDIR is inside the image.

    ``primary_root`` varies per commit, the repository image does not, so a
    root that is not in the tag is a root that one commit chooses for every
    other commit of the same repository and interpreter.
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

    def test_two_roots_give_two_tags(self) -> None:
        # Qiskit: 384 rows rooted at the repository root shared one image with
        # rows rooted at qiskit_pkg, and the first one built decided.
        at_root = get_repo_image_name("Qiskit", "qiskit", "3.12")
        at_pkg = get_repo_image_name("Qiskit", "qiskit", "3.12", "qiskit_pkg")
        assert at_root != at_pkg

    @pytest.mark.parametrize("root", ["", ".", "./", "/", "  ", "./."])
    def test_the_repository_root_adds_nothing_to_the_tag(self, root: str) -> None:
        # Every legacy row and the 88% majority must keep the tag they have.
        assert get_repo_image_name("dask", "dask", "3.11", root) == get_repo_image_name("dask", "dask", "3.11")

    def test_sibling_roots_never_share_a_tag(self) -> None:
        # apache/arrow-adbc really does carry both of these, 73 rows between
        # them. A slug that truncates or sanitises them together is the bug.
        a = get_repo_image_name("apache", "arrow-adbc", "3.12", "python/adbc_driver_bigquery")
        b = get_repo_image_name("apache", "arrow-adbc", "3.12", "python/adbc_driver_flightsql")
        assert a != b

    def test_the_same_root_gives_the_same_tag(self) -> None:
        assert get_repo_image_name("apache", "arrow", "3.11", "python") == get_repo_image_name(
            "apache", "arrow", "3.11", "./python"
        )

    @pytest.mark.parametrize("root", ["python", "python/adbc_driver_bigquery", "Some Dir/pkg", "../pkg", "a" * 200])
    def test_the_tag_stays_a_legal_docker_reference(self, root: str) -> None:
        tag = get_repo_image_name("apache", "arrow", "3.11", root).rsplit(":", 1)[1]
        assert re.fullmatch(r"[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}", tag), tag

    def test_build_pr_image_names_the_parent_that_carries_the_root(self, manager: ImageManager) -> None:
        repo_tag = manager.build_repo_image("apache", "arrow", "/tmp/ctx", py_version="3.11", build_root="python")
        manager.build_pr_image("apache", "arrow", 42, "/tmp/ctx", py_version="3.11", build_root="python")
        args = manager._mock_docker.build.call_args[1]["build_args"]  # type: ignore[attr-defined]
        assert args["REPO_IMAGE"] == repo_tag

    def test_two_roots_do_not_share_a_parent(self, manager: ImageManager) -> None:
        manager.build_pr_image("Qiskit", "qiskit", 42, "/tmp/ctx", py_version="3.12")
        first = manager._mock_docker.build.call_args[1]["build_args"]["REPO_IMAGE"]  # type: ignore[attr-defined]
        manager.build_pr_image("Qiskit", "qiskit", 43, "/tmp/ctx", py_version="3.12", build_root="qiskit_pkg")
        second = manager._mock_docker.build.call_args[1]["build_args"]["REPO_IMAGE"]  # type: ignore[attr-defined]
        assert first != second

    @pytest.mark.parametrize(
        ("a", "b"),
        [
            # Sanitising loses the difference: "/" and "-" both become "-".
            ("pkg/a", "pkg-a"),
            # Truncating loses it too — these agree for the first 40 characters.
            ("src/" + "x" * 40 + "/one", "src/" + "x" * 40 + "/two"),
        ],
    )
    def test_roots_that_sanitise_or_truncate_alike_keep_their_own_tag(self, a: str, b: str) -> None:
        # The slug is for a human reading `docker images`; the digest is what
        # makes "two roots, two images" hold for every pair of roots.
        assert get_repo_image_name("acme", "widget", "3.12", a) != get_repo_image_name("acme", "widget", "3.12", b)
