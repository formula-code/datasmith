"""Tests for datasmith.docker.images — ImageManager."""

from __future__ import annotations

import concurrent.futures
from unittest.mock import MagicMock, patch

import pytest

from datasmith.docker.images import (
    _BUILDER,
    ImageManager,
    _default_context,
    get_base_image_name,
    get_pr_image_name,
    get_repo_image_name,
)

_CTX = "/tmp/ctx"


@pytest.fixture()
def manager() -> ImageManager:
    with patch("datasmith.docker.images.DockerClient") as mock_cls:
        mgr = ImageManager(timeout=60)
        mgr._mock_docker = mock_cls.return_value  # type: ignore[attr-defined]
        yield mgr


class TestDefaultContext:
    def test_default_context_returns_templates_dir(self) -> None:
        ctx = _default_context()
        assert ctx.endswith("templates")

    def test_default_context_contains_dockerfiles(self) -> None:
        import os

        ctx = _default_context()
        assert os.path.isfile(os.path.join(ctx, "Dockerfile.base"))
        assert os.path.isfile(os.path.join(ctx, "Dockerfile.repo"))
        assert os.path.isfile(os.path.join(ctx, "Dockerfile.pr"))

    def test_instance_default_context(self) -> None:
        with patch("datasmith.docker.images.DockerClient"):
            mgr = ImageManager()
            assert mgr._default_context() == _default_context()


class TestImageNameHelpers:
    @pytest.fixture(autouse=True)
    def _set_namespace(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DOCKERHUB_USERNAME", "formulacode")

    def test_get_base_image_name(self) -> None:
        assert get_base_image_name() == "formulacode/base:latest"

    def test_get_repo_image_name(self) -> None:
        assert get_repo_image_name("pandas-dev", "pandas") == "formulacode/pandas-dev-pandas:latest"

    def test_get_pr_image_name(self) -> None:
        assert get_pr_image_name("pandas-dev", "pandas", 42) == "formulacode/pandas-dev-pandas:42"


class TestBuildBaseImage:
    def test_build_base_image_returns_tag(self, manager: ImageManager) -> None:
        tag = manager.build_base_image(_CTX)
        assert tag == get_base_image_name()
        manager._mock_docker.build.assert_called_once_with(  # type: ignore[attr-defined]
            _CTX,
            tags=[get_base_image_name()],
            file=f"{_CTX}/Dockerfile.base",
            builder=_BUILDER,
        )

    def test_build_base_image_default_context(self, manager: ImageManager) -> None:
        tag = manager.build_base_image()
        assert tag == get_base_image_name()
        ctx = _default_context()
        manager._mock_docker.build.assert_called_once_with(  # type: ignore[attr-defined]
            ctx,
            tags=[get_base_image_name()],
            file=f"{ctx}/Dockerfile.base",
            builder=_BUILDER,
        )

    def test_build_base_image_with_py_version(self, manager: ImageManager) -> None:
        tag = manager.build_base_image(_CTX, py_version="3.11")
        assert tag == get_base_image_name()
        manager._mock_docker.build.assert_called_once_with(  # type: ignore[attr-defined]
            _CTX,
            tags=[get_base_image_name()],
            file=f"{_CTX}/Dockerfile.base",
            builder=_BUILDER,
            build_args={"PY_VERSION": "3.11"},
        )


class TestBuildRepoImage:
    def test_build_repo_image_from_base(self, manager: ImageManager) -> None:
        tag = manager.build_repo_image("pandas-dev", "pandas", _CTX)
        assert tag == get_repo_image_name("pandas-dev", "pandas")
        manager._mock_docker.build.assert_called_once_with(  # type: ignore[attr-defined]
            _CTX,
            tags=[get_repo_image_name("pandas-dev", "pandas")],
            file=f"{_CTX}/Dockerfile.repo",
            build_args={
                "BASE_IMAGE": get_base_image_name(),
                "REPO_URL": "https://github.com/pandas-dev/pandas.git",
                "BUILD_ROOT": ".",
            },
            builder=_BUILDER,
        )

    def test_build_repo_image_default_context(self, manager: ImageManager) -> None:
        tag = manager.build_repo_image("pandas-dev", "pandas")
        assert tag == get_repo_image_name("pandas-dev", "pandas")
        ctx = _default_context()
        manager._mock_docker.build.assert_called_once_with(  # type: ignore[attr-defined]
            ctx,
            tags=[get_repo_image_name("pandas-dev", "pandas")],
            file=f"{ctx}/Dockerfile.repo",
            build_args={
                "BASE_IMAGE": get_base_image_name(),
                "REPO_URL": "https://github.com/pandas-dev/pandas.git",
                "BUILD_ROOT": ".",
            },
            builder=_BUILDER,
        )

    def test_build_repo_image_custom_args(self, manager: ImageManager) -> None:
        tag = manager.build_repo_image(
            "numpy",
            "numpy",
            _CTX,
            repo_url="https://github.com/numpy/numpy.git",
            py_version="3.11",
        )
        assert tag == get_repo_image_name("numpy", "numpy", "3.11")
        call_kwargs = manager._mock_docker.build.call_args  # type: ignore[attr-defined]
        args = call_kwargs[1]["build_args"]
        assert args["REPO_URL"] == "https://github.com/numpy/numpy.git"
        assert args["PY_VERSION"] == "3.11"
        assert "COMMIT_SHA" not in args
        assert "ENV_PAYLOAD" not in args


class TestBuildPrImage:
    def test_build_pr_image_from_repo(self, manager: ImageManager) -> None:
        tag = manager.build_pr_image("pandas-dev", "pandas", 42, _CTX)
        assert tag == get_pr_image_name("pandas-dev", "pandas", 42)
        manager._mock_docker.build.assert_called_once_with(  # type: ignore[attr-defined]
            _CTX,
            tags=[get_pr_image_name("pandas-dev", "pandas", 42)],
            file=f"{_CTX}/Dockerfile.pr",
            build_args={
                "REPO_IMAGE": get_repo_image_name("pandas-dev", "pandas"),
                "COMMIT_SHA": "HEAD",
                "ENV_PAYLOAD": "[]",
            },
            builder=_BUILDER,
        )

    def test_build_pr_image_with_build_script(self, manager: ImageManager) -> None:
        tag = manager.build_pr_image("numpy", "numpy", 99, _CTX, build_script="custom_build.sh")
        assert tag == get_pr_image_name("numpy", "numpy", 99)
        call_kwargs = manager._mock_docker.build.call_args  # type: ignore[attr-defined]
        assert call_kwargs[1]["build_args"]["BUILD_SCRIPT"] == "custom_build.sh"

    def test_build_pr_image_default_context(self, manager: ImageManager) -> None:
        tag = manager.build_pr_image("pandas-dev", "pandas", 42)
        assert tag == get_pr_image_name("pandas-dev", "pandas", 42)
        ctx = _default_context()
        manager._mock_docker.build.assert_called_once_with(  # type: ignore[attr-defined]
            ctx,
            tags=[get_pr_image_name("pandas-dev", "pandas", 42)],
            file=f"{ctx}/Dockerfile.pr",
            build_args={
                "REPO_IMAGE": get_repo_image_name("pandas-dev", "pandas"),
                "COMMIT_SHA": "HEAD",
                "ENV_PAYLOAD": "[]",
            },
            builder=_BUILDER,
        )

    def test_build_pr_image_custom_commit_and_env(self, manager: ImageManager) -> None:
        tag = manager.build_pr_image(
            "numpy",
            "numpy",
            77,
            _CTX,
            commit_sha="abc123",
            env_payload='{"dependencies": ["cython"]}',
        )
        assert tag == get_pr_image_name("numpy", "numpy", 77)
        call_kwargs = manager._mock_docker.build.call_args  # type: ignore[attr-defined]
        args = call_kwargs[1]["build_args"]
        assert args["COMMIT_SHA"] == "abc123"
        assert args["ENV_PAYLOAD"] == '{"dependencies": ["cython"]}'


class TestImageExists:
    def test_image_exists_true(self, manager: ImageManager) -> None:
        manager._mock_docker.image.inspect.return_value = MagicMock()  # type: ignore[attr-defined]
        assert manager.image_exists(get_base_image_name()) is True

    def test_image_exists_false(self, manager: ImageManager) -> None:
        manager._mock_docker.image.inspect.side_effect = Exception("not found")  # type: ignore[attr-defined]
        assert manager.image_exists(get_base_image_name()) is False


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
        manager.remove_image(get_base_image_name())
        manager._mock_docker.image.remove.assert_called_once_with(  # type: ignore[attr-defined]
            get_base_image_name(), force=True
        )

    def test_remove_image_failure_does_not_raise(self, manager: ImageManager) -> None:
        manager._mock_docker.image.remove.side_effect = Exception("fail")  # type: ignore[attr-defined]
        # Should not raise
        manager.remove_image(get_base_image_name())

    def test_prune_dangling(self, manager: ImageManager) -> None:
        manager.prune_dangling()
        manager._mock_docker.image.prune.assert_called_once_with(all=False)  # type: ignore[attr-defined]
