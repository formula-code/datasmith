"""Tests for the top-level datasmith package lazy-loading API."""

from __future__ import annotations

import importlib
import sys
import types

import pytest


@pytest.fixture()
def _fresh_datasmith():
    """Remove datasmith and heavy optional deps from sys.modules."""
    heavy = ("dspy", "python_on_whales")
    to_remove = [
        k
        for k in sys.modules
        if k == "datasmith" or k.startswith("datasmith.") or any(k == h or k.startswith(h + ".") for h in heavy)
    ]
    saved = {k: sys.modules.pop(k) for k in to_remove}
    yield
    # Restore everything
    sys.modules.update(saved)
    importlib.import_module("datasmith")


class TestSubmoduleAccess:
    """Accessing ds.<submodule> returns the correct module."""

    @pytest.mark.parametrize(
        "name",
        ["github", "agents", "docker", "runners", "utils", "update", "publish"],
    )
    def test_submodule_is_module(self, name: str) -> None:
        import datasmith as ds

        mod = getattr(ds, name)
        assert isinstance(mod, types.ModuleType)
        assert mod.__name__ == f"datasmith.{name}"

    def test_filters_is_module(self) -> None:
        import datasmith as ds

        mod = ds.filters
        assert isinstance(mod, types.ModuleType)

    def test_preflight_is_module(self) -> None:
        import datasmith as ds

        mod = ds.preflight
        assert isinstance(mod, types.ModuleType)


class TestTypeIdentity:
    """Lazy top-level attrs are the same objects as subpackage attrs."""

    def test_pr_identity(self) -> None:
        import datasmith as ds

        assert ds.PR is ds.github.PR

    def test_docker_context_identity(self) -> None:
        import datasmith as ds

        assert ds.DockerContext is ds.docker.DockerContext

    def test_synthesizer_identity(self) -> None:
        import datasmith as ds

        assert ds.Synthesizer is ds.agents.Synthesizer

    def test_base_runner_identity(self) -> None:
        import datasmith as ds

        assert ds.BaseRunner is ds.runners.BaseRunner

    def test_pipeline_identity(self) -> None:
        import datasmith as ds

        assert ds.Pipeline is ds.update.Pipeline

    def test_settings_identity(self) -> None:
        import datasmith as ds

        assert ds.Settings is ds.utils.Settings

    def test_github_client_identity(self) -> None:
        import datasmith as ds

        assert ds.GitHubClient is ds.github.GitHubClient

    def test_huggingface_publisher_identity(self) -> None:
        import datasmith as ds

        assert ds.HuggingFacePublisher is ds.publish.HuggingFacePublisher


class TestAllCompleteness:
    """__all__ and dir() contain every submodule and every lazy import."""

    def test_all_contains_submodules(self) -> None:
        import datasmith as ds

        for name in ds._SUBMODULES:
            assert name in ds.__all__, f"{name!r} missing from __all__"

    def test_all_contains_lazy_imports(self) -> None:
        import datasmith as ds

        for name in ds._LAZY_IMPORTS:
            assert name in ds.__all__, f"{name!r} missing from __all__"

    def test_dir_matches_all(self) -> None:
        import datasmith as ds

        d = dir(ds)
        for name in ds.__all__:
            assert name in d, f"{name!r} in __all__ but not in dir()"


class TestAttributeError:
    """Accessing a nonexistent name raises AttributeError."""

    def test_nonexistent_raises(self) -> None:
        import datasmith as ds

        with pytest.raises(AttributeError, match="nonexistent"):
            _ = ds.nonexistent  # type: ignore[attr-defined]


class TestNoEagerHeavyImports:
    """A bare 'import datasmith' must not pull in heavy optional deps."""

    @pytest.mark.usefixtures("_fresh_datasmith")
    def test_no_dspy_on_import(self) -> None:
        assert "dspy" not in sys.modules

    @pytest.mark.usefixtures("_fresh_datasmith")
    def test_no_python_on_whales_on_import(self) -> None:
        assert "python_on_whales" not in sys.modules


class TestVersionAndSetup:
    """Existing public API still works."""

    def test_version(self) -> None:
        import datasmith as ds

        assert ds.__version__ == "0.1.0"

    def test_setup_environment_callable(self) -> None:
        import datasmith as ds

        # Should not raise
        ds.setup_environment()
