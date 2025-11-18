"""Tests for resolution data models."""

from __future__ import annotations

from pathlib import Path

from datasmith.execution.resolution.models import (
    ASVCfgAggregate,
    Candidate,
    CandidateMeta,
)


class TestCandidate:
    """Tests for Candidate dataclass."""

    def test_candidate_creation(self) -> None:
        """Test basic Candidate creation."""
        candidate = Candidate(root_relpath="src/mypackage")

        assert candidate.root_relpath == "src/mypackage"
        assert candidate.pyproject_path is None
        assert candidate.setup_cfg_path is None
        assert candidate.setup_py_path is None
        assert candidate.req_files == []
        assert candidate.env_yamls == []

    def test_candidate_with_packaging_files(self, tmp_path: Path) -> None:
        """Test Candidate with packaging files specified."""
        pyproject = tmp_path / "pyproject.toml"
        setup_cfg = tmp_path / "setup.cfg"
        setup_py = tmp_path / "setup.py"

        candidate = Candidate(
            root_relpath=".",
            pyproject_path=pyproject,
            setup_cfg_path=setup_cfg,
            setup_py_path=setup_py,
        )

        assert candidate.pyproject_path == pyproject
        assert candidate.setup_cfg_path == setup_cfg
        assert candidate.setup_py_path == setup_py

    def test_candidate_with_requirement_files(self, tmp_path: Path) -> None:
        """Test Candidate with requirement files."""
        req1 = tmp_path / "requirements.txt"
        req2 = tmp_path / "requirements-dev.txt"

        candidate = Candidate(
            root_relpath=".",
            req_files=[req1, req2],
        )

        assert len(candidate.req_files) == 2
        assert req1 in candidate.req_files
        assert req2 in candidate.req_files

    def test_candidate_with_environment_yamls(self, tmp_path: Path) -> None:
        """Test Candidate with conda environment files."""
        env_yml = tmp_path / "environment.yml"
        env_yaml = tmp_path / "environment.yaml"

        candidate = Candidate(
            root_relpath=".",
            env_yamls=[env_yml, env_yaml],
        )

        assert len(candidate.env_yamls) == 2
        assert env_yml in candidate.env_yamls
        assert env_yaml in candidate.env_yamls


class TestCandidateMeta:
    """Tests for CandidateMeta dataclass."""

    def test_candidate_meta_creation(self) -> None:
        """Test basic CandidateMeta creation."""
        meta = CandidateMeta()

        assert meta.name is None
        assert meta.version is None
        assert meta.import_name is None
        assert meta.requires_python is None
        assert meta.core_deps == set()
        assert meta.extras == {}
        assert meta.build_requires == set()

    def test_candidate_meta_with_package_info(self) -> None:
        """Test CandidateMeta with package information."""
        meta = CandidateMeta(
            name="my-package",
            version="1.0.0",
            import_name="my_package",
            requires_python=">=3.8",
        )

        assert meta.name == "my-package"
        assert meta.version == "1.0.0"
        assert meta.import_name == "my_package"
        assert meta.requires_python == ">=3.8"

    def test_candidate_meta_with_dependencies(self) -> None:
        """Test CandidateMeta with dependencies."""
        meta = CandidateMeta(
            core_deps={"numpy", "pandas", "scipy"},
            build_requires={"setuptools", "wheel"},
        )

        assert meta.core_deps == {"numpy", "pandas", "scipy"}
        assert meta.build_requires == {"setuptools", "wheel"}

    def test_candidate_meta_with_extras(self) -> None:
        """Test CandidateMeta with extra dependencies."""
        meta = CandidateMeta(
            extras={
                "dev": {"pytest", "black", "mypy"},
                "docs": {"sphinx", "sphinx-rtd-theme"},
            }
        )

        assert "dev" in meta.extras
        assert "docs" in meta.extras
        assert meta.extras["dev"] == {"pytest", "black", "mypy"}
        assert meta.extras["docs"] == {"sphinx", "sphinx-rtd-theme"}

    def test_candidate_meta_mutable_defaults(self) -> None:
        """Test that default mutable collections are independent."""
        meta1 = CandidateMeta()
        meta2 = CandidateMeta()

        meta1.core_deps.add("numpy")
        meta1.extras["dev"] = {"pytest"}

        # meta2 should not be affected
        assert len(meta2.core_deps) == 0
        assert len(meta2.extras) == 0


class TestASVCfgAggregate:
    """Tests for ASVCfgAggregate dataclass."""

    def test_asv_cfg_aggregate_creation(self) -> None:
        """Test basic ASVCfgAggregate creation."""
        agg = ASVCfgAggregate()

        assert agg.pythons == set()
        assert agg.build_commands == set()
        assert agg.install_commands == set()
        assert agg.matrix == {}

    def test_asv_cfg_aggregate_with_pythons(self) -> None:
        """Test ASVCfgAggregate with Python versions."""
        agg = ASVCfgAggregate(pythons={(3, 8), (3, 9), (3, 10)})

        assert (3, 8) in agg.pythons
        assert (3, 9) in agg.pythons
        assert (3, 10) in agg.pythons
        assert len(agg.pythons) == 3

    def test_asv_cfg_aggregate_with_commands(self) -> None:
        """Test ASVCfgAggregate with build and install commands."""
        agg = ASVCfgAggregate(
            build_commands={"python setup.py build"},
            install_commands={"pip install -e ."},
        )

        assert "python setup.py build" in agg.build_commands
        assert "pip install -e ." in agg.install_commands

    def test_asv_cfg_aggregate_with_matrix(self) -> None:
        """Test ASVCfgAggregate with ASV matrix configuration."""
        agg = ASVCfgAggregate(
            matrix={
                "req": {"numpy", "scipy"},
                "extras": {"dev", "test"},
            }
        )

        assert "req" in agg.matrix
        assert "extras" in agg.matrix
        assert agg.matrix["req"] == {"numpy", "scipy"}
        assert agg.matrix["extras"] == {"dev", "test"}

    def test_asv_cfg_aggregate_mutable_defaults(self) -> None:
        """Test that default mutable collections are independent."""
        agg1 = ASVCfgAggregate()
        agg2 = ASVCfgAggregate()

        agg1.pythons.add((3, 9))
        agg1.build_commands.add("make build")
        agg1.matrix["foo"] = {"bar"}

        # agg2 should not be affected
        assert len(agg2.pythons) == 0
        assert len(agg2.build_commands) == 0
        assert len(agg2.matrix) == 0
