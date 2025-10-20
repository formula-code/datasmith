"""Tests for resolution constants."""

from __future__ import annotations

from pathlib import Path

from datasmith.execution.resolution.constants import (
    ALLOWLIST_COMMON_PYPI,
    ANSI_RE,
    ASV_REGEX,
    CONDA_SYSTEM_PACKAGES,
    ENV_YML_NAMES,
    GENERIC_LOCAL_NAMES,
    GIT_CACHE_DIR,
    NOT_REQUIREMENTS,
    PYPROJECT,
    REQ_TXT_REGEX,
    SETUP_CFG,
    SETUP_PY,
    SPECIAL_IMPORT_TO_PYPI,
    STDLIB,
)


class TestRegexConstants:
    """Tests for regex pattern constants."""

    def test_asv_regex_matches_asv_files(self) -> None:
        """Test ASV_REGEX matches ASV config files."""
        assert ASV_REGEX.search("asv.conf.json")
        assert ASV_REGEX.search(".asv.conf.json")
        assert ASV_REGEX.search("benchmarks/asv.conf.jsonc")
        assert ASV_REGEX.search("path/to/.asv-config.json")

    def test_asv_regex_does_not_match_non_asv(self) -> None:
        """Test ASV_REGEX does not match non-ASV files."""
        assert not ASV_REGEX.search("config.json")
        assert not ASV_REGEX.search("asv_benchmarks.py")
        assert not ASV_REGEX.search("test_asv.py")

    def test_req_txt_regex_matches_requirements(self) -> None:
        """Test REQ_TXT_REGEX matches requirements files."""
        assert REQ_TXT_REGEX.search("requirements.txt")
        assert REQ_TXT_REGEX.search("requirements.dev.txt")
        assert REQ_TXT_REGEX.search("requirements.test.txt")
        assert REQ_TXT_REGEX.search("constraints.txt")
        assert REQ_TXT_REGEX.search("constraints.prod.txt")
        assert REQ_TXT_REGEX.search("path/to/requirements.txt")

    def test_req_txt_regex_does_not_match_non_requirements(self) -> None:
        """Test REQ_TXT_REGEX does not match non-requirements files."""
        assert not REQ_TXT_REGEX.search("README.txt")
        assert not REQ_TXT_REGEX.search("requirements.md")
        assert not REQ_TXT_REGEX.search("requirements.py")

    def test_ansi_re_removes_color_codes(self) -> None:
        """Test ANSI_RE removes ANSI color codes."""
        colored = "\x1b[31mError\x1b[0m message"
        clean = ANSI_RE.sub("", colored)
        assert clean == "Error message"

    def test_ansi_re_handles_multiple_codes(self) -> None:
        """Test ANSI_RE handles multiple ANSI codes."""
        colored = "\x1b[1m\x1b[32mSuccess\x1b[0m\x1b[0m"
        clean = ANSI_RE.sub("", colored)
        assert clean == "Success"


class TestFileNameConstants:
    """Tests for file name constants."""

    def test_pyproject_constant(self) -> None:
        """Test PYPROJECT constant value."""
        assert PYPROJECT == "pyproject.toml"

    def test_setup_cfg_constant(self) -> None:
        """Test SETUP_CFG constant value."""
        assert SETUP_CFG == "setup.cfg"

    def test_setup_py_constant(self) -> None:
        """Test SETUP_PY constant value."""
        assert SETUP_PY == "setup.py"

    def test_env_yml_names_constant(self) -> None:
        """Test ENV_YML_NAMES constant."""
        assert "environment.yml" in ENV_YML_NAMES
        assert "environment.yaml" in ENV_YML_NAMES
        assert len(ENV_YML_NAMES) == 2


class TestGitCacheDir:
    """Tests for git cache directory constant."""

    def test_git_cache_dir_is_path(self) -> None:
        """Test GIT_CACHE_DIR is a Path object."""
        assert isinstance(GIT_CACHE_DIR, Path)

    def test_git_cache_dir_exists(self) -> None:
        """Test GIT_CACHE_DIR exists (created on import)."""
        assert GIT_CACHE_DIR.exists()
        assert GIT_CACHE_DIR.is_dir()


class TestImportToPackageMappings:
    """Tests for import name to PyPI package mappings."""

    def test_special_import_to_pypi_sklearn(self) -> None:
        """Test sklearn import name mapping."""
        assert SPECIAL_IMPORT_TO_PYPI["sklearn"] == "scikit-learn"

    def test_special_import_to_pypi_pil(self) -> None:
        """Test PIL import name mapping."""
        assert SPECIAL_IMPORT_TO_PYPI["PIL"] == "Pillow"

    def test_special_import_to_pypi_cv2(self) -> None:
        """Test cv2 import name mapping."""
        assert SPECIAL_IMPORT_TO_PYPI["cv2"] == "opencv-python"

    def test_special_import_to_pypi_yaml(self) -> None:
        """Test yaml import name mapping."""
        assert SPECIAL_IMPORT_TO_PYPI["yaml"] == "PyYAML"

    def test_special_import_to_pypi_bs4(self) -> None:
        """Test bs4 import name mapping."""
        assert SPECIAL_IMPORT_TO_PYPI["bs4"] == "beautifulsoup4"


class TestCondaSystemPackages:
    """Tests for conda/system package set."""

    def test_conda_system_packages_has_compilers(self) -> None:
        """Test CONDA_SYSTEM_PACKAGES includes compilers."""
        assert "gcc" in CONDA_SYSTEM_PACKAGES
        assert "clang" in CONDA_SYSTEM_PACKAGES
        assert "gfortran" in CONDA_SYSTEM_PACKAGES

    def test_conda_system_packages_has_build_tools(self) -> None:
        """Test CONDA_SYSTEM_PACKAGES includes build tools."""
        assert "cmake" in CONDA_SYSTEM_PACKAGES
        assert "make" in CONDA_SYSTEM_PACKAGES
        assert "autoconf" in CONDA_SYSTEM_PACKAGES


class TestNotRequirements:
    """Tests for NOT_REQUIREMENTS set."""

    def test_not_requirements_has_stdlib(self) -> None:
        """Test NOT_REQUIREMENTS includes stdlib modules."""
        assert "sqlite3" in NOT_REQUIREMENTS
        assert "tkinter" in NOT_REQUIREMENTS
        assert "distutils" in NOT_REQUIREMENTS

    def test_not_requirements_has_pkg_resources(self) -> None:
        """Test NOT_REQUIREMENTS includes pkg_resources."""
        assert "pkg_resources" in NOT_REQUIREMENTS

    def test_not_requirements_has_platform_specific(self) -> None:
        """Test NOT_REQUIREMENTS includes platform-specific modules."""
        assert "AppKit" in NOT_REQUIREMENTS
        assert "Foundation" in NOT_REQUIREMENTS


class TestAllowlistCommonPypi:
    """Tests for ALLOWLIST_COMMON_PYPI set."""

    def test_allowlist_has_numpy(self) -> None:
        """Test ALLOWLIST_COMMON_PYPI includes numpy."""
        assert "numpy" in ALLOWLIST_COMMON_PYPI

    def test_allowlist_has_scientific_packages(self) -> None:
        """Test ALLOWLIST_COMMON_PYPI includes scientific packages."""
        assert "scipy" in ALLOWLIST_COMMON_PYPI
        assert "pandas" in ALLOWLIST_COMMON_PYPI
        assert "matplotlib" in ALLOWLIST_COMMON_PYPI
        assert "scikit-learn" in ALLOWLIST_COMMON_PYPI

    def test_allowlist_has_dev_tools(self) -> None:
        """Test ALLOWLIST_COMMON_PYPI includes dev tools."""
        assert "pytest" in ALLOWLIST_COMMON_PYPI
        assert "black" in ALLOWLIST_COMMON_PYPI
        assert "sphinx" in ALLOWLIST_COMMON_PYPI


class TestGenericLocalNames:
    """Tests for GENERIC_LOCAL_NAMES set."""

    def test_generic_local_names_has_common_modules(self) -> None:
        """Test GENERIC_LOCAL_NAMES includes common local module names."""
        assert "utils" in GENERIC_LOCAL_NAMES
        assert "core" in GENERIC_LOCAL_NAMES
        assert "helpers" in GENERIC_LOCAL_NAMES
        assert "config" in GENERIC_LOCAL_NAMES

    def test_generic_local_names_has_test_related(self) -> None:
        """Test GENERIC_LOCAL_NAMES includes test-related names."""
        assert "tests" in GENERIC_LOCAL_NAMES
        assert "test" in GENERIC_LOCAL_NAMES
        assert "testing" in GENERIC_LOCAL_NAMES


class TestStdlib:
    """Tests for STDLIB set."""

    def test_stdlib_is_set(self) -> None:
        """Test STDLIB is a set."""
        assert isinstance(STDLIB, set)

    def test_stdlib_has_common_modules(self) -> None:
        """Test STDLIB includes common standard library modules."""
        # Only test if sys.stdlib_module_names is available (Python 3.10+)
        if STDLIB:
            assert "os" in STDLIB or "os" in {m.lower() for m in STDLIB}
            assert "sys" in STDLIB or "sys" in {m.lower() for m in STDLIB}
