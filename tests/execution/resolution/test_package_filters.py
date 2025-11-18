"""Tests for package filtering utilities."""

from __future__ import annotations

from pathlib import Path

from datasmith.execution.resolution.package_filters import (
    clean_pinned,
    extract_pkg_name,
    extract_requested_extras,
    extras_from_install_commands,
    extras_from_matrix,
    filter_pypi_packages,
    filter_requirements_for_pypi,
    is_valid_direct_url,
    is_valid_pypi_requirement,
    normalize_requirement,
    parse_extras_segment,
    split_shell_command,
)


class TestParseExtrasSegment:
    """Tests for parse_extras_segment function."""

    def test_parse_extras_single(self) -> None:
        """Test parsing single extra."""
        assert parse_extras_segment("package[dev]") == ["dev"]

    def test_parse_extras_multiple(self) -> None:
        """Test parsing multiple extras."""
        assert parse_extras_segment("package[dev,test,docs]") == ["dev", "test", "docs"]

    def test_parse_extras_with_spaces(self) -> None:
        """Test parsing extras with spaces."""
        assert parse_extras_segment("package[dev, test, docs]") == ["dev", "test", "docs"]

    def test_parse_extras_no_brackets(self) -> None:
        """Test parsing without brackets returns empty."""
        assert parse_extras_segment("package") == []

    def test_parse_extras_empty_brackets(self) -> None:
        """Test parsing empty brackets."""
        assert parse_extras_segment("package[]") == []


class TestExtrasFromInstallCommands:
    """Tests for extras_from_install_commands function."""

    def test_extras_from_install_commands_pip(self) -> None:
        """Test extracting extras from pip install commands."""
        cmds = ["pip install .[dev,test]", "pip install package[docs]"]
        available = {"dev", "test", "docs", "extra"}
        result = extras_from_install_commands(cmds, available)

        assert "dev" in result
        assert "test" in result
        assert "docs" in result
        assert "extra" not in result

    def test_extras_from_install_commands_filters_unavailable(self) -> None:
        """Test that unavailable extras are filtered out."""
        cmds = ["pip install .[dev,unknown]"]
        available = {"dev", "test"}
        result = extras_from_install_commands(cmds, available)

        assert result == {"dev"}


class TestExtrasFromMatrix:
    """Tests for extras_from_matrix function."""

    def test_extras_from_matrix_basic(self) -> None:
        """Test extracting extras from matrix."""
        matrix = {"extras": {"dev", "test"}}
        available = {"dev", "test", "docs"}
        result = extras_from_matrix(matrix, available)

        assert "dev" in result
        assert "test" in result

    def test_extras_from_matrix_none(self) -> None:
        """Test handling None matrix."""
        result = extras_from_matrix(None, {"dev"})
        assert result == set()

    def test_extras_from_matrix_empty(self) -> None:
        """Test handling empty matrix."""
        result = extras_from_matrix({}, {"dev"})
        assert result == set()


class TestExtractRequestedExtras:
    """Tests for extract_requested_extras function."""

    def test_extract_requested_extras_combined(self) -> None:
        """Test extracting extras from both commands and matrix."""
        cmds = ["pip install .[dev]"]
        matrix = {"extras": {"test"}}
        available = ["dev", "test", "docs"]

        result = extract_requested_extras(cmds, matrix, available)

        assert "dev" in result
        assert "test" in result
        assert "docs" not in result


class TestSplitShellCommand:
    """Tests for split_shell_command function."""

    def test_split_shell_command_and(self) -> None:
        """Test splitting on && operator."""
        cmd = "cmd1 && cmd2 && cmd3"
        assert split_shell_command(cmd) == ["cmd1", "cmd2", "cmd3"]

    def test_split_shell_command_or(self) -> None:
        """Test splitting on || operator."""
        cmd = "cmd1 || cmd2"
        assert split_shell_command(cmd) == ["cmd1", "cmd2"]

    def test_split_shell_command_semicolon(self) -> None:
        """Test splitting on ; operator."""
        cmd = "cmd1; cmd2; cmd3"
        assert split_shell_command(cmd) == ["cmd1", "cmd2", "cmd3"]

    def test_split_shell_command_mixed(self) -> None:
        """Test splitting with mixed operators."""
        cmd = "cmd1 && cmd2 || cmd3 ; cmd4"
        result = split_shell_command(cmd)
        assert len(result) == 4


class TestIsValidDirectUrl:
    """Tests for is_valid_direct_url function."""

    def test_is_valid_direct_url_whl(self) -> None:
        """Test valid URL with .whl extension."""
        assert is_valid_direct_url("https://example.com/package-1.0-py3-none-any.whl")

    def test_is_valid_direct_url_tar_gz(self) -> None:
        """Test valid URL with .tar.gz extension."""
        assert is_valid_direct_url("https://example.com/package-1.0.tar.gz")

    def test_is_valid_direct_url_git(self) -> None:
        """Test git+ URL with supported extension."""
        assert is_valid_direct_url("git+https://github.com/user/repo.git#egg=package&subdirectory=package-1.0.tar.gz")

    def test_is_valid_direct_url_no_extension(self) -> None:
        """Test URL without supported extension."""
        assert not is_valid_direct_url("https://example.com/package")

    def test_is_valid_direct_url_not_url(self) -> None:
        """Test non-URL string."""
        assert not is_valid_direct_url("package>=1.0")


class TestFilterPypiPackages:
    """Tests for filter_pypi_packages function."""

    def test_filter_pypi_packages_removes_python(self) -> None:
        """Test filtering out python version specifiers."""
        reqs = ["numpy>=1.0", "python>=3.8", "pandas"]
        result = filter_pypi_packages(reqs)

        assert "numpy>=1.0" in result
        assert "pandas" in result

    def test_filter_pypi_packages_removes_conda(self) -> None:
        """Test filtering out conda-only packages."""
        reqs = ["numpy", "gcc", "cmake", "pandas"]
        result = filter_pypi_packages(reqs)

        assert "numpy" in result
        assert "pandas" in result
        assert "gcc" not in result
        assert "cmake" not in result


class TestIsValidPypiRequirement:
    """Tests for is_valid_pypi_requirement function."""

    def test_is_valid_pypi_requirement_simple(self) -> None:
        """Test valid simple requirement."""
        assert is_valid_pypi_requirement("numpy")
        assert is_valid_pypi_requirement("scikit-learn")
        assert is_valid_pypi_requirement("numpy>=1.0")

    def test_is_valid_pypi_requirement_with_extras(self) -> None:
        """Test valid requirement with extras."""
        assert is_valid_pypi_requirement("package[dev]")
        assert is_valid_pypi_requirement("package[dev,test]>=1.0")

    def test_is_valid_pypi_requirement_url(self) -> None:
        """Test valid URL requirement."""
        assert is_valid_pypi_requirement("git+https://github.com/user/repo.git")
        assert is_valid_pypi_requirement("https://example.com/package.tar.gz")

    def test_is_valid_pypi_requirement_invalid_template(self) -> None:
        """Test rejection of template variables."""
        assert not is_valid_pypi_requirement("package==${VERSION}")
        assert not is_valid_pypi_requirement("package>=$PYTHON_VERSION")

    def test_is_valid_pypi_requirement_invalid_shell(self) -> None:
        """Test rejection of shell operators."""
        assert not is_valid_pypi_requirement("package && otherpackage")
        assert not is_valid_pypi_requirement("package || backup")

    def test_is_valid_pypi_requirement_invalid_option(self) -> None:
        """Test rejection of pip options."""
        assert not is_valid_pypi_requirement("--upgrade")
        assert not is_valid_pypi_requirement("--no-deps")

    def test_is_valid_pypi_requirement_invalid_single_char(self) -> None:
        """Test rejection of single-character names."""
        assert not is_valid_pypi_requirement("a")
        assert not is_valid_pypi_requirement("x>=1.0")


class TestNormalizeRequirement:
    """Tests for normalize_requirement function."""

    def test_normalize_requirement_valid_package(self) -> None:
        """Test normalizing valid package."""
        assert normalize_requirement("numpy>=1.0") == ["numpy>=1.0"]

    def test_normalize_requirement_url(self) -> None:
        """Test normalizing URL requirement."""
        url = "https://example.com/package.tar.gz"
        assert normalize_requirement(url) == [url]

    def test_normalize_requirement_template(self) -> None:
        """Test rejecting template variables."""
        assert normalize_requirement("package==${VAR}") == []

    def test_normalize_requirement_empty(self) -> None:
        """Test handling empty string."""
        assert normalize_requirement("") == []
        assert normalize_requirement("   ") == []


class TestExtractPkgName:
    """Tests for extract_pkg_name function."""

    def test_extract_pkg_name_simple(self) -> None:
        """Test extracting simple package name."""
        assert extract_pkg_name("numpy") == "numpy"

    def test_extract_pkg_name_with_version(self) -> None:
        """Test extracting name with version specifier."""
        assert extract_pkg_name("numpy>=1.0") == "numpy"
        assert extract_pkg_name("pandas==2.0.0") == "pandas"

    def test_extract_pkg_name_with_extras(self) -> None:
        """Test extracting name with extras."""
        assert extract_pkg_name("package[dev]") == "package"
        assert extract_pkg_name("package[dev,test]>=1.0") == "package"

    def test_extract_pkg_name_with_marker(self) -> None:
        """Test extracting name with environment marker."""
        assert extract_pkg_name("package>=1.0; python_version<'3.11'") == "package"


class TestCleanPinned:
    """Tests for clean_pinned function."""

    def test_clean_pinned_removes_lower_bound(self) -> None:
        """Test removing lower bounds from pinned requirements."""
        reqs = ["torch>=1.8,<=1.9"]
        result = clean_pinned(reqs)

        assert "torch<=1.9" in result

    def test_clean_pinned_preserves_simple(self) -> None:
        """Test preserving simple requirements."""
        reqs = ["numpy>=1.0", "pandas<=2.0"]
        result = clean_pinned(reqs)

        assert "numpy>=1.0" in result
        assert "pandas<=2.0" in result


class TestFilterRequirementsForPypi:
    """Tests for filter_requirements_for_pypi function."""

    def test_filter_requirements_for_pypi_removes_stdlib(self, tmp_path: Path) -> None:
        """Test filtering out stdlib modules."""
        reqs = ["numpy", "os", "sys", "pandas"]
        result = filter_requirements_for_pypi(reqs, project_dir=tmp_path, own_import_name=None)

        assert "numpy" in result
        assert "pandas" in result
        assert "os" not in result
        assert "sys" not in result

    def test_filter_requirements_for_pypi_removes_own_name(self, tmp_path: Path) -> None:
        """Test filtering out project's own name."""
        reqs = ["numpy", "mypackage", "pandas"]
        result = filter_requirements_for_pypi(reqs, project_dir=tmp_path, own_import_name="mypackage")

        assert "numpy" in result
        assert "pandas" in result
        assert "mypackage" not in result

    def test_filter_requirements_for_pypi_removes_generic_local(self, tmp_path: Path) -> None:
        """Test filtering out generic local module names."""
        reqs = ["numpy", "utils", "core", "pandas"]
        result = filter_requirements_for_pypi(reqs, project_dir=tmp_path, own_import_name=None)

        assert "numpy" in result
        assert "pandas" in result
        # utils and core should be filtered as generic local names
        assert "utils" not in result
        assert "core" not in result

    def test_filter_requirements_for_pypi_keeps_urls(self, tmp_path: Path) -> None:
        """Test keeping valid direct URLs."""
        reqs = ["https://example.com/package.tar.gz", "numpy"]
        result = filter_requirements_for_pypi(reqs, project_dir=tmp_path, own_import_name=None)

        assert "https://example.com/package.tar.gz" in result
        assert "numpy" in result

    def test_filter_requirements_for_pypi_removes_python(self, tmp_path: Path) -> None:
        """Test filtering out python references."""
        reqs = ["numpy", "python", "python3", "python3.10", "pandas"]
        result = filter_requirements_for_pypi(reqs, project_dir=tmp_path, own_import_name=None)

        assert "numpy" in result
        assert "pandas" in result
        assert all("python" not in r.lower() for r in result)
