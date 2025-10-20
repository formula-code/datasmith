"""Tests for the self-healing blocklist module."""

import pytest

from datasmith.execution.resolution.blocklist import (
    add_to_blocklist,
    extract_failing_package,
    get_blocklist,
    remove_package_from_requirements,
    should_retry_without_package,
)


class TestExtractFailingPackage:
    """Test extraction of failing package names from error logs."""

    def test_extract_package_not_found(self):
        """Test extracting package from 'not found' error."""
        log = """
Using Python 3.10.13 environment at: /tmp/test/venv_3_10
  x No solution found when resolving dependencies:
  ╰─▶ Because shapefile was not found in the package registry and you require
      shapefile, we can conclude that your requirements are unsatisfiable.
"""
        assert extract_failing_package(log) == "shapefile"

    def test_extract_no_versions(self):
        """Test extracting package from 'no versions' error."""
        log = """
  x No solution found when resolving dependencies:
  ╰─▶ Because there are no versions of fake-package that satisfy the constraint
"""
        assert extract_failing_package(log) == "fake-package"

    def test_extract_version_conflict_unusual_name(self):
        """Test extracting unusual package names from version conflicts."""
        log = """
Because you require 3-0==1.0.0 and 3-0>=2.0.0, we can conclude that
your requirements are unsatisfiable.
"""
        assert extract_failing_package(log) == "3-0"

    def test_extract_no_package(self):
        """Test when no package can be extracted."""
        log = """
  x Failed to build package
  ├─▶ The build backend returned an error
"""
        assert extract_failing_package(log) is None

    def test_extract_empty_log(self):
        """Test with empty log."""
        assert extract_failing_package("") is None
        assert extract_failing_package(None) is None


class TestShouldRetryWithoutPackage:
    """Test determining when to retry."""

    def test_should_retry_not_found(self):
        """Test retry on 'not found' error."""
        log = "Because shapefile was not found in the package registry"
        assert should_retry_without_package(log) is True

    def test_should_retry_no_versions(self):
        """Test retry on 'no versions' error."""
        log = "Because there are no versions of fake-package"
        assert should_retry_without_package(log) is True

    def test_should_not_retry_build_failure(self):
        """Test no retry on build failure."""
        log = "x Failed to build package"
        assert should_retry_without_package(log) is False

    def test_should_not_retry_download_failure(self):
        """Test no retry on download failure."""
        log = "x Failed to download package"
        assert should_retry_without_package(log) is False

    def test_should_not_retry_empty(self):
        """Test no retry on empty log."""
        assert should_retry_without_package("") is False


class TestRemovePackageFromRequirements:
    """Test removing packages from requirement lists."""

    def test_remove_simple_package(self):
        """Test removing a simple package."""
        reqs = ["numpy>=1.0", "shapefile", "pandas"]
        filtered, was_removed = remove_package_from_requirements(reqs, "shapefile")
        assert filtered == ["numpy>=1.0", "pandas"]
        assert was_removed is True

    def test_remove_package_with_version(self):
        """Test removing package with version specifier."""
        reqs = ["numpy>=1.0", "shapefile==2.0", "pandas"]
        filtered, was_removed = remove_package_from_requirements(reqs, "shapefile")
        assert filtered == ["numpy>=1.0", "pandas"]
        assert was_removed is True

    def test_remove_package_with_extras(self):
        """Test removing package with extras."""
        reqs = ["numpy>=1.0", "shapefile[extra]>=1.0", "pandas"]
        filtered, was_removed = remove_package_from_requirements(reqs, "shapefile")
        assert filtered == ["numpy>=1.0", "pandas"]
        assert was_removed is True

    def test_remove_case_insensitive(self):
        """Test case-insensitive removal."""
        reqs = ["numpy>=1.0", "ShapeFile", "pandas"]
        filtered, was_removed = remove_package_from_requirements(reqs, "shapefile")
        assert filtered == ["numpy>=1.0", "pandas"]
        assert was_removed is True

    def test_remove_nonexistent_package(self):
        """Test removing a package that doesn't exist."""
        reqs = ["numpy>=1.0", "pandas"]
        filtered, was_removed = remove_package_from_requirements(reqs, "shapefile")
        assert filtered == ["numpy>=1.0", "pandas"]
        assert was_removed is False

    def test_remove_empty_package_name(self):
        """Test with empty package name."""
        reqs = ["numpy>=1.0", "pandas"]
        filtered, was_removed = remove_package_from_requirements(reqs, "")
        assert filtered == ["numpy>=1.0", "pandas"]
        assert was_removed is False


class TestBlocklistPersistence:
    """Test blocklist persistence and management."""

    @pytest.fixture
    def temp_blocklist(self, monkeypatch, tmp_path):
        """Use a temporary blocklist file for testing."""
        blocklist_path = tmp_path / "test_blocklist.json"
        monkeypatch.setattr("datasmith.execution.resolution.blocklist.BLOCKLIST_PATH", blocklist_path)
        # Reset cache
        monkeypatch.setattr("datasmith.execution.resolution.blocklist._blocklist_cache", None)
        return blocklist_path

    def test_add_to_blocklist(self, temp_blocklist):
        """Test adding packages to blocklist."""
        # First add should return True
        assert add_to_blocklist("shapefile") is True

        # Second add of same package should return False
        assert add_to_blocklist("shapefile") is False

        # Adding different package should return True
        assert add_to_blocklist("fake-package") is True

    def test_get_blocklist(self, temp_blocklist):
        """Test retrieving blocklist."""
        # Initially empty
        blocklist = get_blocklist()
        assert isinstance(blocklist, set)

        # Add packages
        add_to_blocklist("shapefile")
        add_to_blocklist("fake-package")

        # Retrieve and verify
        blocklist = get_blocklist()
        assert "shapefile" in blocklist
        assert "fake-package" in blocklist

    def test_blocklist_persistence(self, temp_blocklist, monkeypatch):
        """Test that blocklist persists across sessions."""
        # Add packages
        add_to_blocklist("shapefile")
        add_to_blocklist("fake-package")

        # Clear cache to simulate new session
        monkeypatch.setattr("datasmith.execution.resolution.blocklist._blocklist_cache", None)

        # Retrieve and verify persistence
        blocklist = get_blocklist()
        assert "shapefile" in blocklist
        assert "fake-package" in blocklist

    def test_add_empty_package(self, temp_blocklist):
        """Test adding empty package name."""
        assert add_to_blocklist("") is False
        assert add_to_blocklist("   ") is False

    def test_case_normalization(self, temp_blocklist):
        """Test that package names are normalized to lowercase."""
        add_to_blocklist("ShapeFile")
        blocklist = get_blocklist()
        assert "shapefile" in blocklist
        assert "ShapeFile" not in blocklist
