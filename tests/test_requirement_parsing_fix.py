"""Tests for requirement string parsing fixes."""

from datasmith.execution.resolution.package_filters import fix_marker_spacing


class TestFixMarkerSpacing:
    """Test cases for fix_marker_spacing function."""

    def test_fix_and_operator_spacing(self):
        """Test fixing missing spaces around 'and' operator."""
        input_req = 'pydap;python_version<"3.10"andextra=="docs"'
        expected = 'pydap;python_version<"3.10" and extra=="docs"'
        assert fix_marker_spacing(input_req) == expected

    def test_fix_or_operator_spacing(self):
        """Test fixing missing spaces around 'or' operator."""
        input_req = 'package;extra=="dev"orextra=="test"'
        expected = 'package;extra=="dev" or extra=="test"'
        assert fix_marker_spacing(input_req) == expected

    def test_fix_multiple_operators(self):
        """Test fixing multiple operators in complex markers."""
        input_req = 'package;(python_version<"3.10"andextra=="docs")or(python_version>="3.10")'
        expected = 'package;(python_version<"3.10" and extra=="docs") or (python_version>="3.10")'
        assert fix_marker_spacing(input_req) == expected

    def test_strip_inline_comments(self):
        """Test stripping inline comments without proper spacing."""
        input_req = "numpy>=1.21#recommendedtouse>=1.22forfullquantilemethodsupport"
        expected = "numpy>=1.21"
        assert fix_marker_spacing(input_req) == expected

    def test_preserve_proper_comments(self):
        """Test that properly spaced comments are preserved."""
        input_req = "package>=1.0 # proper comment"
        expected = "package>=1.0 # proper comment"
        assert fix_marker_spacing(input_req) == expected

    def test_no_changes_needed(self):
        """Test requirements that don't need fixing."""
        test_cases = [
            "package>=1.0",
            "package[extra]>=1.0",
            'package;python_version>="3.8"',
            'package; python_version >= "3.8" and extra == "dev"',
        ]
        for req in test_cases:
            assert fix_marker_spacing(req) == req

    def test_requirements_without_markers(self):
        """Test that requirements without markers pass through unchanged."""
        test_cases = [
            "numpy>=1.21",
            "pandas",
            "scikit-learn>=1.0.0",
            "package[extra1,extra2]>=2.0",
        ]
        for req in test_cases:
            assert fix_marker_spacing(req) == req

    def test_url_requirements(self):
        """Test that URL-based requirements are handled correctly."""
        test_cases = [
            "git+https://github.com/user/repo.git",
            "https://example.com/package.whl",
            "file:///path/to/package.tar.gz",
        ]
        for req in test_cases:
            # Should not break URL requirements
            result = fix_marker_spacing(req)
            assert result == req

    def test_edge_cases(self):
        """Test edge cases and boundary conditions."""
        # Empty string
        assert fix_marker_spacing("") == ""

        # Only whitespace
        assert fix_marker_spacing("   ") == "   "

        # Just a semicolon
        assert fix_marker_spacing(";") == ";"

        # Marker without package
        assert fix_marker_spacing(';python_version<"3.10"') == ';python_version<"3.10"'
