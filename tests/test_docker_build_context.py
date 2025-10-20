"""Tests for Docker build context utilities.

This module tests the build context creation and .dockerignore handling.
"""

from __future__ import annotations

import io
import tarfile
from pathlib import Path

from datasmith.docker.build_context import (
    dir_context_tar_bytes,
    path_matches_any,
    read_dockerignore,
)


class TestReadDockerignore:
    """Tests for .dockerignore parsing."""

    def test_read_dockerignore_basic(self, tmp_path: Path) -> None:
        """Test basic .dockerignore parsing."""
        dockerignore = tmp_path / ".dockerignore"
        dockerignore.write_text("node_modules\n*.pyc\n__pycache__")

        ignores, negates = read_dockerignore(tmp_path)

        assert "node_modules" in ignores
        assert "*.pyc" in ignores
        assert "__pycache__" in ignores
        assert ".git" in ignores  # Auto-added
        assert len(negates) == 0

    def test_read_dockerignore_with_negates(self, tmp_path: Path) -> None:
        """Test .dockerignore parsing with negation patterns."""
        dockerignore = tmp_path / ".dockerignore"
        dockerignore.write_text("*.log\n!important.log\n# Comment\n\n")

        ignores, negates = read_dockerignore(tmp_path)

        assert "*.log" in ignores
        assert "important.log" in negates
        assert ".git" in ignores

    def test_read_dockerignore_comments_and_empty_lines(self, tmp_path: Path) -> None:
        """Test that comments and empty lines are ignored."""
        dockerignore = tmp_path / ".dockerignore"
        dockerignore.write_text("# This is a comment\n\n*.tmp\n  # Another comment  \n\nbuild/")

        ignores, negates = read_dockerignore(tmp_path)

        assert "*.tmp" in ignores
        assert "build/" in ignores
        assert len(ignores) == 3  # *.tmp, build/, .git

    def test_read_dockerignore_missing_file(self, tmp_path: Path) -> None:
        """Test handling of missing .dockerignore file."""
        ignores, negates = read_dockerignore(tmp_path)

        # When .dockerignore doesn't exist, returns empty lists
        assert len(ignores) == 0
        assert len(negates) == 0


class TestPathMatchesAny:
    """Tests for glob pattern matching."""

    def test_path_matches_any_glob_patterns(self) -> None:
        """Test glob pattern matching with *, ?, **."""
        patterns = ["*.pyc", "test_*.py", "**/__pycache__"]

        assert path_matches_any("file.pyc", patterns)
        assert path_matches_any("test_foo.py", patterns)
        assert path_matches_any("dir/__pycache__", patterns)
        assert not path_matches_any("file.py", patterns)

    def test_path_matches_any_directory_prefix(self) -> None:
        """Test directory prefix matching with trailing slash."""
        patterns = ["node_modules/", "build/"]

        assert path_matches_any("node_modules", patterns)
        assert path_matches_any("node_modules/package.json", patterns)
        assert path_matches_any("build", patterns)
        assert path_matches_any("build/output.txt", patterns)
        assert not path_matches_any("src/build.py", patterns)

    def test_path_matches_any_exact_match(self) -> None:
        """Test exact pattern matching."""
        patterns = [".git", "Dockerfile"]

        assert path_matches_any(".git", patterns)
        assert path_matches_any("Dockerfile", patterns)
        assert not path_matches_any(".github", patterns)

    def test_path_matches_any_with_leading_slash(self) -> None:
        """Test that patterns match with or without leading slash."""
        patterns = ["*.pyc", "/root.txt"]

        assert path_matches_any("file.pyc", patterns)
        assert path_matches_any("root.txt", patterns)


class TestDirContextTarBytes:
    """Tests for tar archive creation."""

    def test_dir_context_tar_bytes_creates_tar(self, tmp_path: Path) -> None:
        """Test that a valid tar archive is created."""
        # Create test files
        (tmp_path / "file1.txt").write_text("content1")
        (tmp_path / "file2.py").write_text("content2")
        (tmp_path / "Dockerfile").write_text("FROM ubuntu")

        tar_bytes = dir_context_tar_bytes(str(tmp_path))

        # Verify it's a valid tar
        assert len(tar_bytes) > 0
        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r") as tar:
            names = tar.getnames()
            assert "file1.txt" in names
            assert "file2.py" in names
            assert "Dockerfile" in names

    def test_dir_context_tar_bytes_deterministic(self, tmp_path: Path) -> None:
        """Test that same input produces same tar archive (deterministic)."""
        (tmp_path / "file1.txt").write_text("content")
        (tmp_path / "Dockerfile").write_text("FROM ubuntu")

        tar_bytes1 = dir_context_tar_bytes(str(tmp_path))
        tar_bytes2 = dir_context_tar_bytes(str(tmp_path))

        assert tar_bytes1 == tar_bytes2

    def test_dir_context_tar_bytes_respects_dockerignore(self, tmp_path: Path) -> None:
        """Test that .dockerignore patterns are respected."""
        (tmp_path / ".dockerignore").write_text("*.pyc\n__pycache__/")
        (tmp_path / "file.py").write_text("code")
        (tmp_path / "file.pyc").write_text("bytecode")
        (tmp_path / "__pycache__").mkdir()
        (tmp_path / "__pycache__" / "cached.pyc").write_text("cached")
        (tmp_path / "Dockerfile").write_text("FROM ubuntu")

        tar_bytes = dir_context_tar_bytes(str(tmp_path))

        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r") as tar:
            names = tar.getnames()
            assert "file.py" in names
            assert "file.pyc" not in names
            assert "__pycache__" not in names
            assert not any("cached.pyc" in name for name in names)

    def test_dir_context_tar_bytes_preserves_symlinks(self, tmp_path: Path) -> None:
        """Test that symlinks are preserved in the tar."""
        (tmp_path / "real_file.txt").write_text("content")
        (tmp_path / "link_file.txt").symlink_to("real_file.txt")
        (tmp_path / "Dockerfile").write_text("FROM ubuntu")

        tar_bytes = dir_context_tar_bytes(str(tmp_path))

        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r") as tar:
            link_info = tar.getmember("link_file.txt")
            assert link_info.issym()
            assert link_info.linkname == "real_file.txt"

    def test_dir_context_tar_bytes_normalized_metadata(self, tmp_path: Path) -> None:
        """Test that metadata is normalized (mtime=0, uid/gid=0)."""
        (tmp_path / "file.txt").write_text("content")
        (tmp_path / "Dockerfile").write_text("FROM ubuntu")

        tar_bytes = dir_context_tar_bytes(str(tmp_path))

        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r") as tar:
            for member in tar.getmembers():
                assert member.mtime == 0
                assert member.uid == 0
                assert member.gid == 0
                assert member.uname == ""
                assert member.gname == ""

    def test_dir_context_tar_bytes_dockerfile_aliasing(self, tmp_path: Path) -> None:
        """Test that custom Dockerfile names are copied to 'Dockerfile'."""
        (tmp_path / "Dockerfile.custom").write_text("FROM alpine")
        (tmp_path / "file.txt").write_text("content")

        tar_bytes = dir_context_tar_bytes(str(tmp_path), dockerfile_name="Dockerfile.custom")

        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r") as tar:
            names = tar.getnames()
            assert "Dockerfile" in names
            # Verify content
            dockerfile_content = tar.extractfile("Dockerfile").read().decode()  # type: ignore[union-attr]
            assert dockerfile_content == "FROM alpine"

    def test_dir_context_tar_bytes_sorted_order(self, tmp_path: Path) -> None:
        """Test that files are sorted for deterministic ordering."""
        (tmp_path / "zebra.txt").write_text("z")
        (tmp_path / "apple.txt").write_text("a")
        (tmp_path / "banana.txt").write_text("b")
        (tmp_path / "Dockerfile").write_text("FROM ubuntu")

        tar_bytes = dir_context_tar_bytes(str(tmp_path))

        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r") as tar:
            names = [m.name for m in tar.getmembers() if m.isfile()]
            # Files should be in sorted order
            txt_files = [n for n in names if n.endswith(".txt")]
            assert txt_files == sorted(txt_files)

    def test_dir_context_tar_bytes_with_explicit_git_ignore(self, tmp_path: Path) -> None:
        """Test that .git directory can be excluded with proper pattern."""
        # .git/ with trailing slash matches directory and its contents
        (tmp_path / ".dockerignore").write_text(".git/")
        (tmp_path / ".git").mkdir()
        (tmp_path / ".git" / "config").write_text("git config")
        (tmp_path / "file.txt").write_text("content")
        (tmp_path / "Dockerfile").write_text("FROM ubuntu")

        tar_bytes = dir_context_tar_bytes(str(tmp_path))

        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r") as tar:
            names = tar.getnames()
            assert "file.txt" in names
            # .git directory and its contents should be excluded
            git_names = [n for n in names if n.startswith(".git") and n != ".dockerignore"]
            assert len(git_names) == 0, f"Found .git files in tar: {git_names}"

    def test_dir_context_tar_bytes_with_subdirectories(self, tmp_path: Path) -> None:
        """Test that subdirectories are included correctly."""
        (tmp_path / "src").mkdir()
        (tmp_path / "src" / "main.py").write_text("main")
        (tmp_path / "src" / "utils").mkdir()
        (tmp_path / "src" / "utils" / "helper.py").write_text("helper")
        (tmp_path / "Dockerfile").write_text("FROM ubuntu")

        tar_bytes = dir_context_tar_bytes(str(tmp_path))

        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r") as tar:
            names = tar.getnames()
            assert "src" in names
            assert "src/main.py" in names
            assert "src/utils" in names
            assert "src/utils/helper.py" in names
