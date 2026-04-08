"""Build context utilities for Docker image builds.

This module provides functions to create reproducible tar archives from directories
with .dockerignore support, ensuring stable mtimes and file ordering for better
cache hits.
"""

from __future__ import annotations

import io
import os
import stat
import tarfile
from fnmatch import fnmatch
from pathlib import Path


def read_dockerignore(root: Path) -> tuple[list[str], list[str]]:
    """Read and parse .dockerignore file.

    Args:
        root: Root directory to look for .dockerignore

    Returns:
        Tuple of (ignores, negates) where ignores are patterns to exclude
        and negates are patterns that override ignores (lines starting with !)
    """
    path = root / ".dockerignore"
    if not path.exists():
        return [], []
    ignores: list[str] = []
    negates: list[str] = []
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("!"):
            negates.append(line[1:].strip())
        else:
            ignores.append(line)
    # Always ignore .git if not explicitly negated
    if ".git" not in ignores:
        ignores.append(".git")
    return ignores, negates


def path_matches_any(rel_posix: str, pats: list[str]) -> bool:
    """Check if a relative path matches any pattern.

    Supports glob patterns (*, ?, **) via fnmatch.
    Also treats patterns ending with / as directory prefixes.

    Args:
        rel_posix: Relative POSIX-style path
        pats: List of patterns to check against

    Returns:
        True if path matches any pattern
    """
    # naive but effective: support *, ?, ** via fnmatch; also treat dir/ as prefix
    for p in pats:
        if p.endswith("/") and (rel_posix == p[:-1] or rel_posix.startswith(p)):
            return True
        if fnmatch(rel_posix, p) or fnmatch("/" + rel_posix, p):
            return True
    return False


def dir_context_tar_bytes(root_dir: str, dockerfile_name: str = "Dockerfile") -> bytes:  # noqa: C901
    """Create a reproducible tar archive of a directory for Docker build context.

    Creates a deterministic tar archive with:
    - Stable file ordering (sorted)
    - Normalized metadata (mtime=0, uid/gid=0, etc.)
    - .dockerignore support
    - Symlink preservation

    Args:
        root_dir: Directory to archive
        dockerfile_name: Name of the Dockerfile (will be copied to "Dockerfile" in tar)

    Returns:
        Bytes of the tar archive
    """
    root = Path(root_dir).resolve()
    ignores, negates = read_dockerignore(root)

    def is_included(p: Path) -> bool:
        rel = p.relative_to(root).as_posix()
        if rel == "":
            return True
        ignored = path_matches_any(rel, ignores)
        if ignored and path_matches_any(rel, negates):
            ignored = False
        return not ignored

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        # walk deterministically
        for dirpath, dirnames, filenames in os.walk(root):
            dirpath_p = Path(dirpath)
            # sort for deterministic order
            dirnames.sort()
            filenames.sort()
            # ensure directory entries are added with stable metadata
            rel_dir = dirpath_p.relative_to(root).as_posix()
            if rel_dir != "" and is_included(dirpath_p):
                ti = tarfile.TarInfo(name=rel_dir)
                ti.type = tarfile.DIRTYPE
                ti.mode = 0o755
                ti.mtime = 0
                ti.uid = ti.gid = 0
                ti.uname = ti.gname = ""
                tar.addfile(ti)

            # files
            for name in filenames:
                p = dirpath_p / name
                if not is_included(p):
                    continue
                rel = p.relative_to(root).as_posix()

                try:
                    st = os.lstat(p)
                except FileNotFoundError:
                    continue  # raced; skip

                if stat.S_ISLNK(st.st_mode):
                    # preserve symlink
                    ti = tarfile.TarInfo(name=rel)
                    ti.type = tarfile.SYMTYPE
                    ti.linkname = os.readlink(p)
                    ti.mode = 0o777
                    ti.mtime = 0
                    ti.uid = ti.gid = 0
                    ti.uname = ti.gname = ""
                    tar.addfile(ti)
                elif stat.S_ISREG(st.st_mode):
                    ti = tarfile.TarInfo(name=rel)
                    ti.size = st.st_size
                    ti.mode = stat.S_IMODE(st.st_mode) or 0o644
                    ti.mtime = 0
                    ti.uid = ti.gid = 0
                    ti.uname = ti.gname = ""
                    with open(p, "rb") as f:
                        tar.addfile(ti, fileobj=f)
                # other types (sockets, pipes) are skipped

        # Ensure the Dockerfile exists at root with canonical name
        df = root / dockerfile_name
        if df.exists() and dockerfile_name != "Dockerfile":
            # duplicate/alias to "Dockerfile" for the builder
            with open(df, "rb") as f:
                data = f.read()
            ti = tarfile.TarInfo(name="Dockerfile")
            ti.size = len(data)
            ti.mode = 0o644
            ti.mtime = 0
            ti.uid = ti.gid = 0
            ti.uname = ti.gname = ""
            tar.addfile(ti, io.BytesIO(data))
    buf.seek(0)
    return buf.getvalue()
