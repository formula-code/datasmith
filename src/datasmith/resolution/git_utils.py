"""Git repository operations for dependency resolution."""

from __future__ import annotations

import io
import os
import shutil
import threading
import time
from collections.abc import Iterable
from contextlib import suppress
from pathlib import Path
from typing import Any, Callable, cast

from git import Commit, Repo

from datasmith.utils import get_logger

from .constants import ASV_REGEX, GIT_CACHE_DIR

_worktree_lock_registry: dict[tuple[str, str], threading.Lock] = {}
_worktree_registry_lock = threading.Lock()
_worktree_cleanup_lock = threading.Lock()


def _env_non_negative_int(var: str, default: int) -> int:
    raw = os.getenv(var)
    if raw is None:
        return default
    try:
        value = int(float(raw))
    except ValueError:
        return default
    return max(0, value)


def _env_non_negative_float(var: str, default: float) -> float:
    raw = os.getenv(var)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return max(0.0, value)


def _default_worktree_ttl_seconds() -> int | None:
    raw = os.getenv("DATASMITH_GIT_WORKTREE_TTL_SECONDS")
    if raw is None:
        return 24 * 3600
    try:
        value = int(float(raw))
    except ValueError:
        return 24 * 3600
    return value if value > 0 else None


DEFAULT_MAX_WORKTREES_PER_REPO = _env_non_negative_int("DATASMITH_GIT_MAX_WORKTREES_PER_REPO", 128)
DEFAULT_WORKTREE_TTL_SECONDS = _default_worktree_ttl_seconds()
DEFAULT_WORKTREE_MIN_FREE_GB = _env_non_negative_float("DATASMITH_GIT_WORKTREE_MIN_FREE_GB", 256.0)

logger = get_logger("resolution.git_utils")


def _get_worktree_lock(repo_name: str, sha: str) -> threading.Lock:
    """Return a per-(repo, sha) mutex used to serialize worktree materialization."""
    key = (repo_name, sha)
    with _worktree_registry_lock:
        lock = _worktree_lock_registry.get(key)
        if lock is None:
            lock = threading.Lock()
            _worktree_lock_registry[key] = lock
    return lock


def repo_key(repo_name: str) -> str:
    """Convert a GitHub repo name to a filesystem-safe key."""
    return repo_name.replace("/", "__")


def base_clone_path(repo_name: str) -> Path:
    """Get the path for a base clone of a repository."""
    return GIT_CACHE_DIR / "base_clones" / repo_key(repo_name)


def mirror_path(repo_name: str) -> Path:
    """Get the path for a bare mirror of a repository."""
    return GIT_CACHE_DIR / "mirrors" / f"{repo_key(repo_name)}.git"


def worktree_root(repo_name: str) -> Path:
    """Return the persistent worktree root for a repository."""
    return base_clone_path(repo_name) / "worktrees"


def _free_gb(path: Path) -> float:
    """Calculate the free space at ``path`` in gigabytes."""
    try:
        usage = shutil.disk_usage(path)
    except FileNotFoundError:
        return float("inf")
    return usage.free / (1024**3)


def _remove_worktree_dir(base_repo: Repo | None, worktree_dir: Path) -> None:
    """Remove a worktree directory, falling back to direct removal on failure."""
    with suppress(Exception):
        if base_repo is not None:
            base_repo.git.worktree("remove", str(worktree_dir), "--force")
    with suppress(Exception):
        if worktree_dir.exists():
            shutil.rmtree(worktree_dir)


def cleanup_worktree_cache(  # noqa: C901
    repo_name: str,
    *,
    base_repo: Repo | None = None,
    active_shas: Iterable[str] | None = None,
    max_worktrees: int | None = None,
    max_age_seconds: int | None = None,
    min_free_gb: float | None = None,
) -> list[Path]:
    """Remove stale or excess worktrees for ``repo_name``."""
    protected = set(active_shas or ())
    with _worktree_registry_lock:
        for (name, locked_sha), lock in _worktree_lock_registry.items():
            if name == repo_name and lock.locked():
                protected.add(locked_sha)
    ttl_seconds = max_age_seconds if max_age_seconds is not None else DEFAULT_WORKTREE_TTL_SECONDS
    ttl_seconds = ttl_seconds if ttl_seconds and ttl_seconds > 0 else None
    keep_limit = max_worktrees if max_worktrees is not None else DEFAULT_MAX_WORKTREES_PER_REPO
    if keep_limit is not None:
        keep_limit = max(0, keep_limit)
    min_free: float | None = min_free_gb if min_free_gb is not None else DEFAULT_WORKTREE_MIN_FREE_GB
    min_free = min_free if min_free and min_free > 0 else None

    wroot = worktree_root(repo_name)
    if not wroot.exists():
        return []

    repo = base_repo
    if repo is None:
        with suppress(Exception):
            repo = Repo(base_clone_path(repo_name))

    removed: list[Path] = []

    with _worktree_cleanup_lock:
        entries: list[tuple[Path, float, str]] = []
        protected_entries: list[tuple[Path, float, str]] = []
        for child in wroot.iterdir():
            if not child.is_dir():
                continue
            sha = child.name
            try:
                mtime = child.stat().st_mtime
            except FileNotFoundError:
                continue
            entry = (child, mtime, sha)
            if sha in protected:
                protected_entries.append(entry)
            else:
                entries.append(entry)

        now = time.time()
        if ttl_seconds is not None:
            cutoff = now - ttl_seconds
            fresh_entries: list[tuple[Path, float, str]] = []
            for path, mtime, sha in entries:
                if mtime < cutoff:
                    removed.append(path)
                    _remove_worktree_dir(repo, path)
                else:
                    fresh_entries.append((path, mtime, sha))
            entries = fresh_entries

        entries.sort(key=lambda item: item[1], reverse=True)
        kept_entries: list[tuple[Path, float, str]] = list(protected_entries)

        if keep_limit is not None:
            available = keep_limit - len(protected_entries)
            if available <= 0:
                to_remove = entries
            else:
                kept_entries.extend(entries[:available])
                to_remove = entries[available:]
        else:
            kept_entries.extend(entries)
            to_remove = []

        for entry in to_remove:
            path = entry[0]
            removed.append(path)
            _remove_worktree_dir(repo, path)

        if min_free is not None:
            removable = [entry for entry in kept_entries if entry[2] not in protected]
            removable.sort(key=lambda item: item[1])
            free_space = _free_gb(GIT_CACHE_DIR)
            idx = 0
            while free_space < min_free and idx < len(removable):
                entry = removable[idx]
                path = entry[0]
                idx += 1
                removed.append(path)
                _remove_worktree_dir(repo, path)
                free_space = _free_gb(GIT_CACHE_DIR)

        if repo is not None and removed:
            with suppress(Exception):
                repo.git.worktree("prune", "--expire=now")

    if removed:
        logger.debug("Removed %d worktree(s) for %s", len(removed), repo_name)

    return removed


def ensure_base_clone(repo_name: str) -> Repo:
    """Ensure a non-bare base clone exists (partial clone)."""
    url = f"https://github.com/{repo_name}.git"
    path = base_clone_path(repo_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    repo = Repo.clone_from(url, path, multi_options=["--filter=blob:none"]) if not path.exists() else Repo(path)
    return repo


def ensure_mirror(repo_name: str) -> Path:
    """Ensure a local bare mirror exists."""
    url = f"https://github.com/{repo_name}.git"
    mpath = mirror_path(repo_name)
    mpath.parent.mkdir(parents=True, exist_ok=True)
    if not mpath.exists():
        Repo.clone_from(url, mpath, mirror=True, multi_options=["--filter=blob:none"])
    else:
        with suppress(Exception):
            Repo(mpath).remote().update(prune=True)
    return mpath


def ensure_commit_available(repo: Repo, sha: str) -> None:
    """Make sure the repo has the object for `sha`. If not, fetch just that SHA."""
    with suppress(Exception):
        repo.commit(sha)
        return
    repo.git.fetch("origin", sha)


def prepare_repo_checkout(repo_name: str, sha: str, tmp_root: Path) -> tuple[Repo, Path, Callable[[], None]]:
    """Prefer a worktree from a cached base clone; fall back to a reference clone."""
    persistent_root = worktree_root(repo_name)
    worktree_dir = persistent_root / sha
    lock = _get_worktree_lock(repo_name, sha)
    with lock:
        try:
            base_repo = ensure_base_clone(repo_name)
            cleanup_worktree_cache(repo_name, base_repo=base_repo, active_shas={sha})
            ensure_commit_available(base_repo, sha)
            worktree_dir.parent.mkdir(parents=True, exist_ok=True)
            git_dir = worktree_dir / ".git"
            if git_dir.exists():
                wt_repo = Repo(worktree_dir)
                try:
                    current = wt_repo.head.commit.hexsha
                except Exception:
                    current = None
                if current != sha:
                    with suppress(Exception):
                        wt_repo.git.reset("--hard", sha)
            else:
                if worktree_dir.exists():
                    shutil.rmtree(worktree_dir)
                with suppress(Exception):
                    base_repo.git.worktree("prune", "--expire=now")
                base_repo.git.worktree("add", "--detach", str(worktree_dir), sha)
                wt_repo = Repo(worktree_dir)

            def _cleanup_worktree() -> None:
                with suppress(Exception):
                    base_repo.git.worktree("remove", str(worktree_dir), "--force")
                with suppress(Exception):
                    if worktree_dir.exists():
                        shutil.rmtree(worktree_dir)
                with suppress(Exception):
                    base_repo.git.worktree("prune")

            return wt_repo, worktree_dir, _cleanup_worktree
        except Exception:  # noqa: S110
            pass

    # Reference clone fallback
    repo_dir = tmp_root / "repo"
    mirror = ensure_mirror(repo_name)
    url = f"https://github.com/{repo_name}.git"
    repo = Repo.clone_from(
        url,
        to_path=repo_dir,
        reference=str(mirror),
        multi_options=["--filter=blob:none", "--no-tags"],
    )
    ensure_commit_available(repo, sha)
    with suppress(Exception):
        repo.git.checkout(sha)

    def _cleanup_refclone() -> None:
        return None

    return repo, repo_dir, _cleanup_refclone


def base_tmp_for_commit(commit: Commit) -> Path:
    """Base directory for transient artifacts tied to a specific commit."""
    worktree = commit.repo.working_tree_dir
    if worktree is None:
        raise ValueError("Commit repository has no working tree directory")
    return Path(worktree)


def materialize_blobs(
    commit: Commit,
    predicate: Callable[[str], bool],
    out_dirname: str,
) -> dict[str, Path]:
    """Copy matching blobs from <commit> into a workspace-local folder."""
    base = base_tmp_for_commit(commit) / out_dirname
    base.mkdir(parents=True, exist_ok=True)
    out: dict[str, Path] = {}
    for raw_item in commit.tree.traverse():
        item = cast(Any, raw_item)
        if getattr(item, "type", None) != "blob":
            continue
        relpath = cast(str, getattr(item, "path", ""))
        if predicate(relpath):
            dst = base / relpath
            parent = dst.parent
            try:
                if parent.exists() and parent.is_file():
                    parent.unlink()
            except Exception as e:
                logger.debug("Failed to remove parent file %s: %s", parent, e)
            parent.mkdir(parents=True, exist_ok=True)
            try:
                if dst.exists() and dst.is_dir():
                    shutil.rmtree(dst)
            except Exception as e:
                logger.debug("Failed to remove directory %s: %s", dst, e)
            data_stream = getattr(item, "data_stream", None)
            if data_stream is None:
                continue
            with io.BytesIO(data_stream.read()) as src, open(dst, "wb") as f:
                f.write(src.read())
            out[relpath] = dst
    return out


def read_blob_text(commit: Commit, relpath: str, default: str | None = None) -> str | None:
    """Read a text file from a commit by path."""
    try:
        blob = cast(Any, commit.tree / relpath)
        if getattr(blob, "type", None) != "blob":
            return default
        data_stream = getattr(blob, "data_stream", None)
        if data_stream is None:
            return default
        raw_bytes = data_stream.read()
        if not isinstance(raw_bytes, (bytes, bytearray)):
            return default
        return bytes(raw_bytes).decode("utf-8", errors="replace")
    except Exception:
        return default


def asv_finder(commit: Commit) -> list[Path]:
    """Find ASV configuration files in a commit."""
    mats = materialize_blobs(commit, lambda rel: bool(ASV_REGEX.search(rel)), out_dirname="_asv_blobs")
    return list(mats.values())
