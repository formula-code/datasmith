"""Git repository operations for dependency resolution."""

from __future__ import annotations

import io
import shutil
import threading
from contextlib import suppress
from pathlib import Path
from typing import Any, Callable, cast

from git import Commit, Repo

from .constants import ASV_REGEX, GIT_CACHE_DIR

_worktree_lock_registry: dict[tuple[str, str], threading.Lock] = {}
_worktree_registry_lock = threading.Lock()


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


def ensure_base_clone(repo_name: str) -> Repo:
    """
    Ensure a non-bare base clone exists (partial clone). Suitable for adding worktrees.

    The clone is created once and then reused without automatically fetching; callers
    should invoke ``ensure_commit_available`` for specific SHAs that might be missing.
    """
    url = f"https://github.com/{repo_name}.git"
    path = base_clone_path(repo_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Non-bare partial clone keeps blobs lazy; great for fast checkouts at many SHAs.
    repo = Repo.clone_from(url, path, multi_options=["--filter=blob:none"]) if not path.exists() else Repo(path)
    return repo


def ensure_mirror(repo_name: str) -> Path:
    """
    Ensure a local bare mirror exists (used for fast reference clones fallback).
    """
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
    """
    Make sure the repo has the object for `sha`. If not, fetch just that SHA.
    """
    with suppress(Exception):
        repo.commit(sha)
        return
    # Try to fetch the specific object (GitHub supports this)
    repo.git.fetch("origin", sha)


def prepare_repo_checkout(repo_name: str, sha: str, tmp_root: Path) -> tuple[Repo, Path, Callable[[], None]]:
    """
    Prefer a worktree from a cached base clone; fall back to a reference clone against a local mirror.
    Returns (repo, working_tree_path, cleanup_callback).
    """
    # 1) Reusable worktree (preferred)
    persistent_root = base_clone_path(repo_name) / "worktrees"
    worktree_dir = persistent_root / sha
    lock = _get_worktree_lock(repo_name, sha)
    with lock:
        try:
            base_repo = ensure_base_clone(repo_name)
            ensure_commit_available(base_repo, sha)
            worktree_dir.parent.mkdir(parents=True, exist_ok=True)
            git_dir = worktree_dir / ".git"
            if git_dir.exists():
                wt_repo = Repo(worktree_dir)
                try:
                    current = wt_repo.head.commit.hexsha
                except Exception:
                    current = None
                with suppress(Exception):
                    wt_repo.git.clean("-xfd")
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
                return None

            return wt_repo, worktree_dir, _cleanup_worktree  # noqa: TRY300
        except Exception:  # noqa: S110
            # Fall back to a fresh clone referencing a local mirror
            pass

    # 2) Reference clone fallback
    repo_dir = tmp_root / "repo"
    mirror = ensure_mirror(repo_name)
    url = f"https://github.com/{repo_name}.git"
    repo = Repo.clone_from(
        url,
        to_path=repo_dir,
        reference=str(mirror),  # --reference=<mirror>
        multi_options=["--filter=blob:none", "--no-tags"],
    )
    ensure_commit_available(repo, sha)
    with suppress(Exception):
        repo.git.checkout(sha)

    def _cleanup_refclone() -> None:
        # TempDirectory will remove the files; nothing special required
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
    """
    Copy matching blobs from <commit> into a workspace-local folder inside the
    checked-out worktree, preserving relative paths. Returns a mapping
    {repo_relpath -> local Path}.
    """
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
            dst.parent.mkdir(parents=True, exist_ok=True)
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
