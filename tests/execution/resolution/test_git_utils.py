"""Tests for git worktree cleanup utilities."""

from __future__ import annotations

import os
import time
from collections.abc import Iterable
from pathlib import Path

import pytest

from datasmith.execution.resolution import git_utils


def _set_git_cache_root(monkeypatch: pytest.MonkeyPatch, root: Path) -> None:
    """Point git_utils at a temporary cache root for tests."""

    monkeypatch.setattr(git_utils, "GIT_CACHE_DIR", root)


def _create_worktree(repo: str, sha: str, age_seconds: float) -> Path:
    """Create a fake worktree directory with a specific age."""

    base = git_utils.base_clone_path(repo)
    worktree_dir = base / "worktrees" / sha
    worktree_dir.mkdir(parents=True, exist_ok=True)
    payload = worktree_dir / "data.txt"
    payload.write_text("payload")
    ts = time.time() - age_seconds
    os.utime(worktree_dir, (ts, ts))
    os.utime(payload, (ts, ts))
    return worktree_dir


def _worktree_names(paths: Iterable[Path]) -> set[str]:
    return {p.name for p in paths}


def test_cleanup_skips_locked_worktrees(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Worktrees with active locks are protected from cleanup."""

    _set_git_cache_root(monkeypatch, tmp_path)
    repo = "org/repo"
    locked = _create_worktree(repo, "locked", age_seconds=100)
    victim = _create_worktree(repo, "victim", age_seconds=200)

    lock = git_utils._get_worktree_lock(repo, "locked")  # type: ignore[attr-defined]
    acquired = lock.acquire(blocking=False)
    try:
        assert acquired  # sanity check the lock is now held by this thread

        removed = git_utils.cleanup_worktree_cache(
            repo,
            max_worktrees=0,
            max_age_seconds=None,
            min_free_gb=0,
        )

        assert _worktree_names(removed) == {"victim"}
        assert locked.exists()
        assert not victim.exists()
    finally:
        if acquired:
            lock.release()


def test_cleanup_prunes_by_count(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Oldest worktrees are removed when exceeding the count limit."""

    _set_git_cache_root(monkeypatch, tmp_path)
    repo = "org/repo"
    newest = _create_worktree(repo, "ccc", age_seconds=10)
    mid = _create_worktree(repo, "bbb", age_seconds=20)
    oldest = _create_worktree(repo, "aaa", age_seconds=30)

    removed = git_utils.cleanup_worktree_cache(
        repo,
        max_worktrees=1,
        max_age_seconds=None,
        min_free_gb=0,
    )

    assert _worktree_names(removed) == {"aaa", "bbb"}
    assert newest.exists()
    assert not mid.exists()
    assert not oldest.exists()


def test_cleanup_respects_ttl(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Worktrees older than the TTL are pruned even if under the count limit."""

    _set_git_cache_root(monkeypatch, tmp_path)
    repo = "org/repo"
    fresh = _create_worktree(repo, "fresh", age_seconds=30)
    stale = _create_worktree(repo, "stale", age_seconds=3600)

    removed = git_utils.cleanup_worktree_cache(
        repo,
        max_worktrees=5,
        max_age_seconds=300,
        min_free_gb=0,
    )

    assert _worktree_names(removed) == {"stale"}
    assert fresh.exists()
    assert not stale.exists()


def test_cleanup_preserves_active(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Active SHAs are preserved even when aggressive cleanup is requested."""

    _set_git_cache_root(monkeypatch, tmp_path)
    repo = "org/repo"
    active = _create_worktree(repo, "keep", age_seconds=10)
    doomed = _create_worktree(repo, "drop", age_seconds=20)

    removed = git_utils.cleanup_worktree_cache(
        repo,
        active_shas={"keep"},
        max_worktrees=0,
        max_age_seconds=None,
        min_free_gb=0,
    )

    assert _worktree_names(removed) == {"drop"}
    assert active.exists()
    assert not doomed.exists()


def test_cleanup_frees_space_when_low(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Disk guard removes additional worktrees when free space is insufficient."""

    _set_git_cache_root(monkeypatch, tmp_path)
    repo = "org/repo"
    w1 = _create_worktree(repo, "one", age_seconds=10)
    w2 = _create_worktree(repo, "two", age_seconds=20)

    free_values = iter([0.5, 0.5, 5.0])

    def fake_free_gb(_path: Path) -> float:
        try:
            return next(free_values)
        except StopIteration:  # pragma: no cover - defensive fallback
            return 5.0

    monkeypatch.setattr(git_utils, "_free_gb", fake_free_gb)

    removed = git_utils.cleanup_worktree_cache(
        repo,
        max_worktrees=None,
        max_age_seconds=None,
        min_free_gb=1.0,
    )

    assert _worktree_names(removed) == {"two", "one"}
    assert not w1.exists()
    assert not w2.exists()
