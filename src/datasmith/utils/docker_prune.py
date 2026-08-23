from __future__ import annotations

import contextlib
import os
import shutil
import subprocess
import threading
from collections.abc import Iterator

from datasmith.utils import get_logger

logger = get_logger("utils.docker_prune")

_DEFAULT_INTERVAL_SEC = 7200

# Prune only when the Docker filesystem is actually under pressure.
#
# The watcher used to prune on a timer, unconditionally. That is the wrong
# trigger: `docker builder prune` deletes the BuildKit cache, which is the thing
# that makes a rebuild cheap, and it was firing every 7200s against a filesystem
# at 8% capacity. Median attempt duration is 4098s, so it landed inside roughly
# every second build.
#
# Disk pressure is the condition the watcher exists for, so test that condition.
DATASMITH_DOCKER_PRUNE_MIN_USED_PCT: float = float(os.environ.get("DATASMITH_DOCKER_PRUNE_MIN_USED_PCT", "85"))


def _docker_root() -> str | None:
    """Where Docker actually stores its data. Not always on the root filesystem.

    On this host it is /mnt/sdd2/docker, while / is the filesystem under
    pressure. Pruning because / is full would delete cache to relieve a disk
    Docker does not use.
    """
    docker = shutil.which("docker")
    if docker is None:
        return None
    try:
        result = subprocess.run(
            [docker, "info", "--format", "{{.DockerRootDir}}"],
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (subprocess.TimeoutExpired, OSError):
        return None
    root = (result.stdout or "").strip()
    return root or None


def _used_pct(path: str) -> float | None:
    """Percentage of `path`'s filesystem in use, or None if it cannot be read."""
    try:
        usage = shutil.disk_usage(path)
    except OSError:
        return None
    if usage.total <= 0:
        return None
    return 100.0 * (usage.total - usage.free) / usage.total


def _should_prune() -> tuple[bool, str]:
    """Decide whether to prune, and say why either way.

    When usage cannot be determined the answer is yes. The watcher exists to
    stop the disk filling, and a stalled pipeline costs more than a cold cache.
    """
    root = _docker_root()
    if root is None:
        return True, "docker root unknown; pruning to stay safe"
    used = _used_pct(root)
    if used is None:
        return True, f"cannot read usage of {root}; pruning to stay safe"
    if used >= DATASMITH_DOCKER_PRUNE_MIN_USED_PCT:
        return True, f"{root} is {used:.1f}% used (>= {DATASMITH_DOCKER_PRUNE_MIN_USED_PCT}%)"
    return False, f"{root} is {used:.1f}% used (< {DATASMITH_DOCKER_PRUNE_MIN_USED_PCT}%); keeping build cache"


def _run_prune_cmd(docker: str, args: list[str], label: str) -> None:
    try:
        result = subprocess.run(
            [docker, *args],
            check=False,
            capture_output=True,
            text=True,
            timeout=600,
        )
    except subprocess.TimeoutExpired:
        logger.warning("%s timed out after 600s", label)
        return
    if result.returncode != 0:
        logger.warning(
            "%s exited %d: %s",
            label,
            result.returncode,
            (result.stderr or result.stdout).strip()[:500],
        )
        return
    reclaimed = ""
    for line in (result.stdout or "").splitlines():
        if "Total reclaimed space" in line:
            reclaimed = line.strip()
            break
    logger.info("%s: %s", label, reclaimed or "done")


def _run_prune(force: bool = False) -> None:
    """Prune Docker, but only when the disk needs it.

    `force=True` skips the disk check, for callers that prune between stages
    rather than during a build.
    """
    docker = shutil.which("docker")
    if docker is None:
        logger.warning("docker binary not found on PATH; skipping docker prune")
        return
    if not force:
        wanted, reason = _should_prune()
        if not wanted:
            logger.info("skipping docker prune: %s", reason)
            return
        logger.info("pruning docker: %s", reason)
    _run_prune_cmd(docker, ["builder", "prune", "-f"], "docker builder prune")
    _run_prune_cmd(docker, ["image", "prune", "-f"], "docker image prune")


@contextlib.contextmanager
def builder_prune_watcher(interval_sec: int | None = None) -> Iterator[None]:
    """Periodically run `docker builder prune -f` on a background thread.

    Each tick prunes ONLY when the Docker filesystem is at or above
    DATASMITH_DOCKER_PRUNE_MIN_USED_PCT (default 85). Below that the build cache
    is left alone, because deleting it is what makes every rebuild pay full
    price for downloads and compilation.

    Set DATASMITH_DISABLE_DOCKER_PRUNE=1 to opt out entirely. Override the
    interval with DATASMITH_DOCKER_PRUNE_INTERVAL_SEC (default 7200s).
    """
    if os.environ.get("DATASMITH_DISABLE_DOCKER_PRUNE"):
        logger.info("docker builder prune watcher disabled via env var")
        yield
        return

    if interval_sec is None:
        raw = os.environ.get("DATASMITH_DOCKER_PRUNE_INTERVAL_SEC")
        try:
            interval_sec = int(raw) if raw else _DEFAULT_INTERVAL_SEC
        except ValueError:
            interval_sec = _DEFAULT_INTERVAL_SEC

    stop = threading.Event()

    def _loop() -> None:
        logger.info("docker builder prune watcher started (interval=%ds)", interval_sec)
        while not stop.wait(interval_sec):
            _run_prune()
        logger.info("docker builder prune watcher stopped")

    thread = threading.Thread(target=_loop, name="docker-builder-prune", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=5)
