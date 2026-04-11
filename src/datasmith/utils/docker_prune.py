from __future__ import annotations

import contextlib
import os
import shutil
import subprocess
import threading
from collections.abc import Iterator

from datasmith.utils import get_logger

logger = get_logger("utils.docker_prune")

_DEFAULT_INTERVAL_SEC = 600


def _run_prune() -> None:
    docker = shutil.which("docker")
    if docker is None:
        logger.warning("docker binary not found on PATH; skipping builder prune")
        return
    try:
        result = subprocess.run(
            [docker, "builder", "prune", "-f"],
            check=False,
            capture_output=True,
            text=True,
            timeout=300,
        )
    except subprocess.TimeoutExpired:
        logger.warning("docker builder prune timed out after 300s")
        return
    if result.returncode != 0:
        logger.warning(
            "docker builder prune exited %d: %s",
            result.returncode,
            (result.stderr or result.stdout).strip()[:500],
        )
        return
    reclaimed = ""
    for line in (result.stdout or "").splitlines():
        if "Total reclaimed space" in line:
            reclaimed = line.strip()
            break
    logger.info("docker builder prune: %s", reclaimed or "done")


@contextlib.contextmanager
def builder_prune_watcher(interval_sec: int | None = None) -> Iterator[None]:
    """Periodically run `docker builder prune -f` on a background thread.

    Set DATASMITH_DISABLE_DOCKER_PRUNE=1 to opt out. Override the interval
    with DATASMITH_DOCKER_PRUNE_INTERVAL_SEC (default 600s).
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
