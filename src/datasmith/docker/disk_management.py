"""Disk space monitoring and management utilities for Docker.

This module provides functions to monitor disk space and automatically prune
Docker resources when disk space runs low.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import shutil

import docker

from datasmith.docker.cleanup import soft_prune

logger = logging.getLogger(__name__)


def docker_data_root() -> str:
    """Get the Docker data root directory.

    Can be overridden with the DOCKER_DATA_ROOT environment variable.

    Returns:
        Path to Docker data directory (default: /var/lib/docker)
    """
    return os.environ.get("DOCKER_DATA_ROOT", "/var/lib/docker")


def free_gb(path: str) -> float:
    """Get free disk space in GB for a given path.

    Args:
        path: Filesystem path to check

    Returns:
        Free space in GB, or infinity if path doesn't exist
    """
    try:
        usage = shutil.disk_usage(path)
        return usage.free / (1024**3)
    except FileNotFoundError:
        # Fallback: if path doesn't exist, skip guard
        return float("inf")


async def guard_and_prune(
    client: docker.DockerClient,
    min_free_gb: float,
    data_root: str,
    run_id: str | None,
    hard_fail: bool,
) -> None:
    """Check disk space and prune if below minimum threshold.

    Args:
        client: Docker client instance
        min_free_gb: Minimum required free space in GB
        data_root: Docker data root directory
        run_id: Optional run ID for targeted pruning
        hard_fail: If True, raise SystemExit when disk space cannot be freed

    Raises:
        SystemExit: If hard_fail=True and disk space is still below threshold after pruning
    """
    free = free_gb(data_root)
    if free >= min_free_gb:
        return

    logger.warning("Low disk on %s: %.1f GB free < %.1f GB. Pruning&", data_root, free, min_free_gb)
    # Run pruning in a thread to keep the event loop responsive
    await asyncio.to_thread(soft_prune, client, run_id)
    free2 = free_gb(data_root)
    logger.info("After prune: %.1f GB free (target: %.1f GB)", free2, min_free_gb)

    if hard_fail and free2 < min_free_gb:
        raise SystemExit(f"Insufficient disk space after prune: {free2:.1f} GB free (need {min_free_gb:.1f} GB).")


async def guard_loop(
    client: docker.DockerClient,
    min_free_gb: float,
    data_root: str,
    run_id: str | None,
    interval_s: int,
    hard_fail: bool,
    stop_event: asyncio.Event,
) -> None:
    """Periodically check disk space and prune if necessary.

    Performs an immediate check on startup, then periodic checks at the specified interval.

    Args:
        client: Docker client instance
        min_free_gb: Minimum required free space in GB
        data_root: Docker data root directory
        run_id: Optional run ID for targeted pruning
        interval_s: Check interval in seconds
        hard_fail: If True, raise SystemExit when disk space cannot be freed
        stop_event: Event to signal loop termination

    Raises:
        SystemExit: If hard_fail=True and disk space is still below threshold after pruning
    """
    # First check immediately
    try:
        await guard_and_prune(client, min_free_gb, data_root, run_id, hard_fail)
    except SystemExit:
        raise
    except Exception:
        logger.exception("Initial disk guard failed")

    # Then periodic checks
    while not stop_event.is_set():
        with contextlib.suppress(SystemExit, Exception):
            await guard_and_prune(client, min_free_gb, data_root, run_id, hard_fail)

        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop_event.wait(), timeout=interval_s)
