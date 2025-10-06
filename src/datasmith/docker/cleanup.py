"""Docker cleanup utilities for containers, images, and build artifacts.

This module provides functions to clean up Docker resources including containers,
images, build caches, networks, and volumes.
"""

from __future__ import annotations

import contextlib
import logging
from typing import Any

import docker
from docker.errors import APIError, ImageNotFound, NotFound

logger = logging.getLogger(__name__)


def remove_containers_by_label(client: docker.DockerClient, run_id: str) -> None:
    """
    Fast container cleanup for this run_id:
      - remove all labeled containers (force)
      - prune any stopped containers that still carry the label (server-side)
    """
    with contextlib.suppress(Exception):
        for c in client.containers.list(all=True, filters={"label": f"datasmith.run={run_id}"}):
            try:
                logger.debug("Removing container %s (%s)", c.name, c.id[:12])
                c.remove(force=True)
            except NotFound:
                pass

        # Server-side prune is much faster and avoids N API calls.
        with contextlib.suppress(Exception):
            client.containers.prune(filters={"label": [f"datasmith.run={run_id}"]})


def soft_prune(client: docker.DockerClient, run_id: str | None) -> None:
    """Soft prune of Docker resources.

    Prunes:
    - Stopped containers older than 1h
    - Dangling/unused images (optionally filtered by run label)
    - BuildKit cache (if available)

    Args:
        client: Docker client instance
        run_id: Optional run ID to filter images for pruning
    """
    # Prune stopped containers older than 1h
    try:
        client.containers.prune(filters={"until": "1h"})
    except Exception:
        logger.exception("containers.prune failed")

    # Prune dangling/unused images; filter by run label if available
    try:
        flt: dict[str, Any] = {"until": "1h"}
        if run_id:
            flt["label"] = [f"datasmith.run={run_id}"]  # pyright: ignore[reportArgumentType]
        report = client.images.prune(filters=flt)
        logger.info("images.prune reclaimed %s bytes", report.get("SpaceReclaimed", 0))
    except Exception:
        logger.exception("images.prune failed")

    # Optional: BuildKit cache prune (API may not exist on older docker-py)
    try:
        if hasattr(client.api, "prune_builds"):
            client.api.prune_builds(filters={"until": "24h"})
    except Exception:
        logger.debug("build cache prune not available or failed", exc_info=True)


def fast_cleanup_run_artifacts(  # noqa: C901
    client: docker.DockerClient,
    run_id: str,
    *,
    extra_image_refs: list[str] | None = None,
) -> None:
    """
    Aggressive but safe cleanup that prefers server-side prunes and removes by image ID:
      1) Resolve explicit image refs (tags/names) to IDs.
      2) Union with all images carrying datasmith.run=run_id.
      3) Remove by ID; then issue a server-side prune for all *unused* images with that label.
      4) Best-effort prune build cache, networks, volumes by label.
    """
    extra_image_refs = extra_image_refs or []

    img_ids: set[str] = set()
    with contextlib.suppress(Exception):
        for ref in extra_image_refs:
            try:
                img = client.images.get(ref)
                labels = getattr(img, "labels", None) or img.attrs.get("Config", {}).get("Labels", {}) or {}
                if (labels.get("datasmith.run") == run_id) and img.id:
                    img_ids.add(img.id)
            except (ImageNotFound, NotFound):
                pass

    with contextlib.suppress(Exception):
        for iid in img_ids:
            try:
                logger.debug("Removing image id=%s", iid[:20])
                client.images.remove(iid, force=True, noprune=True)
            except (ImageNotFound, NotFound):
                pass
            except APIError as e:
                if getattr(e, "status_code", None) != 409:
                    logger.debug("images.remove(%s) failed: %s", iid[:20], getattr(e, "explanation", e))

    with contextlib.suppress(Exception):
        client.images.prune(filters={"label": [f"datasmith.run={run_id}"], "dangling": False})

    with contextlib.suppress(Exception):
        low = getattr(client, "api", None)
        if low is not None:
            if hasattr(low, "prune_builds"):
                low.prune_builds(filters={"label": [f"datasmith.run={run_id}"]})
            elif hasattr(low, "build_prune"):
                low.build_prune(filters={"labels": [f"datasmith.run={run_id}"]})

    with contextlib.suppress(Exception):
        client.networks.prune(filters={"label": [f"datasmith.run={run_id}"]})
    with contextlib.suppress(Exception):
        client.volumes.prune(filters={"label": [f"datasmith.run={run_id}"]})
