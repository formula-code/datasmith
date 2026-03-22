"""DockerHub publishing module for Docker images.

This module provides functionality to publish Docker images to DockerHub.
It supports:
- Single repository mode (all images in one repo with encoded tags)
- Mirror mode (each local repo maps to a DockerHub repo)
- Skip existing images via Registry API v2
- Parallel push with configurable workers
- Retry logic with exponential backoff
- Rate limiting detection and handling
"""

import hashlib
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Optional

import docker
import requests
from docker.errors import APIError

from datasmith.core.models import Task
from datasmith.logging_config import configure_logging

logger = configure_logging()


def publish_images_to_dockerhub(  # noqa: C901
    local_refs: list[str],
    namespace: str,
    *,
    repository_mode: str = "single",  # "single" or "mirror"
    single_repo: str = "all",  # used when repository_mode="single"
    dockerhub_repo_prefix: str | None = None,  # used when repository_mode="mirror"
    skip_existing: bool = True,
    verbose: bool = True,
    parallelism: int = 4,  # >1 to push multiple images concurrently
    docker_client: Any = None,
    username: str | None = None,
    password: str | None = None,
) -> dict[str, str]:
    """
    Publish local Docker images to DockerHub and return {local_ref: dockerhub_ref}.

    This function:
      - Authenticates via username/password (no token refresh needed)
      - Uses Docker Registry HTTP API v2 for tag listing
      - Handles rate limiting with exponential backoff
      - Supports both single and mirror repository modes

    Args:
        local_refs: List of local image references (e.g., ["owner-repo-sha:final"])
        namespace: DockerHub namespace (username or organization)
        repository_mode: "single" (all images in one repo) or "mirror" (one repo per image)
        single_repo: Repository name for single mode (default: "all")
        dockerhub_repo_prefix: Prefix for mirror mode repos
        skip_existing: Skip pushing images that already exist on DockerHub
        verbose: Enable detailed logging
        parallelism: Number of concurrent push operations (default: 4)
        docker_client: Optional Docker client (creates one if None)
        username: DockerHub username (or from DOCKERHUB_USERNAME env)
        password: DockerHub token/password (or from DOCKERHUB_TOKEN env)

    Returns:
        Dictionary mapping local_ref -> dockerhub_ref for successfully pushed images

    Raises:
        ValueError: If credentials are missing or repository_mode is invalid
    """

    if not local_refs:
        return {}

    # --- Credential handling ---
    username, password = _get_dockerhub_credentials(username, password)

    # --- Setup Docker ---
    dk = docker_client or docker.from_env()
    registry = "docker.io"

    # --- Helper functions ---
    def _split_image(image_ref: str) -> tuple[str, str]:
        """Split image reference into (repo, tag)."""
        if ":" in image_ref and "/" not in image_ref.split(":", 1)[1]:
            repo, tag = image_ref.rsplit(":", 1)
        else:
            repo, tag = image_ref, "latest"
        return repo, tag

    def _sanitize_repo_name(name: str) -> str:
        """Sanitize repository name for DockerHub."""
        # DockerHub repo: [a-z0-9_.-/]
        sanitized = re.sub(r"[^a-z0-9._/-]", "-", name.lower())
        sanitized = re.sub(r"-+", "-", sanitized).strip("-/.") or "repo"
        if dockerhub_repo_prefix and repository_mode == "mirror":
            pref = dockerhub_repo_prefix.strip("/")
            sanitized = f"{pref}/{sanitized}"
        return sanitized

    def _encode_tag_from_local(local_ref: str) -> str:
        """
        Encode local image reference into DockerHub-compatible tag.

        Docker tag regex: [A-Za-z0-9_][A-Za-z0-9._-]{0,127}
        Encode "/" -> "__", ":" -> "--" to keep info in tag.
        Add an 8-char hash suffix if we must truncate to avoid collisions.
        """
        repo, tag = _split_image(local_ref)
        base = repo.replace("/", "__")
        tag_enc = tag.replace("/", "__").replace(":", "--")
        composed = f"{base}--{tag_enc}"
        if len(composed) <= 128:
            return composed
        h = hashlib.sha256(composed.encode()).hexdigest()[:8]
        trimmed = composed[-(128 - 10) :]
        return f"{trimmed}--{h}"

    # --- DockerHub login ---
    def _login_to_dockerhub() -> None:
        """Login to DockerHub with provided credentials."""
        try:
            dk.login(username=username, password=password, registry=registry)
            if verbose:
                logger.debug(f"Logged in to DockerHub as {username}")
        except Exception as e:
            raise RuntimeError(f"Failed to login to DockerHub: {e}") from e

    _login_to_dockerhub()

    # Determine target repo/tag per local image
    unique_refs = sorted({r for r in local_refs if r})
    plan: list[tuple[str, str, str]] = []  # (local_ref, repo_name, tag)
    repos_needed: set[str] = set()

    if repository_mode == "single":
        repo_name = _sanitize_repo_name(single_repo)
        repos_needed.add(repo_name)
        for lr in unique_refs:
            plan.append((lr, repo_name, _encode_tag_from_local(lr)))
    elif repository_mode == "mirror":
        for lr in unique_refs:
            repo, tag = _split_image(lr)
            repo_name = _sanitize_repo_name(repo)
            repos_needed.add(repo_name)
            plan.append((lr, repo_name, tag))
    else:
        raise ValueError('repository_mode must be "single" or "mirror"')

    # Cache existing tags per repo
    existing_tags_cache: dict[str, set[str]] = {}
    if skip_existing:
        for rn in repos_needed:
            existing_tags_cache[rn] = _list_existing_tags(namespace, rn, username, password)

    lock = threading.Lock()
    results: dict[str, str] = {}
    failures: dict[str, str] = {}

    if verbose:
        logger.info(
            f"Publishing {len(plan)} image(s) to {registry}/{namespace} using mode={repository_mode}, parallelism={parallelism}"
        )
        if repository_mode == "single":
            logger.info(
                "Note: New DockerHub repositories default to PUBLIC visibility. "
                "Change to private manually on DockerHub if needed."
            )

    def _looks_like_rate_limit(msg: Optional[str]) -> bool:
        """Check if error message indicates rate limiting."""
        if not msg:
            return False
        m = str(msg).lower()
        return "rate limit" in m or "429" in m or "too many requests" in m

    def _push_stream_ok_and_digest(lines_iter: Any) -> tuple[bool, Optional[str], Optional[str]]:
        """
        Inspect the push stream:
          - return (ok, digest, error_message)
          - ok=True iff we saw a final 'aux' dict with 'Digest'
        """
        ok = False
        digest = None
        last_error = None
        for line in lines_iter:
            if not isinstance(line, dict):
                continue
            if "error" in line:
                last_error = str(line.get("error"))
            if "errorDetail" in line:
                detail = line.get("errorDetail")
                last_error = str(detail.get("message") if isinstance(detail, dict) else detail)
            aux = line.get("aux")
            if isinstance(aux, dict):
                d = aux.get("Digest") or aux.get("digest")
                if d:
                    ok = True
                    digest = str(d)
            st = line.get("status")
            if st and verbose and any(tok in st for tok in ("Pushed", "Digest", "already exists", "mounted")):
                with lock:
                    logger.debug(st)
        return ok, digest, last_error

    def _push_one(local_ref: str, repo_name: str, tag: str) -> None:  # noqa: C901
        """Push a single image to DockerHub with retry logic."""
        dockerhub_ref = f"{registry}/{namespace}/{repo_name}:{tag}"

        # Skip if already present (from cache)
        if skip_existing:
            tags = existing_tags_cache.get(repo_name)
            if tags is not None and tag in tags:
                if verbose:
                    with lock:
                        logger.debug(f"[DONE] {dockerhub_ref} already exists — skipping")
                with lock:
                    results[local_ref] = dockerhub_ref
                return

        # Ensure the image exists locally and tag it for DockerHub
        try:
            img = dk.images.get(local_ref)
        except Exception as e:
            with lock:
                failures[local_ref] = f"local image not found: {e}"
                logger.debug(f"✖ {local_ref}: not found locally ({e})")
            return

        # Tag (idempotent)
        try:
            img.tag(f"{namespace}/{repo_name}", tag=tag)
        except Exception as e:
            with lock:
                failures[local_ref] = f"failed to tag: {e}"
                logger.debug(f"✖ failed to tag {local_ref} -> {dockerhub_ref}: {e}")
            return

        if verbose:
            with lock:
                logger.debug(f"Pushing {dockerhub_ref} ...")

        # Per-thread low-level client for stable streaming pushes
        def _make_api_client() -> Any:
            return docker.from_env(timeout=1800).api

        def _raise_push_failed(err: Optional[str]) -> None:
            raise RuntimeError(f"push did not complete successfully: {err or 'no digest observed'}")

        max_retries = 3
        backoff_base = 2.0
        rate_limit_wait = int(os.environ.get("DOCKERHUB_RATE_LIMIT_WAIT", "60"))

        for attempt in range(max_retries):
            api = _make_api_client()
            try:
                stream = api.push(
                    f"{namespace}/{repo_name}",
                    tag=tag,
                    stream=True,
                    decode=True,
                    auth_config={
                        "username": username,
                        "password": password,
                    },
                )
                ok, digest, err = _push_stream_ok_and_digest(stream)
                if ok:
                    with lock:
                        if skip_existing:
                            existing_tags_cache.setdefault(repo_name, set()).add(tag)
                        results[local_ref] = dockerhub_ref
                        if verbose and digest:
                            logger.debug(f"[DONE] pushed {dockerhub_ref} ({digest})")
                    return

                # Stream completed without success
                # Check for rate limiting and back off
                if _looks_like_rate_limit(err) and attempt < max_retries - 1:
                    wait_time = min(rate_limit_wait * (2**attempt), 3600)
                    with lock:
                        if verbose:
                            logger.warning(
                                f"⚠ DockerHub rate limit detected for {dockerhub_ref}; "
                                f"waiting {wait_time}s before retry..."
                            )
                    time.sleep(wait_time)
                else:
                    _raise_push_failed(err)

            except APIError as e:
                # Handle rate limiting
                code = getattr(getattr(e, "response", None), "status_code", None)
                if code == 429 and attempt < max_retries - 1:
                    wait_time = min(rate_limit_wait * (2**attempt), 3600)
                    with lock:
                        if verbose:
                            logger.warning(
                                f"⚠ HTTP 429 (rate limit) pushing {dockerhub_ref}; waiting {wait_time}s before retry..."
                            )
                    time.sleep(wait_time)
                else:
                    if attempt >= max_retries - 1:
                        with lock:
                            failures[local_ref] = f"Docker APIError: {e}"
                            logger.debug(f"✖ failed to push {dockerhub_ref}: {e}")
                        return
            except Exception as e:
                if attempt >= max_retries - 1:
                    with lock:
                        failures[local_ref] = str(e)
                        logger.debug(f"✖ failed to push {dockerhub_ref}: {e}")
                    return
            finally:
                if attempt < max_retries - 1:
                    time.sleep((backoff_base**attempt) * 0.7)

    # Parallel pushes
    parallelism = max(1, int(parallelism))
    if parallelism == 1:
        for lr, rn, tg in plan:
            _push_one(lr, rn, tg)
    else:
        with ThreadPoolExecutor(max_workers=parallelism) as ex:
            futs = [ex.submit(_push_one, lr, rn, tg) for lr, rn, tg in plan]
            for _ in as_completed(futs):
                pass

    if verbose and failures:
        with lock:
            logger.info(f"Completed with {len(results)} success(es) and {len(failures)} failure(s).")
            for k, v in failures.items():
                logger.debug(f"  • {k}: {v}")

    return results


def _get_dockerhub_credentials(username: str | None = None, password: str | None = None) -> tuple[str, str]:
    """
    Get DockerHub credentials from multiple sources.

    Priority: function parameters > environment variables > docker config.json

    Args:
        username: Optional username parameter
        password: Optional password parameter

    Returns:
        Tuple of (username, password)

    Raises:
        ValueError: If credentials cannot be found
    """
    # Try parameters first
    user = username or os.environ.get("DOCKERHUB_USERNAME")
    pwd = password or os.environ.get("DOCKERHUB_TOKEN") or os.environ.get("DOCKERHUB_PASSWORD")

    # Try docker config as fallback
    if not user or not pwd:
        config_user, config_pwd = _read_docker_config_credentials()
        user = user or config_user
        pwd = pwd or config_pwd

    if not user or not pwd:
        raise ValueError(
            "DockerHub credentials not found. Please provide via:\n"
            "  1. Function parameters (username, password)\n"
            "  2. Environment variables (DOCKERHUB_USERNAME, DOCKERHUB_TOKEN)\n"
            "  3. Docker login (docker login docker.io)\n"
            "Generate tokens at: https://hub.docker.com/settings/security"
        )

    return user, pwd


def _read_docker_config_credentials() -> tuple[str | None, str | None]:
    """
    Read DockerHub credentials from ~/.docker/config.json.

    Returns:
        Tuple of (username, password) or (None, None) if not found
    """
    import base64
    import json

    config_path = Path.home() / ".docker" / "config.json"
    if not config_path.exists():
        return None, None

    try:
        with open(config_path) as f:
            config = json.load(f)

        # Check auths section for docker.io
        auths = config.get("auths", {})
        for registry in ["docker.io", "https://index.docker.io/v1/"]:
            if registry in auths:
                auth_entry = auths[registry]
                # Decode base64 auth if present
                if "auth" in auth_entry:
                    decoded = base64.b64decode(auth_entry["auth"]).decode()
                    if ":" in decoded:
                        username, password = decoded.split(":", 1)
                        return username, password
    except Exception as e:
        logger.debug(f"Could not read docker config.json: {e}")

    return None, None


def _list_existing_tags(namespace: str, repo_name: str, username: str, password: str) -> set[str]:
    """
    Query DockerHub Registry API v2 to list existing tags for a repository.

    Args:
        namespace: DockerHub namespace (user or org)
        repo_name: Repository name
        username: DockerHub username for auth
        password: DockerHub password/token for auth

    Returns:
        Set of existing tags, or empty set if repo doesn't exist or on error
    """
    try:
        # Step 1: Get bearer token from auth server
        auth_endpoint = "https://auth.docker.io/token"
        token_params = {
            "service": "registry.docker.io",
            "scope": f"repository:{namespace}/{repo_name}:pull",
        }
        token_resp = requests.get(auth_endpoint, params=token_params, auth=(username, password), timeout=30)

        # If 404 or auth fails, repo might not exist
        if token_resp.status_code == 404:
            return set()
        if token_resp.status_code != 200:
            logger.warning(f"Failed to get auth token for {namespace}/{repo_name}: HTTP {token_resp.status_code}")
            return set()

        token_data = token_resp.json()
        token = token_data.get("token")
        if not token:
            logger.warning(f"No token in auth response for {namespace}/{repo_name}")
            return set()

        # Step 2: List tags using the bearer token
        tags_url = f"https://registry.hub.docker.com/v2/{namespace}/{repo_name}/tags/list"
        headers = {"Authorization": f"Bearer {token}"}
        tags_resp = requests.get(tags_url, headers=headers, timeout=30)

        if tags_resp.status_code == 404:
            # Repository doesn't exist yet
            logger.debug(f"Repository {namespace}/{repo_name} not found on DockerHub")
            return set()

        if tags_resp.status_code != 200:
            logger.warning(f"Failed to list tags for {namespace}/{repo_name}: HTTP {tags_resp.status_code}")
            return set()

        tags_data = tags_resp.json()
        tags = tags_data.get("tags", [])
        return set(tags) if tags else set()

    except requests.exceptions.Timeout:
        logger.warning(f"Timeout querying DockerHub for {namespace}/{repo_name}")
        return set()
    except Exception as e:
        logger.warning(f"Error listing tags for {namespace}/{repo_name}: {e}")
        return set()


def _encode_dockerhub_tag_from_local(local_ref: str) -> str:
    """
    Encode a local image reference into the tag used for single-repo DockerHub publishing.

    Encoding logic:
      - local_ref like "repo[:tag]" becomes "repo--tag" (slashes in either side become "__").
      - If the result exceeds 128 chars, add an 8-char hash suffix.

    Args:
        local_ref: Local image reference (e.g., "owner-repo-sha:final")

    Returns:
        Encoded tag suitable for DockerHub
    """
    if ":" in local_ref and "/" not in local_ref.split(":", 1)[1]:
        repo, tag = local_ref.rsplit(":", 1)
    else:
        repo, tag = local_ref, "latest"
    base = repo.replace("/", "__")
    tag_enc = tag.replace("/", "__").replace(":", "--")
    composed = f"{base}--{tag_enc}"
    if len(composed) <= 128:
        return composed
    h = hashlib.sha256(composed.encode()).hexdigest()[:8]
    trimmed = composed[-(128 - 10) :]
    return f"{trimmed}--{h}"


def _list_dockerhub_tags_single_repo(*, namespace: str, repo_name: str, username: str, password: str) -> set[str]:
    """
    Return the set of existing image tags for a DockerHub repository.

    Safe: returns empty set on missing repo or auth issues. Logs warnings instead of raising.

    Args:
        namespace: DockerHub namespace
        repo_name: Repository name
        username: DockerHub username
        password: DockerHub password/token

    Returns:
        Set of existing tags
    """
    return _list_existing_tags(namespace, repo_name, username, password)


def filter_tasks_not_on_dockerhub(
    tasks: list[Task],
    *,
    namespace: str,
    username: str,
    password: str,
    repository_mode: str = "single",
    single_repo: str = "all",
) -> list[Task]:
    """
    Filter out tasks whose target image already exists on DockerHub.

    Currently supports repository_mode="single" (default used by Context.build_and_publish_to_dockerhub).

    Args:
        tasks: List of tasks to filter
        namespace: DockerHub namespace
        username: DockerHub username
        password: DockerHub password/token
        repository_mode: "single" or "mirror"
        single_repo: Repository name for single mode

    Returns:
        Filtered list of tasks that don't exist on DockerHub
    """
    if repository_mode != "single":
        # Fallback: if we don't know how tags are computed, don't filter
        logger.warning("DockerHub pre-filter only supports repository_mode='single'; skipping filter.")
        return tasks

    existing_tags = _list_dockerhub_tags_single_repo(
        namespace=namespace, repo_name=single_repo, username=username, password=password
    )
    if not existing_tags:
        return tasks

    filtered: list[Task] = []
    skipped = 0
    for t in tasks:
        local_ref = t.with_tag("final").get_image_name()  # e.g., owner-repo-sha:final
        enc_tag = _encode_dockerhub_tag_from_local(local_ref)  # e.g., owner-repo-sha--final
        if enc_tag in existing_tags:
            skipped += 1
            logger.info("Skipping %s (already on DockerHub as %s/%s:%s)", local_ref, namespace, single_repo, enc_tag)
            continue
        filtered.append(t)
    if skipped:
        logger.info("Filtered out %d/%d tasks already on DockerHub", skipped, len(tasks))
    return filtered
