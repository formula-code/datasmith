"""Shared file/path helpers."""

from __future__ import annotations

import re
import shutil
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlparse

import requests

from datasmith.logging_config import get_logger

logger = get_logger("core.file_utils")

_HEX = re.compile(r"[0-9a-fA-F]{7,40}$")


def extract_repo_full_name(url: str) -> str | None:
    """Turn a GitHub repo URL into the canonical ``owner/repo`` string."""
    if not url:
        return None

    path = url.split(":", 1)[-1] if url.startswith(("git@", "ssh://")) else urlparse(url).path.lstrip("/")
    path = path.rstrip("/").removesuffix(".git")
    if "/" not in path:
        return None
    owner, repo = path.split("/", 1)
    return f"{owner}/{repo}"


def parse_commit_url(url: str) -> tuple[str, str, str]:
    """Return ``(owner, repo, sha)`` from a GitHub commit URL."""
    parsed = urlparse(url.strip())
    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"Unsupported URL scheme: {parsed.scheme!r}")

    if parsed.hostname not in {"github.com", "www.github.com"}:
        raise ValueError(f"Not a GitHub URL: {url!r}")

    path = unquote(parsed.path)
    parts = [p for p in PurePosixPath(path).parts if p != "/"]

    if len(parts) < 4 or parts[2] != "commit":
        raise ValueError(f"Not a GitHub commit URL: {url!r}")

    owner, repo, sha = parts[0], parts[1], parts[3]
    if not _HEX.fullmatch(sha):
        raise ValueError(f"Invalid commit SHA: {sha!r}")

    return owner, repo, sha.lower()


def dl_and_open(url: str, dl_dir: str, base: str | None = None, force: bool = False) -> str | None:
    """Fetch *url* into *dl_dir* and return the local filename."""
    parsed = urlparse(url)
    is_http = parsed.scheme in ("http", "https")
    is_file = parsed.scheme == "file"

    rel_path = url[len(base) :].lstrip("/") if base and url.startswith(base) else parsed.path.lstrip("/")
    raw_parts = [unquote(part) for part in Path(rel_path).parts]
    raw_path = Path(dl_dir).joinpath(*raw_parts).resolve()

    if raw_path.exists():
        local_path = raw_path
    else:

        def clean_component(component: str) -> str:
            component = unquote(component)
            component = component.replace(" ", "_").replace("@", "AT")
            component = component.replace("(", "").replace(")", "")
            return re.sub(r"[^A-Za-z0-9.\-_/]", "_", component)

        clean_parts = [clean_component(part) for part in raw_parts]
        local_path = Path(dl_dir).joinpath(*clean_parts).resolve()

    local_path.parent.mkdir(parents=True, exist_ok=True)

    if is_http:
        if force or not local_path.exists():
            try:
                response = requests.get(url, timeout=20)
                if response.status_code == 404:
                    return None
                response.raise_for_status()
                local_path.write_bytes(response.content)
            except requests.RequestException:
                return None
        return str(local_path)

    src_path = Path(parsed.path) if is_file else Path(url)
    if not src_path.exists():
        return None
    if force or not local_path.exists():
        try:
            shutil.copy2(src_path, local_path)
        except OSError:
            return None
    return str(local_path)


__all__ = ["dl_and_open", "extract_repo_full_name", "parse_commit_url"]
