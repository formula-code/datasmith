#!/usr/bin/env python3
"""Backfill: build and push Docker images for already-synthesized tasks.

Finds all rows in ``docker_contexts`` whose corresponding ``pull_requests``
row has no ``container_name``, builds the three-tier image hierarchy
(base → repo → PR), pushes to DockerHub, and records the container_name.

Usage:
    # Dry run — show what would be built
    uv run python scratch/scripts/backfill_push.py --dry-run

    # Build and push everything (default concurrency: 1)
    uv run python scratch/scripts/backfill_push.py

    # Build with higher concurrency
    uv run python scratch/scripts/backfill_push.py --concurrency 4

    # Limit to specific repo
    uv run python scratch/scripts/backfill_push.py --repo numpy/numpy

    # Limit total tasks
    uv run python scratch/scripts/backfill_push.py --limit 10
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

# Ensure project is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / "tokens.env")

from datasmith.docker.context import DockerContext
from datasmith.docker.images import ImageManager, get_base_image_name, get_repo_image_name, get_pr_image_name
from datasmith.docker.publish import DockerHubPublisher
from datasmith.utils.db import fetch_all, get_client

TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "src" / "datasmith" / "docker" / "templates"


def find_pending_tasks(repo_filter: str | None = None, limit: int | None = None) -> list[dict[str, Any]]:
    """Find synthesized contexts that haven't been pushed yet."""

    # 1) All synthesized contexts
    ctx_rows = fetch_all("docker_contexts", select="owner, repo, sha, issue_number")
    print(f"Found {len(ctx_rows)} rows in docker_contexts")

    # 2) PRs without container_name that have is_performance_commit=True
    pr_rows = fetch_all(
        "pull_requests",
        select="owner, repo, issue_number, merge_commit_sha",
        filters={"is_performance_commit": True},
        is_null=["container_name"],
    )
    # Index by (owner, repo, sha) for fast lookup
    pr_lookup: dict[tuple[str, str, str], dict[str, Any]] = {}
    for pr in pr_rows:
        sha = pr.get("merge_commit_sha", "")
        if sha:
            pr_lookup[(pr["owner"], pr["repo"], sha)] = pr
    print(f"Found {len(pr_lookup)} PRs without container_name")

    # 3) Packages for env_payload / python_version
    pkg_rows = fetch_all(
        "packages",
        select="owner, repo, sha, env_payload, python_version",
        filters={"can_install": True},
    )
    pkg_lookup: dict[tuple[str, str, str], dict[str, Any]] = {
        (p["owner"], p["repo"], p["sha"]): p for p in pkg_rows
    }
    print(f"Found {len(pkg_lookup)} resolved packages")

    # 4) Join: contexts that have a matching PR without container_name
    tasks: list[dict[str, Any]] = []
    for ctx in ctx_rows:
        key = (ctx["owner"], ctx["repo"], ctx["sha"])
        pr = pr_lookup.get(key)
        if pr is None:
            continue

        if repo_filter:
            filt_owner, filt_repo = repo_filter.split("/")
            if ctx["owner"] != filt_owner or ctx["repo"] != filt_repo:
                continue

        pkg = pkg_lookup.get(key, {})
        tasks.append({
            "owner": ctx["owner"],
            "repo": ctx["repo"],
            "sha": ctx["sha"],
            "issue_number": ctx["issue_number"],
            "env_payload": pkg.get("env_payload", "{}"),
            "python_version": pkg.get("python_version", ""),
        })

    # Deduplicate by (owner, repo, issue_number)
    seen: set[tuple[str, str, int]] = set()
    deduped: list[dict[str, Any]] = []
    for t in tasks:
        k = (t["owner"], t["repo"], t["issue_number"])
        if k not in seen:
            seen.add(k)
            deduped.append(t)

    if limit:
        deduped = deduped[:limit]

    return deduped


def load_context(owner: str, repo: str, sha: str) -> DockerContext | None:
    """Load a synthesized DockerContext from the database."""
    try:
        client = get_client()
        resp = (
            client.table("docker_contexts")
            .select("*")
            .eq("owner", owner)
            .eq("repo", repo)
            .eq("sha", sha)
            .execute()
        )
        if resp.data:
            row = resp.data[0]
            return DockerContext(
                dockerfile=row.get("dockerfile", ""),
                build_base_sh=row.get("build_base_sh", ""),
                build_env_sh=row.get("build_env_sh", ""),
                build_pkg_sh=row.get("build_pkg_sh", ""),
                build_run_sh=row.get("build_run_sh", ""),
                build_final_sh=row.get("build_final_sh", ""),
                profile_sh=row.get("profile_sh", ""),
                run_tests_sh=row.get("run_tests_sh", ""),
                entrypoint_sh=row.get("entrypoint_sh", ""),
            )
    except Exception as e:
        print(f"  ERROR loading context: {e}")
    return None


def ensure_dockerfile_pr(context_dir: str) -> None:
    """Copy the template Dockerfile.pr into the context dir if missing."""
    target = os.path.join(context_dir, "Dockerfile.pr")
    if os.path.exists(target):
        return
    template = TEMPLATES_DIR / "Dockerfile.pr"
    if template.exists():
        shutil.copy2(str(template), target)


def build_and_push_task(
    task: dict[str, Any],
    mgr: ImageManager,
    publisher: DockerHubPublisher,
    dry_run: bool = False,
) -> bool:
    """Build and push a single task. Returns True on success."""
    owner = task["owner"]
    repo = task["repo"]
    sha = task["sha"]
    issue_number = task["issue_number"]
    env_payload = task["env_payload"]
    py_version = task["python_version"]

    base_tag = get_base_image_name()
    repo_tag = get_repo_image_name(owner, repo)
    pr_tag = get_pr_image_name(owner, repo, issue_number)

    print(f"\n{'='*60}")
    print(f"Processing {owner}/{repo}#{issue_number} (sha={sha[:12]})")
    print(f"  PR image:  {pr_tag}")
    print(f"  Repo image: {repo_tag}")

    if dry_run:
        print("  [DRY RUN] Would build and push")
        return True

    # 1) Ensure base image
    if not mgr.image_exists(base_tag):
        print(f"  Building base image: {base_tag}")
        try:
            mgr.build_base_image(py_version=py_version)
        except Exception as e:
            print(f"  ERROR building base image: {e}")
            return False

    # 2) Ensure repo image
    if not mgr.image_exists(repo_tag):
        print(f"  Building repo image: {repo_tag}")
        try:
            mgr.build_repo_image(owner, repo, py_version=py_version)
        except Exception as e:
            print(f"  ERROR building repo image: {e}")
            return False

    # 3) Load synthesized context
    ctx = load_context(owner, repo, sha)
    if ctx is None:
        print("  ERROR: no context found in DB")
        return False

    # 4) Build PR image from synthesized context
    try:
        with tempfile.TemporaryDirectory(prefix="docker-ctx-") as tmpdir:
            ctx.to_directory(tmpdir)
            ensure_dockerfile_pr(tmpdir)
            print(f"  Building PR image: {pr_tag}")
            mgr.build_pr_image(
                owner,
                repo,
                issue_number,
                context=tmpdir,
                commit_sha=sha,
                env_payload=env_payload or "{}",
            )
    except Exception as e:
        print(f"  ERROR building PR image: {e}")
        return False

    # 5) Push repo image (idempotent)
    try:
        print(f"  Pushing repo image: {repo_tag}")
        publisher.push(repo_tag)
    except Exception as e:
        print(f"  WARNING: failed to push repo image: {e}")

    # 6) Push PR image
    try:
        print(f"  Pushing PR image: {pr_tag}")
        publisher.push(pr_tag)
    except Exception as e:
        print(f"  ERROR pushing PR image: {e}")
        return False

    # 7) Update container_name in Supabase
    try:
        client = get_client()
        client.table("pull_requests").update({"container_name": pr_tag}).eq(
            "owner", owner
        ).eq("repo", repo).eq("issue_number", issue_number).execute()
        print(f"  SUCCESS: recorded container_name={pr_tag}")
    except Exception as e:
        print(f"  WARNING: push succeeded but DB update failed: {e}")

    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill: build and push synthesized Docker images")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be built without building")
    parser.add_argument("--repo", type=str, default=None, help="Filter to a specific repo (e.g., numpy/numpy)")
    parser.add_argument("--limit", type=int, default=None, help="Max number of tasks to process")
    parser.add_argument("--concurrency", type=int, default=1, help="Number of concurrent builds (default: 1)")
    args = parser.parse_args()

    tasks = find_pending_tasks(repo_filter=args.repo, limit=args.limit)
    if not tasks:
        print("No pending tasks found.")
        return

    # Group by repo for display
    repos: dict[str, int] = {}
    for t in tasks:
        key = f"{t['owner']}/{t['repo']}"
        repos[key] = repos.get(key, 0) + 1

    print(f"\nPending tasks: {len(tasks)} across {len(repos)} repos")
    for repo_name, count in sorted(repos.items()):
        print(f"  {repo_name}: {count} tasks")

    if args.dry_run:
        print("\n[DRY RUN] No images will be built or pushed.")
        for t in tasks:
            print(f"  Would build: {get_pr_image_name(t['owner'], t['repo'], t['issue_number'])}")
        return

    mgr = ImageManager()
    publisher = DockerHubPublisher()

    succeeded = 0
    failed = 0
    start = time.time()

    for i, task in enumerate(tasks, 1):
        print(f"\n[{i}/{len(tasks)}]", end="")
        try:
            ok = build_and_push_task(task, mgr, publisher, dry_run=args.dry_run)
            if ok:
                succeeded += 1
            else:
                failed += 1
        except KeyboardInterrupt:
            print("\n\nInterrupted! Cleaning up...")
            break
        except Exception as e:
            print(f"  UNEXPECTED ERROR: {e}")
            failed += 1

    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"Done in {elapsed:.0f}s: {succeeded} succeeded, {failed} failed, {len(tasks) - succeeded - failed} skipped")


if __name__ == "__main__":
    main()
