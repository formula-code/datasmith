from __future__ import annotations

import asyncio
import tempfile
import threading
from typing import Any

from datasmith.runners.base import BaseRunner
from datasmith.utils import get_client, get_logger

logger = get_logger("runners.synthesize_images")


def _ensure_prerequisite_images(owner: str, repo: str, py_version: str = "") -> None:
    """Build the base and repo Docker images if they don't exist locally.

    The three-tier hierarchy (base → repo → PR) requires each parent image
    to be present in the local daemon before the child can be built.
    """
    from datasmith.docker.images import ImageManager, get_base_image_name, get_repo_image_name

    mgr = ImageManager()
    base_tag = get_base_image_name()
    repo_tag = get_repo_image_name(owner, repo)

    if not mgr.image_exists(base_tag):
        logger.info("Building missing base image: %s", base_tag)
        mgr.build_base_image(py_version=py_version)

    if not mgr.image_exists(repo_tag):
        logger.info("Building missing repo image: %s", repo_tag)
        mgr.build_repo_image(owner, repo, py_version=py_version)


def _build_and_push_pr_image(
    owner: str,
    repo: str,
    issue_number: int,
    sha: str,
    env_payload: str,
    docker_context: Any | None = None,
) -> str:
    """Build the final PR image from synthesized context and push to DockerHub.

    *docker_context* is an optional ``DockerContext`` whose files are written
    to a temp directory and used as the Docker build context.  When ``None``,
    the context is loaded from the ``docker_contexts`` Supabase table.

    Returns the pushed image tag.
    """
    from datasmith.docker.context import DockerContext
    from datasmith.docker.images import ImageManager, get_pr_image_name, get_repo_image_name
    from datasmith.docker.publish import DockerHubPublisher

    # Resolve context: argument > DB lookup > default templates
    ctx: DockerContext | None = docker_context
    if ctx is None:
        ctx = _load_context_from_db(owner, repo, sha)

    mgr = ImageManager()
    pr_tag = get_pr_image_name(owner, repo, issue_number)

    if ctx is not None:
        # Materialize synthesized context to a temp dir for docker build
        with tempfile.TemporaryDirectory(prefix="docker-ctx-") as tmpdir:
            ctx.to_directory(tmpdir)
            # Ensure the Dockerfile.pr template is present (synthesized contexts
            # may omit it — fall back to the built-in template).
            _ensure_dockerfile_pr(tmpdir)
            mgr.build_pr_image(
                owner,
                repo,
                issue_number,
                context=tmpdir,
                commit_sha=sha or "HEAD",
                env_payload=env_payload or "{}",
            )
    else:
        # No synthesized context — use default templates
        mgr.build_pr_image(
            owner,
            repo,
            issue_number,
            commit_sha=sha or "HEAD",
            env_payload=env_payload or "{}",
        )

    # Push to DockerHub
    publisher = DockerHubPublisher()
    repo_tag = get_repo_image_name(owner, repo)

    # Push repo image if not already remote (idempotent — DockerHub deduplicates layers)
    try:
        publisher.push(repo_tag)
    except Exception:
        logger.warning("Failed to push repo image %s (non-fatal)", repo_tag)

    publisher.push(pr_tag)
    logger.info("Pushed PR image: %s", pr_tag)
    return pr_tag


def _load_context_from_db(owner: str, repo: str, sha: str) -> Any | None:
    """Load a DockerContext from the docker_contexts table, or return None."""
    if not sha:
        return None
    from datasmith.docker.context import DockerContext

    try:
        client = get_client()
        resp = client.table("docker_contexts").select("*").eq("owner", owner).eq("repo", repo).eq("sha", sha).execute()
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
    except Exception:
        logger.debug("Failed to load context from DB for %s/%s@%s", owner, repo, sha[:12] if sha else "?")
    return None


def _ensure_dockerfile_pr(context_dir: str) -> None:
    """Copy the template Dockerfile.pr into *context_dir* if not already present."""
    import os
    import shutil
    from pathlib import Path

    target = os.path.join(context_dir, "Dockerfile.pr")
    if os.path.exists(target):
        return
    template = Path(__file__).parents[1] / "docker" / "templates" / "Dockerfile.pr"
    if template.exists():
        shutil.copy2(str(template), target)


# Lock to serialize prerequisite image builds (base + repo) across threads.
# Building these is expensive and they're shared, so we avoid duplicate work.
_prereq_lock = threading.Lock()
# Track repos whose prerequisite images are confirmed present.
_prereq_done: set[tuple[str, str]] = set()


class SynthesizeImagesRunner(BaseRunner):
    """Run Synthesizer for each PR to produce Docker build contexts."""

    def __init__(
        self,
        synthesizer: Any,
        verifier: Any,
        gh: Any | None = None,
        n_concurrent: int = 3,
    ) -> None:
        super().__init__(name="synthesize_images", n_concurrent=n_concurrent)
        self._synthesizer = synthesizer
        self._verifier = verifier
        self._gh = gh  # GitHubClient, optional — needed for rendering problem statements

    async def _render_problem(self, item: dict[str, Any]) -> str | None:
        """Render the problem statement for a PR, scraping linked issues.

        Returns the rendered markdown, or ``None`` if rendering is skipped
        (no GitHubClient) or fails.
        """
        if self._gh is None:
            return None

        owner: str = item["owner"]
        repo: str = item["repo"]
        issue_number: int = item["issue_number"]

        from datasmith.github.links import scrape_links
        from datasmith.github.models import PR
        from datasmith.github.render import render_problem_statement

        # Build a PR object for scrape_links and render_problem_statement
        pr = PR(
            repository=f"{owner}/{repo}",
            issue_number=issue_number,
            title=item.get("title", ""),
            body=item.get("body", ""),
            created_at=item.get("created_at"),
        )

        # BFS-scrape linked issues (async GitHub API calls)
        issues = await scrape_links(
            pr,
            self._gh.get_issue_expanded,
            depth=2,
            only_issues=True,
            limit=6,
        )

        logger.info(
            "Scraped %d linked issues for %s/%s#%d",
            len(issues),
            owner,
            repo,
            issue_number,
        )

        # Render the problem statement (may invoke ProblemExtractor LLM — run in thread)
        repo_description: str = item.get("repo_description", "")
        rendered = await asyncio.to_thread(
            render_problem_statement,
            pr,
            issues=issues,
            repo_description=repo_description,
            anonymize=True,
            extract=True,
        )

        # Persist to DB
        client = get_client()
        client.table("pull_requests").update({"rendered_problem": rendered}).eq("owner", owner).eq("repo", repo).eq(
            "issue_number", issue_number
        ).execute()

        logger.info("Rendered problem statement for %s/%s#%d", owner, repo, issue_number)
        return rendered

    async def _process_item(self, item: Any) -> None:
        """Process a PR dict with owner, repo, issue_number, pr_context."""
        owner = item["owner"]
        repo = item["repo"]
        issue_number = item["issue_number"]
        pr_context = item.get("pr_context", "")
        py_version = item.get("python_version", "")

        # Ensure base and repo images exist before synthesis needs them
        await asyncio.to_thread(self._ensure_prereqs, owner, repo, py_version)

        # Render the problem statement before synthesis (skip if already rendered)
        if not pr_context:
            rendered = await self._render_problem(item)
            if rendered:
                pr_context = rendered

        sha = item.get("sha", "")
        env_payload = item.get("env_payload", "")

        # Run synthesizer in thread (Docker operations are blocking)
        ctx = await asyncio.to_thread(
            self._synthesizer.run,
            owner,
            repo,
            issue_number,
            pr_context,
            self._verifier,
            sha,
            base_context=item.get("base_context"),
            env_payload=env_payload,
            python_version=py_version,
        )

        if ctx is None:
            raise RuntimeError(f"Synthesis failed for {owner}/{repo}#{issue_number}")

        logger.info("Successfully synthesized image for %s/%s#%d", owner, repo, issue_number)

        # Build the final PR image and push to DockerHub
        pr_tag = await asyncio.to_thread(_build_and_push_pr_image, owner, repo, issue_number, sha, env_payload, ctx)

        # Record the container name in Supabase
        client = get_client()
        client.table("pull_requests").update({"container_name": pr_tag}).eq("owner", owner).eq("repo", repo).eq(
            "issue_number", issue_number
        ).execute()

    @staticmethod
    def _ensure_prereqs(owner: str, repo: str, py_version: str) -> None:
        """Build base/repo images if missing, with dedup across threads."""
        key = (owner, repo)
        if key in _prereq_done:
            return
        with _prereq_lock:
            if key in _prereq_done:
                return
            _ensure_prerequisite_images(owner, repo, py_version)
            _prereq_done.add(key)
