from __future__ import annotations

import asyncio
import tempfile
import threading
from typing import Any

from datasmith.agents.synthesizer import Synthesizer
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


def _build_pr_image(
    owner: str,
    repo: str,
    issue_number: int,
    sha: str,
    env_payload: str,
    docker_context: Any | None = None,
    python_version: str = "",
) -> str:
    """Build the final PR image from synthesized context (no push).

    Returns the PR image tag that will be used for the subsequent push.
    """
    from datasmith.docker.images import ImageManager, get_pr_image_name

    ctx = docker_context
    mgr = ImageManager()
    pr_tag = get_pr_image_name(owner, repo, issue_number)

    if ctx is not None:
        with tempfile.TemporaryDirectory(prefix="docker-ctx-") as tmpdir:
            ctx.to_directory(tmpdir)
            _fill_missing_scripts(tmpdir, base_commit=sha)
            mgr.build_pr_image(
                owner,
                repo,
                issue_number,
                context=tmpdir,
                commit_sha=sha or "HEAD",
                env_payload=env_payload or "[]",
                py_version=python_version,
            )
    else:
        mgr.build_pr_image(
            owner,
            repo,
            issue_number,
            commit_sha=sha or "HEAD",
            env_payload=env_payload or "[]",
            py_version=python_version,
        )

    return pr_tag


def _push_pr_image(owner: str, repo: str, pr_tag: str) -> None:
    """Push a previously-built PR image (and its repo parent) to DockerHub."""
    from datasmith.docker.images import get_repo_image_name
    from datasmith.docker.publish import DockerHubPublisher

    publisher = DockerHubPublisher()
    repo_tag = get_repo_image_name(owner, repo)

    try:
        publisher.push(repo_tag)
    except Exception:
        logger.warning("Failed to push repo image %s (non-fatal)", repo_tag)

    publisher.push(pr_tag)
    logger.info("Pushed PR image: %s", pr_tag)


def _render_run_tests_sh(docker_templates: Any, base_commit: str) -> str:
    """Render the run-tests.sh Jinja2 template with embedded scripts."""
    from pathlib import Path

    from jinja2 import Environment, FileSystemLoader

    docker_templates = Path(docker_templates)
    env = Environment(
        loader=FileSystemLoader(str(docker_templates)),
        keep_trailing_newline=True,
        autoescape=False,
    )
    template = env.get_template("run-tests.sh")

    pytest_runner = (docker_templates / "pytest_runner.py").read_text()
    parser = (docker_templates / "parser.py").read_text()

    return template.render(
        base_commit=base_commit,
        pytest_runner=pytest_runner,
        parser=parser,
        run_pytest=True,
    )


def _fill_missing_scripts(context_dir: str, base_commit: str = "") -> None:
    """Copy any missing shell scripts and Dockerfile.pr from the templates directory.

    Synthesized contexts may only contain a subset of the 9 expected files
    (e.g. only ``build_pkg_sh``).  The Dockerfile.pr ``COPY`` directives
    require every file to be present, so we backfill from the built-in
    templates for anything the synthesizer didn't produce.

    ``run-tests.sh`` is a Jinja2 template that requires rendering with
    ``base_commit`` and embedded Python scripts before it can be used.
    """
    import os
    import shutil
    from pathlib import Path

    templates = Path(__file__).parents[1] / "docker" / "templates"

    # Every file that Dockerfile.pr references via COPY
    required = [
        "Dockerfile.pr",
        "docker_build_env.sh",
        "docker_build_pkg.sh",
        "docker_build_run.sh",
        "docker_build_final.sh",
        "profile.sh",
        "run-tests.sh",
        "entrypoint.sh",
    ]

    for fname in required:
        target = os.path.join(context_dir, fname)
        if os.path.exists(target):
            continue
        if fname == "run-tests.sh":
            # run-tests.sh is a Jinja2 template — render it instead of copying raw
            rendered = _render_run_tests_sh(templates, base_commit=base_commit)
            with open(target, "w") as f:
                f.write(rendered)
        else:
            src = templates / fname
            if src.exists():
                shutil.copy2(str(src), target)


# Lock to serialize prerequisite image builds (base + repo) across threads.
# Building these is expensive and they're shared, so we avoid duplicate work.
_prereq_lock = threading.Lock()
# Track repos whose prerequisite images are confirmed present.
_prereq_done: set[tuple[str, str]] = set()


class SynthesizeImagesRunner(BaseRunner):
    """Run Synthesizer for each PR to produce Docker build contexts."""

    def __init__(
        self,
        synthesizer: Synthesizer,
        gh: Any | None = None,
        n_concurrent: int = 3,
    ) -> None:
        super().__init__(name="synthesize_images", n_concurrent=n_concurrent)
        self._synthesizer = synthesizer
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

        from datasmith.docker.images import get_repo_image_name

        repo_image = get_repo_image_name(owner, repo)

        # Run synthesizer in thread (Docker operations are blocking)
        ctx = await asyncio.to_thread(
            self._synthesizer.run,
            owner,
            repo,
            issue_number,
            pr_context,
            sha,
            repo_image=repo_image,
            env_payload=env_payload,
            python_version=py_version,
        )

        if ctx is None:
            raise RuntimeError(f"Synthesis failed for {owner}/{repo}#{issue_number}")

        logger.info("Successfully synthesized image for %s/%s#%d", owner, repo, issue_number)

        # Build the final PR image locally (no push yet)
        pr_tag = await asyncio.to_thread(_build_pr_image, owner, repo, issue_number, sha, env_payload, ctx, py_version)

        # Record the container name in Supabase *before* pushing. If the DB
        # write fails, the image stays unpublished and a re-run picks up the
        # PR cleanly — avoids orphan images on DockerHub with no DB state.
        client = get_client()
        client.table("pull_requests").update({"container_name": pr_tag}).eq("owner", owner).eq("repo", repo).eq(
            "issue_number", issue_number
        ).execute()

        # DB state is durable — safe to publish the image.
        await asyncio.to_thread(_push_pr_image, owner, repo, pr_tag)

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
